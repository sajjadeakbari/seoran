# seoran/processor/text_processor.py
# سطح: پردازشگر متن بهبود یافته با قابلیت‌های بیشتر، گزارش‌گیری دقیق‌تر

import os
import glob
from bs4 import BeautifulSoup, Comment # Comment برای حذف کامنت‌های HTML
from hazm import Normalizer
import re # برای عبارات منظم پیشرفته‌تر
import time # برای اندازه‌گیری زمان پردازش

# --- پیکربندی ---
HTML_FILES_BASE_DIR = os.path.join("..", "crawler", "downloaded_pages")
PROCESSED_TEXTS_DIR = "processed_texts"

# تگ‌هایی که محتوای آنها باید کاملا حذف شود
UNWANTED_TAGS = [
    'script', 'style', 'header', 'footer', 'nav', 'aside', 'form', 'button', 
    'select', 'textarea', 'iframe', 'link', 'meta', 'noscript', 'embed', 'object'
]
# سلکتورهای CSS برای حذف بخش‌های خاص (مثل تبلیغات، کامنت‌ها، سایدبار)
UNWANTED_CSS_SELECTORS = [
    '.ads', '.advertisement', '.ad', '.banner', '.popup', '.cookie-banner', '.cookie-notice',
    '#sidebar', '#comments', '.comments-area', '.comment-list', '.reply',
    '.related-posts', '.related_posts', '.share-buttons', '.social-sharing',
    '.site-footer', '.site-header', '.main-navigation', '.menu', '.social-links', 
    '.breadcrumbs', '.pagination', '.widget', '.author-bio', '.post-meta-data',
    '[class*="promo"]', '[id*="promo"]', '[class*="advert"]', '[id*="advert"]',
    '[aria-hidden="true"]' # المان‌هایی که برای صفحه‌خوان‌ها پنهان هستند
]

# حداقل طول متن (تعداد کاراکتر) برای اینکه یک صفحه پردازش و ذخیره شود
MIN_TEXT_LENGTH = 100 # افزایش به ۱۰۰ کاراکتر

# --- مقداردهی اولیه ابزارهای پردازش زبان ---
hazm_normalizer = Normalizer()

# --- کلاس برای نگهداری آمار پردازش ---
class ProcessingStats:
    def __init__(self):
        self.total_html_files = 0
        self.successfully_processed = 0
        self.failed_to_read = 0
        self.empty_or_short_extracted_text = 0
        self.empty_or_short_normalized_text = 0
        self.failed_to_save = 0
        self.failed_files_list = [] # لیستی از فایل‌هایی که در پردازش خطا داشتند

    def report(self):
        print("\n--- آمار نهایی پردازش متن ---")
        print(f"تعداد کل فایل‌های HTML بررسی شده: {self.total_html_files}")
        print(f"تعداد فایل‌های با موفقیت پردازش و ذخیره شده: {self.successfully_processed}")
        print(f"تعداد فایل‌هایی که خواندن آنها با خطا مواجه شد: {self.failed_to_read}")
        print(f"تعداد فایل‌هایی با متن استخراجی خالی یا بسیار کوتاه: {self.empty_or_short_extracted_text}")
        print(f"تعداد فایل‌هایی با متن نرمال‌شده خالی یا بسیار کوتاه: {self.empty_or_short_normalized_text}")
        print(f"تعداد فایل‌هایی که ذخیره آنها با خطا مواجه شد: {self.failed_to_save}")
        if self.failed_files_list:
            print("\nلیست فایل‌هایی که در پردازش آنها خطا رخ داد یا رد شدند:")
            for f_path, reason in self.failed_files_list:
                print(f"- {f_path} (دلیل: {reason})")
        print("------------------------------")

# --- توابع کمکی ---

def extract_text_from_html_v2(html_content, stats):
    """
    نسخه بهبود یافته استخراج متن از HTML.
    - حذف دقیق‌تر تگ‌های ناخواسته
    - حذف URLها از متن نهایی
    - حذف متن داخل تگ a اگرچه خود تگ a حذف نمی‌شود (برای حفظ متن لینک)
    """
    if not html_content:
        return ""

    soup = BeautifulSoup(html_content, 'lxml')

    # 0. حذف کامنت‌های HTML
    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()

    # 1. حذف تگ‌های ناخواسته و محتوای آنها
    for tag_name in UNWANTED_TAGS:
        for tag in soup.find_all(tag_name):
            tag.decompose()

    # 2. حذف عناصر بر اساس سلکتورهای CSS ناخواسته
    for selector in UNWANTED_CSS_SELECTORS:
        try:
            for element in soup.select(selector):
                element.decompose()
        except Exception:
            pass # نادیده گرفتن خطاهای مربوط به سلکتورهای نامعتبر

    # 3. استخراج متن از بدنه اصلی
    body = soup.find('body')
    if not body:
        text_content = soup.get_text(separator=' ', strip=True)
    else:
        # اولویت با تگ‌های محتوای اصلی
        main_content_tags = ['article', 'main', '.post-content', '.entry-content', '#content', '#main-content']
        main_text_element = None
        for tag_selector in main_content_tags:
            if tag_selector.startswith('.'): # اگر کلاس است
                main_text_element = body.select_one(tag_selector)
            elif tag_selector.startswith('#'): # اگر ID است
                main_text_element = body.select_one(tag_selector)
            else: # اگر نام تگ است
                main_text_element = body.find(tag_selector)
            
            if main_text_element:
                break # اگر یکی پیدا شد، کافی است
        
        if main_text_element:
            text_content = main_text_element.get_text(separator=' ', strip=True)
        else:
            text_content = body.get_text(separator=' ', strip=True)
    
    if not text_content:
        return ""

    # 4. پاکسازی‌های بیشتر روی متن استخراج شده
    # 4.1. حذف URL های کامل از متن
    url_pattern = r'https?://[^\s/$.?#].[^\s]*|www\.[^\s/$.?#].[^\s]*'
    text_content = re.sub(url_pattern, '', text_content)

    # 4.2. حذف خطوط خالی اضافی و فضاهای متعدد (بهبود یافته)
    lines = (line.strip() for line in text_content.splitlines())
    # استفاده از ' '.join برای اطمینان از یک فاصله بین کلمات پس از strip
    text_content = ' '.join(line for line in lines if line) 
    text_content = re.sub(r'\s{2,}', ' ', text_content).strip() # جایگزینی چند فاصله با یک فاصله

    return text_content


def normalize_persian_text_v2(text, remove_numbers=False, remove_english=False):
    """
    نسخه بهبود یافته نرمال‌سازی متن فارسی.
    - نرمال‌سازی با Hazm
    - حذف کاراکترهای تکراری
    - گزینه برای حذف اعداد
    - گزینه برای حذف حروف انگلیسی
    - حذف علائم نگارشی اضافی و کاراکترهای خاص نامربوط
    """
    if not text:
        return ""

    # 1. نرمال‌سازی اولیه با Hazm
    normalized_text = hazm_normalizer.normalize(text)

    # 2. حذف کاراکترهای تکراری بیش از حد (مثال: سلاااام -> سلام)
    # این الگو کاراکترهایی که 3 بار یا بیشتر تکرار شده‌اند را به 2 بار تکرار کاهش می‌دهد.
    # برای کاهش به یکبار: r'\1'
    normalized_text = re.sub(r'(.)\1{2,}', r'\1\1', normalized_text) 

    # 3. حذف اعداد (اختیاری)
    if remove_numbers:
        # حذف اعداد فارسی و انگلیسی
        normalized_text = re.sub(r'[0-9۰-۹]+', '', normalized_text)

    # 4. حذف حروف انگلیسی (اختیاری)
    if remove_english:
        normalized_text = re.sub(r'[a-zA-Z]+', '', normalized_text)

    # 5. حذف علائم نگارشی خاص و کاراکترهای ناخواسته
    # نگه داشتن حروف فارسی، اعداد (اگر حذف نشده باشند)، فاصله، نیم‌فاصله و علائم نگارشی اصلی فارسی
    # علائم نگارشی اصلی فارسی: نقطه، ویرگول، نقطه‌ویرگول، علامت سوال، علامت تعجب، پرانتز، گیومه
    # \u200c برای نیم‌فاصله
    # \u0600-\u06FF محدوده حروف فارسی
    # \u064B-\u0652 محدوده برخی اعراب (اگرچه نرمالایزر Hazm معمولا حذف می‌کند)
    # اضافه کردن کاراکترهای دیگر مثل (؟ ، ؛ « » ( ) [ ] { } : - – — / \ | @ # $ % ^ & * _ + = < > ` ~)
    # الگو برای نگه داشتن: حروف فارسی، اعداد (اگر remove_numbers=False)، فاصله، نیم‌فاصله، و علائم نگارشی پرکاربرد
    
    allowed_chars_pattern = r'[^\w\s\u0600-\u06FF\u064B-\u0652'
    if not remove_numbers:
        allowed_chars_pattern += r'0-9۰-۹'
    # علائم نگارشی که می‌خواهیم نگه داریم:
    # . , ؛ ؟ ! « » ( ) [ ] { } : - – — " ' / \
    # نقطه \. ویرگول ، نقطه‌ویرگول ؛ علامت سوال \؟ تعجب \! گیومه «» پرانتز \(\) براکت \[\] آکولاد \{\} دو نقطه : خط تیره - – — کوتیشن " ' اسلش / \\
    # دقت کنید که برخی از اینها ممکن است توسط نرمالایزر Hazm به شکل دیگری تبدیل شده باشند.
    # یک رویکرد ساده‌تر: فقط حروف و اعداد و فاصله/نیم‌فاصله را نگهدار و بقیه را حذف کن، سپس علائم مهم را جداگانه بررسی کن.
    # در اینجا ما یک لیست محدود از کاراکترهای غیر الفبایی-عددی را حذف می‌کنیم.
    # این بخش نیاز به دقت و تست زیاد دارد تا کاراکترهای مفید حذف نشوند.
    
    # حذف هر چیزی غیر از حروف فارسی، انگلیسی (اگر حذف نشده باشد)، اعداد (اگر حذف نشده باشند)، فاصله و نیم‌فاصله
    # و علائم نگارشی استاندارد (نقطه، ویرگول، علامت سوال، تعجب).
    # این یک پاکسازی نسبتا تهاجمی است.
    
    # قدم اول: حذف کاراکترهایی که قطعا نمی‌خواهیم
    # کاراکترهای کنترلی، برخی نمادهای خاص و ...
    normalized_text = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', normalized_text) # حذف کاراکترهای کنترلی
    # normalized_text = re.sub(r'[^\u0600-\u06FF\u0698\u067E\u0686\u06AFa-zA-Z0-9۰-۹\s\u200c\.\،\؛\؟\ lắc()\[\]\{\}:\"\'\«\»_\-–—]', '', normalized_text)
    # این الگو هنوز کامل نیست و نیاز به تنظیم دقیق دارد.
    # برای سادگی فعلا فقط نرمال‌سازی Hazm و حذف تکرار را انجام می‌دهیم.
    # بهبودهای بیشتر در این بخش می‌تواند در آینده اضافه شود.

    # 6. حذف فضاهای اضافی که ممکن است در اثر حذف‌ها ایجاد شده باشند
    normalized_text = re.sub(r'\s{2,}', ' ', normalized_text).strip()
    
    return normalized_text


def process_html_file_task(html_filepath, output_base_dir, stats):
    """
    یک فایل HTML را پردازش می‌کند (خواندن، استخراج، نرمال‌سازی، ذخیره).
    این تابع برای استفاده در پردازش موازی (در آینده) طراحی شده.
    """
    try:
        with open(html_filepath, 'r', encoding='utf-8', errors='replace') as f:
            html_content = f.read()
    except IOError as e:
        # print(f"خطا در خواندن فایل {html_filepath}: {e}")
        stats.failed_to_read += 1
        stats.failed_files_list.append((html_filepath, f"IOError on read: {e}"))
        return
    except Exception as e:
        # print(f"خطای پیش‌بینی نشده در خواندن {html_filepath}: {e}")
        stats.failed_to_read += 1
        stats.failed_files_list.append((html_filepath, f"Unexpected error on read: {e}"))
        return

    # 1. استخراج متن
    extracted_text = extract_text_from_html_v2(html_content, stats)
    if not extracted_text or len(extracted_text.strip()) < MIN_TEXT_LENGTH:
        # print(f"متن مفید کمی از {html_filepath} استخراج شد یا متن خالی بود. رد می‌شود.")
        stats.empty_or_short_extracted_text += 1
        stats.failed_files_list.append((html_filepath, "Extracted text too short or empty"))
        return

    # 2. نرمال‌سازی متن فارسی
    # در اینجا می‌توانید گزینه‌های remove_numbers=True یا remove_english=True را فعال کنید
    normalized_text = normalize_persian_text_v2(extracted_text, remove_numbers=False, remove_english=False)
    if not normalized_text or len(normalized_text.strip()) < MIN_TEXT_LENGTH:
        # print(f"متن پس از نرمال‌سازی برای {html_filepath} خالی یا بسیار کوتاه شد. رد می‌شود.")
        stats.empty_or_short_normalized_text += 1
        stats.failed_files_list.append((html_filepath, "Normalized text too short or empty"))
        return

    # 3. ذخیره متن پردازش شده
    base_filename = os.path.basename(html_filepath)
    output_filename = os.path.splitext(base_filename)[0] + "_processed.txt"
    
    relative_path_from_base_dir = os.path.relpath(os.path.dirname(html_filepath), HTML_FILES_BASE_DIR)
    # اگر html_filepath مستقیما در HTML_FILES_BASE_DIR باشد، relative_path_from_base_dir می شود '.'
    # باید این حالت را مدیریت کرد
    if relative_path_from_base_dir == '.':
        domain_subdir_name = '' # یا نام دامنه پیش‌فرض اگر می‌خواهید
    else:
        domain_subdir_name = relative_path_from_base_dir

    final_output_dir = os.path.join(output_base_dir, domain_subdir_name)

    if not os.path.exists(final_output_dir):
        try:
            os.makedirs(final_output_dir, exist_ok=True)
        except OSError as e:
            # print(f"خطا در ایجاد پوشه خروجی {final_output_dir}: {e}")
            stats.failed_to_save += 1 # این خطا مربوط به ایجاد پوشه است، نه خود فایل
            stats.failed_files_list.append((html_filepath, f"OSError on creating output dir {final_output_dir}: {e}"))
            return
            
    output_filepath = os.path.join(final_output_dir, output_filename)

    try:
        with open(output_filepath, 'w', encoding='utf-8') as f:
            f.write(normalized_text)
        # print(f"متن پردازش شده از {html_filepath} در {output_filepath} ذخیره شد.")
        stats.successfully_processed += 1
    except IOError as e:
        # print(f"خطا در ذخیره فایل پردازش شده {output_filepath}: {e}")
        stats.failed_to_save += 1
        stats.failed_files_list.append((html_filepath, f"IOError on save: {e}"))
    except Exception as e:
        # print(f"خطای پیش‌بینی نشده در ذخیره {output_filepath}: {e}")
        stats.failed_to_save += 1
        stats.failed_files_list.append((html_filepath, f"Unexpected error on save: {e}"))


def main_processor():
    """
    تابع اصلی برای اجرای پردازشگر متن روی تمام فایل‌های HTML.
    """
    print("شروع پردازش فایل‌های HTML (نسخه بهبود یافته)...")
    start_time = time.time()
    
    processing_stats = ProcessingStats()

    if not os.path.exists(PROCESSED_TEXTS_DIR):
        try:
            os.makedirs(PROCESSED_TEXTS_DIR)
            print(f"پوشه خروجی {PROCESSED_TEXTS_DIR} ایجاد شد.")
        except OSError as e:
            print(f"خطا: عدم امکان ایجاد پوشه خروجی اصلی {PROCESSED_TEXTS_DIR}: {e}. برنامه خاتمه می‌یابد.")
            return

    html_files_pattern = os.path.join(HTML_FILES_BASE_DIR, "**", "*.html")
    html_file_paths = glob.glob(html_files_pattern, recursive=True)
    
    htm_files_pattern = os.path.join(HTML_FILES_BASE_DIR, "**", "*.htm")
    html_file_paths.extend(glob.glob(htm_files_pattern, recursive=True))

    processing_stats.total_html_files = len(html_file_paths)

    if not html_file_paths:
        print(f"هیچ فایل HTML در مسیر {HTML_FILES_BASE_DIR} یافت نشد.")
        print("لطفاً ابتدا خزنده را اجرا کنید تا صفحاتی دانلود شوند.")
        processing_stats.report()
        return

    print(f"تعداد {processing_stats.total_html_files} فایل HTML برای پردازش یافت شد.")
    
    # پردازش تک نخی (برای سادگی در این مرحله)
    for i, filepath in enumerate(html_file_paths):
        print(f"پردازش فایل {i+1}/{processing_stats.total_html_files}: {filepath}")
        process_html_file_task(filepath, PROCESSED_TEXTS_DIR, processing_stats)

    end_time = time.time()
    total_time = end_time - start_time

    processing_stats.report()
    print(f"کل زمان صرف شده برای پردازش: {total_time:.2f} ثانیه.")


if __name__ == "__main__":
    main_processor()
