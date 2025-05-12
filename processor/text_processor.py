# seoran/processor/text_processor.py
# سطح: پردازشگر متن با قابلیت‌های کامل NLP (توکنایزیشن، حذف کلمات توقف، لماتایزیشن)

import os
import glob
from bs4 import BeautifulSoup, Comment
from hazm import Normalizer, sent_tokenize, word_tokenize, Lemmatizer, Stemmer # <<< جدید: ابزارهای NLP از Hazm
import re
import time
# import json # <<< برای ذخیره به صورت JSON (فعلا استفاده نمی‌شود)

# --- پیکربندی ---
HTML_FILES_BASE_DIR = os.path.join("..", "crawler", "downloaded_pages")
PROCESSED_TEXTS_DIR = "processed_texts_tokens" # <<< تغییر نام پوشه خروجی برای تمایز

# تگ‌هایی که محتوای آنها باید کاملا حذف شود (بدون تغییر نسبت به قبل)
UNWANTED_TAGS = [
    'script', 'style', 'header', 'footer', 'nav', 'aside', 'form', 'button', 
    'select', 'textarea', 'iframe', 'link', 'meta', 'noscript', 'embed', 'object'
]
# سلکتورهای CSS برای حذف بخش‌های خاص (بدون تغییر نسبت به قبل)
UNWANTED_CSS_SELECTORS = [
    '.ads', '.advertisement', '.ad', '.banner', '.popup', '.cookie-banner', '.cookie-notice',
    '#sidebar', '#comments', '.comments-area', '.comment-list', '.reply',
    '.related-posts', '.related_posts', '.share-buttons', '.social-sharing',
    '.site-footer', '.site-header', '.main-navigation', '.menu', '.social-links', 
    '.breadcrumbs', '.pagination', '.widget', '.author-bio', '.post-meta-data',
    '[class*="promo"]', '[id*="promo"]', '[class*="advert"]', '[id*="advert"]',
    '[aria-hidden="true"]'
]

MIN_TEXT_LENGTH = 100 # حداقل طول متن استخراجی اولیه
MIN_TOKEN_COUNT = 20  # <<< جدید: حداقل تعداد توکن پس از پردازش NLP برای ذخیره

# --- مقداردهی اولیه ابزارهای پردازش زبان ---
hazm_normalizer = Normalizer()
hazm_lemmatizer = Lemmatizer() # <<< جدید
# hazm_stemmer = Stemmer() # اگر بخواهیم از Stemmer استفاده کنیم

# لیست کلمات توقف فارسی (می‌توان این لیست را از فایل خواند یا تکمیل کرد)
# یک لیست اولیه از کلمات توقف رایج Hazm به همراه چند تای دیگر
DEFAULT_STOP_WORDS = [
    "و", "در", "به", "از", "که", "این", "آن", "با", "است", "هست", "بود", "شد", "شود",
    "برای", "تا", "یک", "خود", "ها", "های", "می", "هم", "نیز", "اما", "ولی", "یا",
    "پس", "اگر", "هر", "همه", "نه", "حتی", "یعنی", "باید", "شاید", "چون", "چنین",
    "دیگر", "همین", "همان", "فقط", "آنها", "ایشان", "ما", "شما", "تو", "او", "من",
    "کند", "کنند", "کنیم", "کنی", "کنید", "کرد", "کرده", "کردهم", "کردهای", "کردهاند",
    "دارد", "دارند", "داریم", "داری", "دارید", "داشت", "داشته", "داشتهاند", "باشد",
    "باشند", "باشیم", "باشی", "باشید", "خواهد", "خواهند", "خواهیم", "خواهی", "خواهید",
    "برو", "برود", "بروند", "برویم", "بروی", "بروید", "بیا", "بیاید", "بیایند", "بیاییم",
    "بیایی", "بیایید", "دهد", "دهند", "دهیم", "دهی", "دهید", "گیرد", "گیرند", "گیریم",
    "گیری", "گیرید", "گوید", "گویند", "گوییم", "گویی", "گویید", "رود", "روند", "رویم",
    "روی", "روید", "شود", "شوند", "شویم", "شوی", "شوید", "آید", "آیند", "آییم", "آیی",
    "آیید", "داد", "گرفت", "گفت", "رفت", "شد", "آمد", "کرد", "زد", "شدن", "کردن", "بودن",
    "مثل", "چیزی", "کسی", "نوعی", "چیست", "کدام", "چه", "چطور", "چگونه", "کجا", "کی",
    "وقتی", "هنگامی", "آیا", "مگر", "هنوز", "بسیار", "خیلی", "بیش", "کم", "اول", "دوم",
    "سوم", "بعد", "قبل", "زیر", "روی", "بالای", "پایین", "بین", "میان", "پیش", "پس", "نزد",
    "حدود", "تقریبا", "حدودا", "مورد", "باره", "خصوص", "طریق", "لحاظ", "نسبت", "ابتدا",
    "انتها", "داخل", "خارج", "سمت", "طرف", "جای", "جاییکه", "هرگز", "همیشه", "گاهی",
    "اغلب", "معمولا", "واقعا", "حتما", "امروز", "دیروز", "فردا", "شب", "روز", "ماه", "سال",
    "شنبه", "یکشنبه", "دوشنبه", "سهشنبه", "چهارشنبه", "پنجشنبه", "جمعه", "مختلف", "جدید",
    "قدیم", "بزرگ", "کوچک", "خوب", "بد", "زیبا", "زشت", "سفید", "سیاه", "قرمز", "آبی",
    "سبز", "زرد", "اولین", "دومین", "آخرین", "مهم", "اصلی", "فرعی", "کلی", "جزئی", "کاملا",
    "هیچ", "یکدیگر", "برخی", "بعضی", "تمام", "همگی", "اینجا", "آنجا", "ای", "ایا", "اینکه",
    "اینگونه", "انچه", "انکه", "اه", "او", "اول", "اي", "ايشان", "اكنون", "اگر", "اما", "امده",
    "امدن", "اند", "ان", "انها", "انگاه", "اني", "ایا", "ايد", "اين", "اينكه", "اينگونه", "با",
    "بار", "باره", "باشد", "باشند", "باشيم", "باشي", "باشيد", "بالا", "بالاي", "بايد", "براي",
    "برخي", "برود", "بروند", "برويم", "بروي", "بروييم", "برويد", "بسيار", "بش", "بشود", "بشوند",
    "بشويم", "بشوي", "بشوييم", "بشوييد", "بعد", "بعضا", "بعضي", "بلکه", "بلكه", "به", "بهتر",
    "بود", "بودن", "بودند", "بودم", "بودي", "بوديم", "بي", "بيا", "بيايد", "بيايند", "بياييم",
    "بيايي", "بياييد", "بيش", "بيشتر", "بين", "تا", "تازه", "تحت", "تر", "ترين", "پس", "پيش",
    "تنها", "توان", "تواند", "توانند", "توانيم", "تواني", "توانيد", "جا", "جاي", "جايي", "جز",
    "چرا", "چطور", "چقدر", "چگونه", "چند", "چندين", "چه", "چو", "چون", "چيز", "چيزي", "چی",
    "حتي", "حال", "حالا", "خب", "خدمت", "خود", "خودش", "خودشان", "خودم", "خودمان", "خودت",
    "خودتان", "داد", "دادن", "دادند", "دادم", "دادي", "داديم", "دار", "دارد", "دارند", "دارم",
    "داري", "داريم", "داشت", "داشتن", "داشتند", "داشتم", "داشتي", "داشتيم", "دان", "داند", "دانند",
    "دانم", "داني", "دانيم", "دانيد", "در", "درباره", "درون", "دست", "دفع", "دوباره", "دوم",
    "ديد", "دیگر", "ديگر", "ديگران", "ديگري", "دیگر", "را", "راه", "رفت", "رفتن", "رفتند",
    "رفتم", "رفتي", "رفتيم", "روز", "روزها", "روي", "سال", "ساله", "ساير", "سر", "سراسر", "سعي",
    "سوم", "سوي", "سپس", "شان", "شايد", "شد", "شدن", "شدند", "شدم", "شدي", "شديم", "شما",
    "شناس", "شناسد", "شناسند", "شناسم", "شناسي", "شناسيم", "شناسيد", "شود", "شوند", "شويم",
    "شوي", "شوييم", "شوييد", "طبق", "طريق", "طور", "طي", "ع", "فقط", "فعلا", "قبل", "مانند",
    "مثل", "من", "مگر", "مي", "ميليون", "ميليارد", "ناشي", "نام", "نبايد", "نباشد", "نبود",
    "نخست", "نخواهد", "نخواست", "نخواهد", "نخواهند", "نخواهيم", "نخواهي", "نخواهيد", "ندارد",
    "ندارند", "ندارم", "نداري", "نداريم", "نداشت", "نداشتند", "نداشتم", "نداشتي", "نداشتيم",
    "نزد", "نزديك", "نشان", "نشده", "نشود", "نشوند", "نشويم", "نشوي", "نشوييد", "نه", "نوع",
    "نوعي", "نيست", "نيستند", "نيستم", "نيستي", "نيستيم", "نيز", "ها", "هاي", "هر", "هرگز",
    "هزار", "هست", "هستند", "هستم", "هستي", "هستيم", "هم", "همان", "همه", "همين", "هميشه",
    "هنوز", "هيچ", "هیچگاه", "و", "وقت", "وقتي", "ولي", "يا", "یاد", "يك", "یکی", "یکی", "یک", "گويد",
    "گويند", "گوييم", "گويي", "گوييد", "گيرد", "گيرند", "گيريم", "گيري", "گيريد", "لازم", "لطفا",
    "ضمن", "فوق", "همچنان", "همچنين", "هنوز", "هنگام", "واقعي", "پيش", "پس", "تحت", "تنها",
    "چيزي", "حدود", "خصوص", "خود", "داخل", "درباره", "دیگری", "مختلف", "نزدیک", "سایر", "آقای",
    "خانم", "علیه", "اینکه", "بطوری", "جناح", "جناب", "جهت", "حتی", "حدود", "خصوصا", "خیلی",
    "دانست", "داشت", "سرکار", "شخصی", "طی", "علیرغم", "فوق", "قصد", "گونه", "گهگاه", "لاکن",
    "متاسفانه", "نهایت", "وقتی", "خواهشا", "یعنی", "یواش"
]
# تبدیل لیست کلمات توقف به set برای جستجوی سریعتر
STOP_WORDS = set(DEFAULT_STOP_WORDS)


# --- کلاس برای نگهداری آمار پردازش (بدون تغییر) ---
class ProcessingStats:
    def __init__(self):
        self.total_html_files = 0
        self.successfully_processed = 0
        self.failed_to_read = 0
        self.empty_or_short_extracted_text = 0
        self.empty_or_short_normalized_text = 0 # این شمارنده حالا برای متن قبل از توکنایزیشن است
        self.empty_or_short_token_list = 0 # <<< جدید: برای لیست توکن‌های بسیار کوتاه
        self.failed_to_save = 0
        self.failed_files_list = []

    def report(self):
        print("\n--- آمار نهایی پردازش متن (با NLP) ---")
        print(f"تعداد کل فایل‌های HTML بررسی شده: {self.total_html_files}")
        print(f"تعداد فایل‌های با موفقیت پردازش و ذخیره شده (توکن‌ها): {self.successfully_processed}")
        print(f"تعداد فایل‌هایی که خواندن آنها با خطا مواجه شد: {self.failed_to_read}")
        print(f"تعداد فایل‌هایی با متن استخراجی اولیه خالی یا بسیار کوتاه: {self.empty_or_short_extracted_text}")
        print(f"تعداد فایل‌هایی با متن نرمال‌شده (قبل از NLP) خالی یا بسیار کوتاه: {self.empty_or_short_normalized_text}")
        print(f"تعداد فایل‌هایی با لیست توکن‌های نهایی خالی یا بسیار کوتاه: {self.empty_or_short_token_list}") # <<< جدید
        print(f"تعداد فایل‌هایی که ذخیره آنها با خطا مواجه شد: {self.failed_to_save}")
        if self.failed_files_list:
            print("\nلیست فایل‌هایی که در پردازش آنها خطا رخ داد یا رد شدند:")
            for f_path, reason in self.failed_files_list:
                print(f"- {f_path} (دلیل: {reason})")
        print("------------------------------------")

# --- توابع کمکی ---

# extract_text_from_html_v2 بدون تغییر باقی می‌ماند (همان نسخه قبلی)
def extract_text_from_html_v2(html_content, stats):
    if not html_content:
        return ""
    soup = BeautifulSoup(html_content, 'lxml')
    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()
    for tag_name in UNWANTED_TAGS:
        for tag in soup.find_all(tag_name):
            tag.decompose()
    for selector in UNWANTED_CSS_SELECTORS:
        try:
            for element in soup.select(selector):
                element.decompose()
        except Exception:
            pass
    body = soup.find('body')
    if not body:
        text_content = soup.get_text(separator=' ', strip=True)
    else:
        main_content_tags = ['article', 'main', '.post-content', '.entry-content', '#content', '#main-content']
        main_text_element = None
        for tag_selector in main_content_tags:
            if tag_selector.startswith('.'): main_text_element = body.select_one(tag_selector)
            elif tag_selector.startswith('#'): main_text_element = body.select_one(tag_selector)
            else: main_text_element = body.find(tag_selector)
            if main_text_element: break
        if main_text_element: text_content = main_text_element.get_text(separator=' ', strip=True)
        else: text_content = body.get_text(separator=' ', strip=True)
    if not text_content: return ""
    url_pattern = r'https?://[^\s/$.?#].[^\s]*|www\.[^\s/$.?#].[^\s]*'
    text_content = re.sub(url_pattern, '', text_content)
    lines = (line.strip() for line in text_content.splitlines())
    text_content = ' '.join(line for line in lines if line) 
    text_content = re.sub(r'\s{2,}', ' ', text_content).strip()
    return text_content


# normalize_persian_text_v2 بدون تغییر باقی می‌ماند (همان نسخه قبلی)
def normalize_persian_text_v2(text, remove_numbers=False, remove_english=False):
    if not text: return ""
    normalized_text = hazm_normalizer.normalize(text)
    normalized_text = re.sub(r'(.)\1{2,}', r'\1\1', normalized_text) 
    if remove_numbers: normalized_text = re.sub(r'[0-9۰-۹]+', '', normalized_text)
    if remove_english: normalized_text = re.sub(r'[a-zA-Z]+', '', normalized_text)
    normalized_text = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', normalized_text)
    normalized_text = re.sub(r'\s{2,}', ' ', normalized_text).strip()
    return normalized_text


def process_text_with_nlp(text): # <<< تابع جدید
    """
    متن نرمال‌شده را دریافت کرده و مراحل کامل NLP را روی آن اجرا می‌کند:
    1. توکنایز کردن جملات
    2. توکنایز کردن کلمات
    3. حذف کلمات توقف
    4. لماتایز کردن (یا ریشه‌یابی)
    5. (اختیاری) حذف توکن‌های خیلی کوتاه یا نامعتبر
    """
    if not text:
        return []

    processed_tokens = []
    
    # 1. توکنایز کردن جملات
    sentences = sent_tokenize(text)
    
    for sentence in sentences:
        # 2. توکنایز کردن کلمات
        words = word_tokenize(sentence)
        
        for word in words:
            # 3. حذف کلمات توقف
            if word in STOP_WORDS:
                continue
            
            # (اختیاری) حذف توکن‌هایی که فقط عدد یا علامت نگارشی هستند یا خیلی کوتاه‌اند
            # این کار می‌تواند دقت را افزایش دهد اما ممکن است برخی اطلاعات را از دست بدهد
            # مثال: اگر کلمه فقط از اعداد تشکیل شده باشد یا طولش کمتر از 2 باشد (به جز موارد خاص)
            if word.isnumeric(): # اگر کلمه فقط عدد است
                 continue
            if len(word) < 2 and word not in ['ما', 'تو', 'او', 'من']: # کلمات تک حرفی (به جز ضمایر)
                 continue
            # حذف علائم نگارشی که به تنهایی توکن شده‌اند
            # Hazm word_tokenize معمولا علائم را جدا می‌کند
            # می‌توانیم یک لیست از علائم نگارشی تعریف کنیم و اگر توکن در آن بود، حذفش کنیم
            punctuation_pattern = r'^[\.\،\؛\؟\!\(\)\[\]\{\}:\"\'\«\»_\-–—/\\]+$'
            if re.match(punctuation_pattern, word):
                continue

            # 4. لماتایز کردن (یا ریشه‌یابی)
            # استفاده از لماتایزر:
            lemma = hazm_lemmatizer.lemmatize(word)
            # اگر از Stemmer استفاده می‌کنید:
            # stem = hazm_stemmer.stem(word)
            # lemma = stem # یا هر کدام که انتخاب می‌کنید

            # گاهی لماتایزر یک # قبل از بن ماضی اضافه می‌کند، آن را حذف می‌کنیم
            # یا برای کلمات ناشناس خود کلمه را برمی‌گرداند
            if '#' in lemma:
                parts = lemma.split('#')
                # اگر قسمت دوم (بن) وجود داشت و معتبر بود، از آن استفاده کن، وگرنه از قسمت اول (اسم)
                lemma = parts[1] if len(parts) > 1 and parts[1] else parts[0]
            
            if lemma and len(lemma.strip()) > 1 : # توکن لماتایز شده نباید خالی یا خیلی کوتاه باشد
                processed_tokens.append(lemma.strip())
                
    return processed_tokens


def process_html_file_task_v2(html_filepath, output_base_dir, stats): # <<< تغییر نام تابع و منطق
    """
    یک فایل HTML را پردازش می‌کند: خواندن، استخراج متن، نرمال‌سازی اولیه،
    پردازش NLP (توکنایز، حذف کلمات توقف، لماتایز) و ذخیره لیست توکن‌ها.
    """
    try:
        with open(html_filepath, 'r', encoding='utf-8', errors='replace') as f:
            html_content = f.read()
    except IOError as e:
        stats.failed_to_read += 1
        stats.failed_files_list.append((html_filepath, f"IOError on read: {e}"))
        return
    except Exception as e:
        stats.failed_to_read += 1
        stats.failed_files_list.append((html_filepath, f"Unexpected error on read: {e}"))
        return

    # 1. استخراج متن
    extracted_text = extract_text_from_html_v2(html_content, stats) # stats پاس داده نمی‌شود چون داخل خودش مدیریت می‌کند
    if not extracted_text or len(extracted_text.strip()) < MIN_TEXT_LENGTH:
        stats.empty_or_short_extracted_text += 1
        stats.failed_files_list.append((html_filepath, "Extracted text too short or empty"))
        return

    # 2. نرمال‌سازی اولیه متن فارسی
    normalized_text = normalize_persian_text_v2(extracted_text, remove_numbers=True, remove_english=True) # اعداد و انگلیسی را حذف می‌کنیم
    if not normalized_text or len(normalized_text.strip()) < MIN_TEXT_LENGTH / 2: # آستانه کمتر برای متن نرمال شده
        stats.empty_or_short_normalized_text += 1
        stats.failed_files_list.append((html_filepath, "Normalized text (pre-NLP) too short or empty"))
        return

    # 3. پردازش NLP برای تولید لیست توکن‌ها <<< جدید
    final_tokens = process_text_with_nlp(normalized_text)
    
    if not final_tokens or len(final_tokens) < MIN_TOKEN_COUNT:
        stats.empty_or_short_token_list += 1
        stats.failed_files_list.append((html_filepath, "Final token list too short or empty"))
        return

    # 4. ذخیره لیست توکن‌ها
    # فعلا توکن‌ها را با فاصله از هم در یک فایل .txt ذخیره می‌کنیم
    # در آینده می‌توان به فرمت JSON یا فرمت‌های بهینه‌تر دیگر ذخیره کرد
    output_content = " ".join(final_tokens)

    base_filename = os.path.basename(html_filepath)
    # پسوند فایل خروجی را تغییر می‌دهیم تا مشخص باشد حاوی توکن است
    output_filename = os.path.splitext(base_filename)[0] + "_tokens.txt" 
    
    relative_path_from_base_dir = os.path.relpath(os.path.dirname(html_filepath), HTML_FILES_BASE_DIR)
    if relative_path_from_base_dir == '.': domain_subdir_name = ''
    else: domain_subdir_name = relative_path_from_base_dir

    final_output_dir = os.path.join(output_base_dir, domain_subdir_name)

    if not os.path.exists(final_output_dir):
        try:
            os.makedirs(final_output_dir, exist_ok=True)
        except OSError as e:
            stats.failed_to_save += 1
            stats.failed_files_list.append((html_filepath, f"OSError on creating output dir {final_output_dir}: {e}"))
            return
            
    output_filepath = os.path.join(final_output_dir, output_filename)

    try:
        with open(output_filepath, 'w', encoding='utf-8') as f:
            f.write(output_content)
        stats.successfully_processed += 1
    except IOError as e:
        stats.failed_to_save += 1
        stats.failed_files_list.append((html_filepath, f"IOError on save: {e}"))
    except Exception as e:
        stats.failed_to_save += 1
        stats.failed_files_list.append((html_filepath, f"Unexpected error on save: {e}"))


def main_processor_v2(): # <<< تغییر نام تابع اصلی
    print("شروع پردازش فایل‌های HTML (با مراحل کامل NLP)...")
    start_time = time.time()
    
    processing_stats = ProcessingStats()

    # اطمینان از وجود پوشه اصلی خروجی برای توکن‌ها
    if not os.path.exists(PROCESSED_TEXTS_DIR): # PROCESSED_TEXTS_DIR حالا "processed_texts_tokens" است
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
    
    for i, filepath in enumerate(html_file_paths):
        print(f"پردازش فایل {i+1}/{processing_stats.total_html_files}: {filepath}")
        # استفاده از تابع وظیفه جدید
        process_html_file_task_v2(filepath, PROCESSED_TEXTS_DIR, processing_stats) 

    end_time = time.time()
    total_time = end_time - start_time

    processing_stats.report()
    print(f"کل زمان صرف شده برای پردازش: {total_time:.2f} ثانیه.")


if __name__ == "__main__":
    main_processor_v2() # <<< فراخوانی تابع اصلی جدید
