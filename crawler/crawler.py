# seoran/crawler/crawler.py
# سطح: خزنده وب با قابلیت استخراج لینک، مدیریت صف، جلوگیری از تکرار،
# محدودیت دامنه، مدیریت اولیه خطا و تاخیر مودبانه.

import requests
import os
import re
from urllib.parse import urlparse, urljoin, unquote
from bs4 import BeautifulSoup
import time
import validators

# --- پیکربندی اولیه ---
# پوشه‌ای که صفحات دانلود شده در آن ذخیره می‌شوند
DOWNLOAD_DIR = "downloaded_pages"

# User-Agent برای ارسال با درخواست‌ها
HEADERS = {
    'User-Agent': 'SeoranBot/1.0 (+http://sajjadakbari.ir/seoran-bot-info)'
}

# لیستی از دامنه‌های مجاز برای خزش. اگر خالی باشد، به دامنه URL شروع محدود می‌شود.
# مثال: ALLOWED_DOMAINS = ["sajjadakbari.ir", "virgool.io"]
ALLOWED_DOMAINS = [] # در این نسخه، با استفاده از base_domain در تابع crawl_website کنترل می‌کنیم


# --- مجموعه‌ها و متغیرهای سراسری برای ردیابی URL ها و وضعیت خزش ---
urls_to_visit = set()  # URL هایی که باید بازدید شوند (Frontier)
visited_urls = set()   # URL هایی که قبلا بازدید شده‌اند

# حداکثر تعداد صفحاتی که می‌خواهیم دانلود کنیم (برای تست)
MAX_PAGES_TO_CRAWL = 10  # می‌توانید این عدد را برای تست‌های بزرگتر افزایش دهید
pages_crawled_count = 0

# تاخیر بین درخواست‌ها (به ثانیه) برای اینکه به سرور فشار نیاوریم
REQUEST_DELAY = 1


# --- توابع کمکی ---

def sanitize_filename(url):
    """
    یک نام فایل امن از URL ایجاد می‌کند.
    """
    try:
        decoded_url = unquote(url)
    except Exception: # اگر unquote به هر دلیلی خطا داد (مثلا URL خیلی ناقص بود)
        decoded_url = url

    parsed_url = urlparse(decoded_url)
    
    filename = parsed_url.netloc if parsed_url.netloc else "unknown_domain"
    
    if parsed_url.path and parsed_url.path != "/":
        path_part = parsed_url.path.replace('/', '_').strip('_')
        common_extensions = ['.html', '.htm', '.php', '.asp', '.aspx', '.jsp']
        for ext in common_extensions:
            if path_part.lower().endswith(ext): # مقایسه بدون حساسیت به بزرگی و کوچکی حروف
                path_part = path_part[:-len(ext)]
                break
        if path_part:
            filename += "_" + path_part
    
    if parsed_url.query:
        # فقط کاراکترهای مجاز برای نام فایل از کوئری نگه داشته شوند
        safe_query = re.sub(r'[^\w\-\.]', '_', parsed_url.query)
        filename += "_query_" + safe_query[:50] # بخشی از کوئری برای جلوگیری از نام‌های خیلی طولانی

    # پاکسازی نهایی برای حذف کاراکترهای غیرمجاز باقیمانده
    filename = re.sub(r'[^\w\-\.]', '_', filename)
    
    # محدود کردن طول نام فایل
    if len(filename) > 180:
        filename = filename[:180]
        
    # اطمینان از اینکه با .html ختم می‌شود و تکراری نیست
    if not filename.lower().endswith(".html"):
         filename += ".html"
    
    # اگر نام فایل به دلایلی خیلی کوتاه یا نامفهوم شد
    if not filename or filename == ".html" or filename == "_query_.html" or len(filename) < 10 : # افزایش شرط برای نام‌های خیلی کوتاه
        # استفاده از بخشی از هش URL برای ایجاد نام منحصر به فرد تر در موارد خاص
        import hashlib
        url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()[:8]
        domain_part = parsed_url.netloc.replace('.', '_') if parsed_url.netloc else "nodomain"
        filename = f"{domain_part}_{url_hash}_page.html"
        filename = re.sub(r'[^\w\-\.]', '_', filename) # پاکسازی مجدد

    return filename


def fetch_page(url):
    """
    محتوای HTML یک URL را دانلود می‌کند.
    """
    global pages_crawled_count
    print(f"درحال تلاش برای دانلود: {url}")
    try:
        # استفاده از stream=True و بررسی اولیه هدرها برای فایل‌های بزرگ یا غیر HTML
        response = requests.get(url, headers=HEADERS, timeout=20, stream=True, allow_redirects=True)
        response.raise_for_status()

        content_type = response.headers.get('Content-Type', '').lower()
        if 'text/html' not in content_type:
            print(f"محتوای غیر HTML در {url} (نوع: {content_type}). رد می‌شود.")
            response.close() # بستن اتصال در صورت عدم نیاز به محتوا
            return None

        # خواندن محتوا (مهم: بعد از stream=True، محتوا باید خوانده شود)
        # برای جلوگیری از دانلود فایل‌های بسیار بزرگ، می‌توان یک محدودیت حجم در نظر گرفت
        content_length = response.headers.get('Content-Length')
        if content_length and int(content_length) > 5 * 1024 * 1024: # 5MB limit
            print(f"صفحه {url} بیش از حد بزرگ است ({content_length} بایت). رد می‌شود.")
            response.close()
            return None

        html_text = response.text # این خط محتوای کامل را می‌خواند

        # تشخیص انکودینگ
        if response.encoding is None or 'iso-8859-1' in response.encoding.lower() or 'windows-1256' in response.encoding.lower():
            # اگر تشخیص اولیه خوب نبود یا انکودینگ عربی/غربی بود، utf-8 را امتحان می‌کنیم
            # BeautifulSoup می‌تواند در تشخیص انکودینگ بهتر عمل کند
            soup_for_encoding = BeautifulSoup(response.content, 'lxml') # استفاده از response.content (بایت‌ها)
            if soup_for_encoding.original_encoding:
                 response.encoding = soup_for_encoding.original_encoding
            else: # اگر باز هم تشخیص نداد، به UTF-8 بازمیگردیم
                 response.encoding = 'utf-8'
            try:
                html_text = response.content.decode(response.encoding, errors='replace')
            except Exception as e:
                print(f"خطا در دیکود کردن محتوا با انکودینگ {response.encoding} برای {url}: {e}")
                html_text = response.content.decode('utf-8', errors='replace') # بازگشت به utf-8 با جایگزینی خطاها

        print(f"صفحه {url} با موفقیت دانلود شد.")
        pages_crawled_count += 1
        return html_text
        
    except requests.exceptions.HTTPError as e:
        print(f"خطای HTTP {e.response.status_code} هنگام دانلود {url}: {e.response.reason}")
        if e.response.status_code == 404:
            print(f"صفحه {url} یافت نشد (404).")
        elif e.response.status_code == 403:
            print(f"دسترسی به {url} ممنوع است (403).")
        # می‌توانیم خطاهای دیگر مانند 401, 400, 5xx را نیز جداگانه بررسی کنیم
        return None
    except requests.exceptions.Timeout:
        print(f"زمان انتظار برای {url} تمام شد (Timeout).")
        return None
    except requests.exceptions.ConnectionError:
        print(f"خطا در برقراری اتصال با {url} (ConnectionError).")
        return None
    except requests.exceptions.TooManyRedirects:
        print(f"تعداد تغییر مسیرها برای {url} بیش از حد مجاز بود (TooManyRedirects).")
        return None
    except requests.exceptions.RequestException as e:
        print(f"خطای کلی درخواست در دانلود {url}: {e}")
        return None
    except Exception as e:
        print(f"یک خطای پیش‌بینی نشده در هنگام دانلود {url}: {e}")
        return None
    finally:
        if 'response' in locals() and response: # اطمینان از بسته شدن اتصال
            response.close()


def save_page(url, content, directory):
    """
    محتوای دانلود شده را در یک فایل ذخیره می‌کند.
    """
    if not content:
        return

    if not os.path.exists(directory):
        try:
            os.makedirs(directory, exist_ok=True) # exist_ok=True از خطا در صورت وجود پوشه جلوگیری می‌کند
            print(f"پوشه {directory} ایجاد شد یا از قبل وجود داشت.")
        except OSError as e:
            print(f"خطا در ایجاد پوشه {directory}: {e}")
            return

    filename = sanitize_filename(url)
    filepath = os.path.join(directory, filename)

    try:
        with open(filepath, 'w', encoding='utf-8', errors='replace') as f:
            f.write(content)
        print(f"صفحه {url} در {filepath} ذخیره شد.")
    except IOError as e:
        print(f"خطا در ذخیره فایل {filepath}: {e}")
    except Exception as e:
        print(f"یک خطای پیش‌بینی نشده در هنگام ذخیره {filepath}: {e}")


def extract_links(html_content, base_url):
    """
    تمام لینک‌های معتبر را از محتوای HTML استخراج می‌کند.
    لینک‌ها را به URL های کامل تبدیل می‌کند و بر اساس دامنه فیلتر می‌کند.
    """
    links = set()
    if not html_content:
        return links
        
    soup = BeautifulSoup(html_content, 'lxml') # استفاده از lxml برای سرعت بیشتر
    
    base_domain = urlparse(base_url).netloc

    for anchor_tag in soup.find_all('a', href=True):
        href = anchor_tag['href'].strip()
        
        href = href.split('#')[0] # حذف fragment identifiers

        # نادیده گرفتن لینک‌های خاص
        if not href or href.startswith(('mailto:', 'tel:', 'javascript:', 'ftp:')) or href.endswith(('.pdf', '.jpg', '.jpeg', '.png', '.gif', '.zip', '.rar', '.exe', '.mp3', '.mp4', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx')):
            continue

        try:
            absolute_url = urljoin(base_url, href)
        except ValueError: # در صورت بروز مشکل در urljoin (مثلا href خیلی نامعتبر باشد)
            # print(f"خطا در ساخت URL مطلق از: '{href}' با base_url: '{base_url}'")
            continue
        
        # اعتبارسنجی اولیه URL
        if not validators.url(absolute_url):
            # print(f"لینک نامعتبر (ساختاری) یافت شد و رد شد: {absolute_url} (از href: '{href}')")
            continue

        parsed_absolute_url = urlparse(absolute_url)
        
        # فقط پروتکل‌های http و https
        if parsed_absolute_url.scheme not in ['http', 'https']:
            # print(f"پروتکل نامعتبر در لینک: {absolute_url}")
            continue
            
        # کنترل دامنه
        if ALLOWED_DOMAINS: # اگر لیست دامنه‌های مجاز پر است
            if parsed_absolute_url.netloc not in ALLOWED_DOMAINS:
                # print(f"لینک به دامنه خارجی {parsed_absolute_url.netloc} (خارج از لیست مجاز) رد شد: {absolute_url}")
                continue
        else: # اگر لیست دامنه‌های مجاز خالی است، فقط به دامنه سایت مبدا محدود شو
            if parsed_absolute_url.netloc != base_domain:
                # print(f"لینک به دامنه خارجی {parsed_absolute_url.netloc} (دامنه مبدا: {base_domain}) رد شد: {absolute_url}")
                continue
        
        links.add(absolute_url)
            
    return links


# --- تابع اصلی خزنده ---
def crawl_website(start_url, max_pages=MAX_PAGES_TO_CRAWL, allowed_domains_list=None):
    """
    تابع اصلی برای شروع خزش از یک URL.
    """
    global pages_crawled_count, urls_to_visit, visited_urls, ALLOWED_DOMAINS

    # بازنشانی متغیرهای سراسری برای هر اجرای crawl_website (اگر به صورت ماژول استفاده شود)
    pages_crawled_count = 0
    urls_to_visit = set()
    visited_urls = set()

    if allowed_domains_list is not None:
        ALLOWED_DOMAINS = allowed_domains_list
    
    # اعتبارسنجی URL شروع
    if not validators.url(start_url):
        print(f"URL شروع نامعتبر است: {start_url}")
        return

    initial_domain = urlparse(start_url).netloc
    if not ALLOWED_DOMAINS and initial_domain:
        print(f"محدود کردن خزش به دامنه اولیه: {initial_domain}")
        ALLOWED_DOMAINS = [initial_domain] # محدود کردن به دامنه URL شروع اگر لیست جهانی خالی باشد
    elif not initial_domain:
        print("خطا: دامنه URL شروع قابل تشخیص نیست. لطفاً URL معتبر وارد کنید.")
        return


    urls_to_visit.add(start_url)

    print(f"شروع خزش از: {start_url}")
    print(f"حداکثر صفحات برای خزش: {max_pages}")
    print(f"دامنه‌های مجاز: {ALLOWED_DOMAINS if ALLOWED_DOMAINS else 'فقط دامنه شروع'}")
    print(f"تاخیر بین درخواست‌ها: {REQUEST_DELAY} ثانیه")
    print("---")

    while urls_to_visit and pages_crawled_count < max_pages:
        # انتخاب URL از صف. برای کنترل بیشتر می‌توان از صف اولویت‌دار یا deque استفاده کرد.
        # set.pop() یک عنصر را به صورت غیر قطعی برمی‌دارد.
        current_url = urls_to_visit.pop()

        if current_url in visited_urls:
            continue

        print(f"\n({pages_crawled_count + 1}/{max_pages}) درحال پردازش: {current_url}")
        visited_urls.add(current_url)

        # بررسی مجدد دامنه قبل از دانلود (احتیاط بیشتر)
        parsed_current_url = urlparse(current_url)
        current_domain = parsed_current_url.netloc
        
        if ALLOWED_DOMAINS and current_domain not in ALLOWED_DOMAINS:
            print(f"دامنه {current_domain} خارج از لیست مجاز است. رد می‌شود: {current_url}")
            continue
        # این شرط اضافه شد تا اگر ALLOWED_DOMAINS در طول اجرا تغییر کرد (نباید بکند ولی برای اطمینان)
        # یا اگر در ابتدا خالی بود و بعد بر اساس initial_domain پر شد، درست عمل کند.
        elif not ALLOWED_DOMAINS and initial_domain and current_domain != initial_domain:
             print(f"دامنه {current_domain} خارج از دامنه اولیه ({initial_domain}) است. رد می‌شود: {current_url}")
             continue


        html_content = fetch_page(current_url)

        if html_content:
            # ذخیره سازی صفحه
            page_sub_dir = current_domain.replace('.', '_') # ایجاد زیرپوشه برای هر دامنه
            full_download_path = os.path.join(DOWNLOAD_DIR, page_sub_dir)
            save_page(current_url, html_content, full_download_path)
            
            # استخراج لینک‌های جدید
            if pages_crawled_count < max_pages: # فقط اگر هنوز جا برای خزش داریم لینک استخراج کن
                new_links = extract_links(html_content, current_url)
                # print(f"{len(new_links)} لینک در {current_url} یافت شد.")
                
                added_to_queue_count = 0
                for link in new_links:
                    if link not in visited_urls and link not in urls_to_visit:
                        # بررسی مجدد دامنه برای لینک‌های جدید قبل از افزودن به صف
                        parsed_link_domain = urlparse(link).netloc
                        if ALLOWED_DOMAINS and parsed_link_domain in ALLOWED_DOMAINS:
                            urls_to_visit.add(link)
                            added_to_queue_count +=1
                        elif not ALLOWED_DOMAINS and initial_domain and parsed_link_domain == initial_domain:
                            urls_to_visit.add(link)
                            added_to_queue_count +=1
                        # else:
                            # print(f"لینک {link} به دلیل عدم تطابق دامنه به صف اضافه نشد.")
                if added_to_queue_count > 0:
                    print(f"{added_to_queue_count} لینک جدید به صف اضافه شد.")
        
        # معرفی تاخیر
        # print(f"تاخیر {REQUEST_DELAY} ثانیه‌ای...") # این خط را می‌توانید حذف کنید تا خروجی تمیزتر باشد
        time.sleep(REQUEST_DELAY)

    print("\n--- گزارش نهایی خزش ---")
    if pages_crawled_count >= max_pages:
        print(f"به حداکثر تعداد صفحات برای خزش ({max_pages}) رسیدیم.")
    elif not urls_to_visit:
        print("دیگر لینکی برای بازدید در صف (در دامنه‌های مجاز) وجود ندارد.")
    
    print(f"تعداد کل صفحات دانلود شده: {pages_crawled_count}")
    print(f"تعداد کل URL های منحصربفرد بازدید شده: {len(visited_urls)}")
    print(f"تعداد URL های باقیمانده در صف: {len(urls_to_visit)}")
    print("--- خزش به پایان رسید ---")

# --- اجرای برنامه ---
if __name__ == "__main__":
    
    # --- تنظیمات برای اجرای تست ---
    # URL شروع برای خزش
    # test_start_url = "http://sajjadakbari.ir"
    # test_start_url = "https://www.python.org/"
    test_start_url = "https://virgool.io/" # یک سایت با محتوای فارسی و لینک‌های زیاد
    # test_start_url = "https://fa.wikipedia.org/wiki/%D8%B5%D9%81%D8%AD%D9%87%D9%94_%D8%A7%D8%B5%D9%84%DB%8C"

    # حداکثر تعداد صفحاتی که می‌خواهیم در این تست دانلود کنیم
    test_max_pages = 15

    # دامنه‌هایی که می‌خواهیم خزش شوند.
    # اگر این لیست را خالی بگذارید (مثلا `test_allowed_domains = []` یا `test_allowed_domains = None`)
    # خزنده فقط به دامنه‌ای که `test_start_url` به آن تعلق دارد محدود خواهد شد.
    # test_allowed_domains = ["sajjadakbari.ir"]
    # test_allowed_domains = ["python.org"]
    test_allowed_domains = ["virgool.io"] # برای تست ویرگول
    # test_allowed_domains = ["fa.wikipedia.org", "commons.wikimedia.org", "species.wikimedia.org"] # مثال برای ویکی‌پدیا و زیردامنه‌های مرتبط

    # بررسی اولیه برای URL شروع
    if not test_start_url or not validators.url(test_start_url):
        print("خطا: لطفاً یک test_start_url معتبر در کد وارد کنید.")
    else:
        # اطمینان از اینکه پوشه اصلی دانلود وجود دارد
        if not os.path.exists(DOWNLOAD_DIR):
            try:
                os.makedirs(DOWNLOAD_DIR)
                print(f"پوشه اصلی دانلود {DOWNLOAD_DIR} ایجاد شد.")
            except OSError as e:
                print(f"خطا در ایجاد پوشه اصلی دانلود {DOWNLOAD_DIR}: {e}")
                # اگر پوشه اصلی ایجاد نشود، ادامه نده
                exit() 
            
        crawl_website(start_url=test_start_url, 
                      max_pages=test_max_pages, 
                      allowed_domains_list=test_allowed_domains)
