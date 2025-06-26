import logging
import socket
import subprocess
import tempfile
import os
import json
import time
from typing import Dict, Optional, Tuple, Callable, List, Any
from urllib.parse import urlparse

# وابستگی‌های مورد نیاز
from config import ProxyConfig # برای دسترسی به تنظیمات پروتکل‌ها و مهلت زمانی
from config_validator import ConfigValidator # برای استخراج آدرس سرور

logger = logging.getLogger(__name__)

class ConnectionTester:
    """
    کلاس ConnectionTester مسئول تست اتصال کانفیگ‌ها به صورت دو مرحله‌ای:
    ۱. تست پورت سبک و سریع
    ۲. تست عملکردی با Mihomo (برای پروتکل‌های پشتیبانی شده)
    همچنین موقعیت جغرافیایی (پرچم و کشور) را به کانفیگ‌های فعال اضافه می‌کند.
    """
    def __init__(self, config: ProxyConfig, validator: ConfigValidator, get_location_func: Callable[[str], Tuple[str, str]]):
        """
        سازنده ConnectionTester.
        config: یک نمونه از ProxyConfig.
        validator: یک نمونه از ConfigValidator.
        get_location_func: تابعی که آدرس سرور را می‌گیرد و پرچم و کشور (str, str) را برمی‌گرداند.
                           این تابع از ConfigFetcher به عنوان GeoIPResolver تزریق می‌شود.
        """
        self.config = config
        self.validator = validator
        self.get_location = get_location_func # تابعی برای دریافت موقعیت جغرافیایی
        
        # پروتکل‌هایی که Mihomo به صورت مستقیم و ساده پشتیبانی می‌کند و می‌توانیم تست کنیم.
        # این لیست را می‌توان با توجه به نیاز و آزمایش‌های واقعی Mihomo به‌روزرسانی کرد.
        self.mihomo_testable_protocols = {
            "vmess://", "vless://", "trojan://", "ss://", "hysteria2://", "hysteria://", "tuic://", "juicity://",
            # Hysteria v1 معمولاً در Mihomo پشتیبانی می‌شود.
            # WireGuard و SSR ممکن است نیاز به پیکربندی پیچیده‌تر داشته باشند یا فقط به صورت کلاینت کار کنند،
            # بنابراین فعلاً برای تست با Mihomo در نظر نمی‌گیریم.
            # برای SSH، Anytls، Mieru، Snell، Warp هم معمولاً تست مستقیم با ابزار پراکسی پیچیده‌تر است.
        }
        logger.info("ConnectionTester با موفقیت مقداردهی اولیه شد.")

    def _ping_test(self, address: str, port: int, timeout: int = 5) -> bool:
        """
        تست پینگ سبک و سریع با بررسی باز بودن پورت TCP.
        از سوکت برای تلاش برای اتصال به IP:Port استفاده می‌کند.
        """
        if not address or not port:
            logger.debug(f"آدرس یا پورت برای تست پینگ نامعتبر است: {address}:{port}")
            return False
        try:
            # سعی می‌کند نام دامنه را به IP تبدیل کند
            ip = socket.gethostbyname(address)
            with socket.create_connection((ip, port), timeout=timeout) as sock:
                # اگر اتصال برقرار شود، یعنی پورت باز است
                logger.debug(f"تست پینگ موفق برای {address}:{port} (IP: {ip}).")
                return True
        except (socket.gaierror, socket.timeout, ConnectionRefusedError, OSError) as e:
            logger.debug(f"تست پینگ ناموفق برای {address}:{port}: {e}")
            return False
        except Exception as e:
            logger.error(f"خطای ناشناخته در تست پینگ برای {address}:{port}: {e}")
            return False

    def _test_with_mihomo(self, config_string: str) -> bool:
        """
        تست عملکردی یک کانفیگ با استفاده از Mihomo CLI.
        کانفیگ موقت ایجاد کرده، Mihomo را اجرا و لاگ‌ها را برای موفقیت بررسی می‌کند.
        """
        logger.debug(f"شروع تست Mihomo برای کانفیگ: {config_string[:min(len(config_string), 50)]}...")
        
        # Mihomo نیاز به یک فایل کانفیگ YAML دارد.
        # ما یک فایل موقت ایجاد می‌کنیم.
        mihomo_config_content = {
            "port": 7890,  # پورت داخلی Mihomo
            "log-level": "info",
            "mode": "rule",
            "proxies": [
                {
                    "name": "test-proxy",
                    "type": "external", # نوع خارجی برای کانفیگ‌های مستقیم
                    "url": config_string # Mihomo می‌تواند URLهای پراکسی را مستقیماً از اینجا بخواند
                }
            ],
            "proxy-groups": [
                {
                    "name": "proxy",
                    "type": "select",
                    "proxies": ["test-proxy"]
                }
            ],
            "rules": [
                "MATCH,proxy" # تمام ترافیک را از طریق پراکسی ارسال کن
            ]
        }

        temp_config_file_path = None
        try:
            # ایجاد یک فایل موقت برای کانفیگ Mihomo
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.yaml', encoding='utf-8') as temp_config_file:
                json.dump(mihomo_config_content, temp_config_file, indent=2) # از json.dump برای تولید YAML ساده استفاده می‌کنیم
                temp_config_file_path = temp_config_file.name
            
            # دستور اجرای Mihomo (فرض می‌کنیم mihomo در PATH موجود است)
            # -f: مشخص کردن فایل کانفیگ
            # -d: مشخص کردن دایرکتوری داده (برای لاگ‌ها و cache)
            # --no-autoclose: برای جلوگیری از بسته شدن فوری Mihomo در تست
            mihomo_command = [
                "mihomo", 
                "-f", temp_config_file_path, 
                "-d", tempfile.gettempdir(), # از یک دایرکتوری موقت برای داده‌ها استفاده کن
                "--no-autoclose"
            ]

            # اجرای Mihomo به عنوان یک فرآیند جداگانه
            # stdout و stderr به PIPE هدایت می‌شوند تا بتوانیم خروجی را بخوانیم
            # preexec_fn=os.setsid برای ایجاد یک session جدید و جلوگیری از بسته شدن Mihomo با بسته شدن پایتون
            process = subprocess.Popen(mihomo_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, preexec_fn=os.setsid)
            
            # صبر کردن کمی برای اینکه Mihomo بالا بیاید و کانفیگ را بارگذاری کند
            time.sleep(2) 

            # تلاش برای اتصال از طریق Mihomo
            # Mihomo پورت 7890 را برای SOCKS5 و HTTP/HTTPS باز می‌کند
            test_proxy_url = "http://127.0.0.1:7890" # پورت Mihomo

            # یک درخواست کوچک به یک سرور معروف ارسال کنید (مثلا Cloudflare's captive portal check)
            # این URL برای تست اتصال اینترنت بدون redirection است.
            test_target_url = "http://cp.cloudflare.com/"
            
            try:
                # استفاده از requests برای ارسال درخواست از طریق Mihomo
                test_response = requests.get(test_target_url, proxies={"http": test_proxy_url, "https": test_proxy_url}, timeout=self.config.REQUEST_TIMEOUT / 2)
                
                if test_response.status_code == 200:
                    logger.debug(f"تست Mihomo موفق برای کانفیگ: {config_string[:min(len(config_string), 50)]}...")
                    return True
                else:
                    logger.debug(f"تست Mihomo ناموفق برای کانفیگ (کد وضعیت: {test_response.status_code}): {config_string[:min(len(config_string), 50)]}...")
                    return False
            except requests.exceptions.RequestException as e:
                logger.debug(f"خطا در درخواست از طریق Mihomo برای کانفیگ '{config_string[:min(len(config_string), 50)]}...': {e}")
                return False
            finally:
                # اطمینان از terminate کردن Mihomo
                if process.poll() is None: # اگر فرآیند هنوز در حال اجراست
                    try:
                        os.killpg(os.getpgid(process.pid), 9) # Kill the process group
                        process.wait(timeout=1)
                    except OSError as e:
                        logger.debug(f"خطا در kill کردن فرآیند Mihomo: {e}")

        except FileNotFoundError:
            logger.error("اجرایی Mihomo یافت نشد. لطفاً مطمئن شوید که Mihomo در PATH سیستم شما نصب و قابل دسترس است.")
            return False
        except Exception as e:
            logger.error(f"خطا در اجرای تست Mihomo برای کانفیگ '{config_string[:min(len(config_string), 50)]}...': {e}", exc_info=True)
            return False
        finally:
            # پاکسازی فایل کانفیگ موقت
            if temp_config_file_path and os.path.exists(temp_config_file_path):
                os.remove(temp_config_file_path)

        return False

    def test_and_enrich_config(self, config_data: Dict[str, str]) -> Optional[Dict[str, str]]:
        """
        کانفیگ را تست می‌کند و در صورت فعال بودن، اطلاعات پرچم و کشور را اضافه می‌کند.
        این متد برای استفاده موازی طراحی شده است.
        config_data: دیکشنری حاوی 'config', 'protocol', 'canonical_id'.
        """
        config_string = config_data['config']
        protocol = config_data['protocol']
        
        # 1. تست پینگ/پورت سریع
        server_address = self.validator.get_server_address(config_string, protocol)
        parsed_url = urlparse(config_string)
        port = parsed_url.port # پورت را از URLparse استخراج کن
        
        if not server_address or not port:
            logger.debug(f"آدرس سرور یا پورت برای کانفیگ '{config_string[:min(len(config_string), 50)]}...' یافت نشد. تست پینگ/عملکردی نادیده گرفته شد.")
            return None

        if not self._ping_test(server_address, port):
            logger.debug(f"کانفیگ '{config_string[:min(len(config_string), 50)]}...' تست پینگ/پورت را رد کرد. نادیده گرفته شد.")
            return None

        # 2. تست عملکردی با Mihomo (فقط برای پروتکل‌های پشتیبانی شده)
        if protocol in self.mihomo_testable_protocols:
            if not self._test_with_mihomo(config_string):
                logger.debug(f"کانفیگ '{config_string[:min(len(config_string), 50)]}...' تست Mihomo را رد کرد. نادیده گرفته شد.")
                return None
            logger.debug(f"کانفیگ '{config_string[:min(len(config_string), 50)]}...' تست Mihomo را با موفقیت پشت سر گذاشت.")
        else:
            logger.debug(f"پروتکل '{protocol}' توسط Mihomo تست نمی‌شود. از تست Mihomo صرف نظر شد.")
            # اگر پروتکل توسط Mihomo تست نمی‌شود، بعد از تست پینگ آن را معتبر فرض می‌کنیم.

        # اگر هر دو تست (یا فقط تست پینگ) موفقیت آمیز بود، پرچم و کشور را اضافه می‌کنیم.
        flag, country = self.get_location(server_address) # استفاده از تابع تزریق شده
        
        enriched_config = {
            'config': config_string,
            'protocol': protocol,
            'flag': flag,
            'country': country,
            'canonical_id': config_data['canonical_id'] # شناسه کانونی را حفظ می‌کنیم
        }
        logger.debug(f"کانفیگ '{config_string[:min(len(config_string), 50)]}...' با موفقیت تست و غنی‌سازی شد.")
        return enriched_config

