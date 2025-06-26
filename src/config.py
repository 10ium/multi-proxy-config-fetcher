from typing import Dict, List, Optional
from datetime import datetime
import re
from urllib.parse import urlparse
from dataclasses import dataclass
import logging
import os 

# فرض بر این است که user_settings.py در دسترس است.
# این فایل شامل متغیرهایی مانند SOURCE_URLS، USE_MAXIMUM_POWER و ENABLED_PROTOCOLS است.
from user_settings import SOURCE_URLS, USE_MAXIMUM_POWER, SPECIFIC_CONFIG_COUNT, ENABLED_PROTOCOLS, MAX_CONFIG_AGE_DAYS

# پیکربندی لاگ‌گیری (این پیکربندی باید در main.py انجام شود و در اینجا فقط لاگر ایجاد می‌شود)
logger = logging.getLogger(__name__)

@dataclass
class ChannelMetrics:
    """
    کلاس ChannelMetrics برای نگهداری و مدیریت معیارهای عملکرد هر کانال منبع.
    """
    total_configs: int = 0  # تعداد کل کانفیگ‌های پیدا شده (شامل نامعتبرها و تکراری‌ها)
    valid_configs: int = 0  # تعداد کانفیگ‌های معتبر و منحصر به فرد (تست شده و فیلتر شده)
    unique_configs: int = 0 # تعداد کانفیگ‌های منحصر به فرد در سطح کلی (بین همه کانال‌ها)
    avg_response_time: float = 0 # میانگین زمان پاسخگویی کانال
    last_success_time: Optional[datetime] = None # آخرین زمان موفقیت‌آمیز واکشی
    fail_count: int = 0     # تعداد دفعات شکست در واکشی
    success_count: int = 0  # تعداد دفعات موفقیت در واکشی
    overall_score: float = 0.0 # امتیاز کلی کانال بر اساس عملکرد
    protocol_counts: Dict[str, int] = None # شمارش کانفیگ‌ها بر اساس پروتکل (در این کانال)

    def __post_init__(self):
        # اطمینان از مقداردهی اولیه protocol_counts اگر None باشد.
        if self.protocol_counts is None:
            self.protocol_counts = {}

class ChannelConfig:
    """
    کلاس ChannelConfig برای نگهداری پیکربندی و وضعیت یک کانال منبع خاص.
    """
    def __init__(self, url: str):
        self.url = self._validate_url(url) # اعتبارسنجی و ذخیره URL کانال
        self.enabled = True                # وضعیت فعال/غیرفعال بودن کانال
        self.metrics = ChannelMetrics()    # معیارهای عملکرد کانال
        self.is_telegram = bool(re.match(r'^https://t\.me/s/', self.url)) # بررسی تلگرام بودن کانال
        self.error_count = 0               # تعداد خطاهای متوالی (برای ردیابی)
        self.last_check_time = None        # آخرین زمان بررسی کانال
        self.next_check_time: Optional[datetime] = None # زمان بعدی که باید کانال بررسی شود (برای Smart Retry)
        self.retry_level: int = 0         # سطح تلاش مجدد برای Smart Retry (0: عادی، 1: 3 روز، 2: 1 هفته و...)
        logger.debug(f"شیء ChannelConfig برای URL: '{self.url}' ایجاد شد.") # لاگ DEBUG

    def _validate_url(self, url: str) -> str:
        """
        اعتبارسنجی فرمت URL و پروتکل آن.
        """
        if not url or not isinstance(url, str):
            raise ValueError("URL نامعتبر است.")
        url = url.strip()
        # اضافه کردن تمامی پیشوندهای پروتکل‌های پشتیبانی شده برای اعتبارسنجی اولیه URL
        valid_protocols_prefixes = (
            'http://', 'https://', 'ssconf://', 
            'ssr://', 'mieru://', 'snell://', 'anytls://', 'ssh://', 'juicity://',
            'hysteria://', 'warp://', 'wireguard://', 'hysteria2://', 'vless://', 
            'vmess://', 'ss://', 'trojan://', 'tuic://'
        )
        if not url.startswith(valid_protocols_prefixes):
            raise ValueError(f"پروتکل URL نامعتبر است: {url}")
        return url

    def calculate_overall_score(self):
        """
        محاسبه امتیاز کلی کانال بر اساس قابلیت اطمینان، کیفیت، منحصر به فرد بودن و زمان پاسخگویی.
        """
        try:
            total_attempts = max(1, self.metrics.success_count + self.metrics.fail_count)
            reliability_score = (self.metrics.success_count / total_attempts) * 35 # امتیاز قابلیت اطمینان (تا 35%)

            total_configs = max(1, self.metrics.total_configs)
            # valid_configs و unique_configs در حال حاضر در ChannelMetrics به طور دقیق بروز نمی‌شوند
            # اینها نشان‌دهنده کانفیگ‌های *یافت شده اولیه* هستند، نه لزوما تست شده/فیلتر شده.
            quality_score = (self.metrics.valid_configs / total_configs) * 25 if total_configs > 0 else 0 # امتیاز کیفیت (تا 25%)

            # uniqueness_score در حال حاضر به دلیل پیچیدگی ردیابی منبع کانفیگ نهایی به کانال، دقیق نیست.
            # برای حفظ منطق، آن را بر اساس total_configs محاسبه می‌کنیم.
            uniqueness_score = (self.metrics.unique_configs / max(1, self.metrics.valid_configs)) * 25 if self.metrics.valid_configs > 0 else 0 # امتیاز منحصر به فرد بودن (تا 25%)

            response_score = 15 # امتیاز زمان پاسخگویی (تا 15%)
            if self.metrics.avg_response_time > 0:
                response_score = max(0, min(15, 15 * (1 - (self.metrics.avg_response_time / 10))))

            self.metrics.overall_score = round(reliability_score + quality_score + uniqueness_score + response_score, 2)
            logger.debug(f"امتیاز کلی برای کانال '{self.url}' محاسبه شد: {self.metrics.overall_score}")
        except Exception as e:
            logger.error(f"خطا در محاسبه امتیاز برای کانال '{self.url}': {str(e)}")
            self.metrics.overall_score = 0.0

class ProxyConfig:
    """
    کلاس ProxyConfig برای مدیریت تنظیمات و منابع کلی پراکسی.
    """
    def __init__(self, initial_source_urls: List[str] = None):
        logger.info("در حال بارگذاری و پیکربندی ProxyConfig...")
        self.use_maximum_power = USE_MAXIMUM_POWER           # آیا از حداکثر قدرت واکشی استفاده شود؟
        self.specific_config_count = SPECIFIC_CONFIG_COUNT   # تعداد کانفیگ‌های مورد نیاز در صورت عدم استفاده از حداکثر قدرت
        self.MAX_CONFIG_AGE_DAYS = MAX_CONFIG_AGE_DAYS       # حداکثر عمر کانفیگ‌های معتبر

        logger.info(f"حالت واکشی: {'حداکثر قدرت' if self.use_maximum_power else f'تعداد مشخص ({self.specific_config_count} کانفیگ)'}")
        logger.info(f"حداکثر عمر کانفیگ‌ها: {self.MAX_CONFIG_AGE_DAYS} روز.")

        # بارگذاری URLهای منبع اولیه از user_settings.py
        if initial_source_urls is None:
            initial_urls_list = SOURCE_URLS
            logger.info(f"URLهای منبع اولیه از 'user_settings.py' بارگذاری شدند. ({len(initial_urls_list)} URL)")
        else:
            initial_urls_list = initial_source_urls
            logger.info(f"URLهای منبع اولیه به صورت دستی ارائه شدند. ({len(initial_urls_list)} URL)")


        initial_channel_configs = [ChannelConfig(url=url) for url in initial_urls_list]
        # حذف URLهای تکراری از لیست اولیه کانال‌ها
        self.SOURCE_URLS = self._remove_duplicate_urls(initial_channel_configs) 
        logger.info(f"پس از حذف تکراری‌ها، {len(self.SOURCE_URLS)} کانال منحصر به فرد برای پردازش باقی ماند.")

        self.SUPPORTED_PROTOCOLS = self._initialize_protocols() # مقداردهی اولیه پروتکل‌های پشتیبانی شده
        logger.info(f"پروتکل‌های پشتیبانی شده اولیه شدند. ({len(self.SUPPORTED_PROTOCOLS)} پروتکل)")
        logger.debug(f"وضعیت فعال بودن پروتکل‌ها: {self._get_protocol_enablement_status()}")

        self._initialize_settings()                             # مقداردهی اولیه سایر تنظیمات
        self._set_smart_limits()                                # تنظیم محدودیت‌های هوشمند
        logger.info("پیکربندی ProxyConfig با موفقیت انجام شد.")

    def _get_protocol_enablement_status(self) -> str:
        """برای لاگ‌گیری، وضعیت فعال بودن پروتکل‌ها را برمی‌گرداند."""
        status = []
        for p, info in self.SUPPORTED_PROTOCOLS.items():
            status.append(f"{p.replace('://', '')}: {'فعال' if info['enabled'] else 'غیرفعال'}")
        return ", ".join(status)

    def _initialize_protocols(self) -> Dict:
        """
        تعریف تمامی پروتکل‌های پراکسی پشتیبانی شده به همراه اولویت و نام‌های مستعار.
        """
        return {
            "wireguard://": {"priority": 1, "aliases": [], "enabled": ENABLED_PROTOCOLS.get("wireguard://", False), "min_configs": 0, "max_configs": 0, "flexible_max": False},
            "hysteria2://": {"priority": 2, "aliases": ["hy2://"], "enabled": ENABLED_PROTOCOLS.get("hysteria2://", False), "min_configs": 0, "max_configs": 0, "flexible_max": False},
            "vless://": {"priority": 2, "aliases": [], "enabled": ENABLED_PROTOCOLS.get("vless://", False), "min_configs": 0, "max_configs": 0, "flexible_max": False},
            "vmess://": {"priority": 1, "aliases": [], "enabled": ENABLED_PROTOCOLS.get("vmess://", False), "min_configs": 0, "max_configs": 0, "flexible_max": False}, 
            "ss://": {"priority": 2, "aliases": [], "enabled": ENABLED_PROTOCOLS.get("ss://", False), "min_configs": 0, "max_configs": 0, "flexible_max": False},
            "trojan://": {"priority": 2, "aliases": [], "enabled": ENABLED_PROTOCOLS.get("trojan://", False), "min_configs": 0, "max_configs": 0, "flexible_max": False},
            "tuic://": {"priority": 1, "aliases": [], "enabled": ENABLED_PROTOCOLS.get("tuic://", False), "min_configs": 0, "max_configs": 0, "flexible_max": False},
            "ssr://": {"priority": 3, "aliases": [], "enabled": ENABLED_PROTOCOLS.get("ssr://", False), "min_configs": 0, "max_configs": 0, "flexible_max": False},
            "mieru://": {"priority": 1, "aliases": [], "enabled": ENABLED_PROTOCOLS.get("mieru://", False), "min_configs": 0, "max_configs": 0, "flexible_max": False},
            "snell://": {"priority": 2, "aliases": [], "enabled": ENABLED_PROTOCOLS.get("snell://", False), "min_configs": 0, "max_configs": 0, "flexible_max": False},
            "anytls://": {"priority": 2, "aliases": [], "enabled": ENABLED_PROTOCOLS.get("anytls://", False), "min_configs": 0, "max_configs": 0, "flexible_max": False},
            "ssh://": {"priority": 3, "aliases": [], "enabled": ENABLED_PROTOCOLS.get("ssh://", False), "min_configs": 0, "max_configs": 0, "flexible_max": False},
            "juicity://": {"priority": 1, "aliases": [], "enabled": ENABLED_PROTOCOLS.get("juicity://", False), "min_configs": 0, "max_configs": 0, "flexible_max": False},
            "hysteria://": {"priority": 1, "aliases": ["hy1://"], "enabled": ENABLED_PROTOCOLS.get("hysteria://", False), "min_configs": 0, "max_configs": 0, "flexible_max": False}, # Hysteria 1
            "warp://": {"priority": 4, "aliases": [], "enabled": ENABLED_PROTOCOLS.get("warp://", False), "min_configs": 0, "max_configs": 0, "flexible_max": False} # Cloudflare WARP
        }

    def _initialize_settings(self):
        """
        مقداردهی اولیه تنظیمات مختلف از جمله محدودیت‌ها و مسیرهای فایل‌های خروجی.
        """
        self.CHANNEL_RETRY_LIMIT = min(10, max(1, 5))    # حداکثر تلاش مجدد برای یک کانال
        self.CHANNEL_ERROR_THRESHOLD = min(0.9, max(0.1, 0.7)) # آستانه خطا برای غیرفعال کردن کانال

        # مسیرهای خروجی جدید برای سازماندهی بهتر فایل‌ها
        self.OUTPUT_DIR = 'subs'                          # پوشه اصلی برای تمامی خروجی‌ها
        self.TEXT_OUTPUT_DIR = os.path.join(self.OUTPUT_DIR, 'text') # پوشه برای فایل‌های متنی عادی
        self.BASE64_OUTPUT_DIR = os.path.join(self.OUTPUT_DIR, 'base64') # پوشه برای فایل‌های Base64 شده
        self.SINGBOX_OUTPUT_DIR = os.path.join(self.OUTPUT_DIR, 'singbox') # پوشه برای فایل‌های Singbox JSON

        self.OUTPUT_FILE = os.path.join(self.TEXT_OUTPUT_DIR, 'proxy_configs.txt') # فایل اصلی کانفیگ‌های متنی
        self.STATS_FILE = os.path.join(self.OUTPUT_DIR, 'channel_stats.json') # فایل آمار کانال‌ها (در پوشه اصلی subs)

        self.MAX_RETRIES = min(10, max(1, 5))             # حداکثر تلاش مجدد برای درخواست‌های HTTP
        self.RETRY_DELAY = min(60, max(5, 15))            # تاخیر بین تلاش‌های مجدد (ثانیه)
        self.REQUEST_TIMEOUT = min(120, max(10, 60))      # مهلت زمانی برای درخواست‌های HTTP (ثانیه)
        logger.info(f"تنظیمات واکشی: مهلت زمانی={self.REQUEST_TIMEOUT}s، حداکثر تلاش مجدد HTTP={self.MAX_RETRIES}، تاخیر تلاش مجدد={self.RETRY_DELAY}s.")

        self.HEADERS = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }

    def _set_smart_limits(self):
        """
        تنظیم محدودیت‌های هوشمند برای تعداد کانفیگ‌ها بر اساس حالت "حداکثر قدرت" یا "تعداد مشخص".
        min_configs و max_configs در SUPPORTED_PROTOCOLS اکنون بر اساس SPECIFIC_CONFIG_COUNT تنظیم می‌شوند.
        """
        if self.use_maximum_power:
            self._set_maximum_power_mode()
            logger.info("محدودیت‌ها برای حالت 'حداکثر قدرت' تنظیم شدند.")
        else:
            self._set_specific_count_mode()
            logger.info("محدودیت‌ها برای حالت 'تعداد مشخص' تنظیم شدند.")

    def _set_maximum_power_mode(self):
        """
        تنظیم محدودیت‌ها برای حالت "حداکثر قدرت".
        در این حالت، سیستم سعی می‌کند تا حد ممکن کانفیگ جمع‌آوری کند.
        min_configs و max_configs برای هر پروتکل روی مقادیر بسیار بالا تنظیم می‌شوند.
        """
        max_configs_val = 10000 

        for protocol in self.SUPPORTED_PROTOCOLS:
            if self.SUPPORTED_PROTOCOLS[protocol]["enabled"]:
                self.SUPPORTED_PROTOCOLS[protocol].update({
                    "min_configs": 1,          # حداقل 1 کانفیگ برای هر پروتکل (اگر یافت شود)
                    "max_configs": max_configs_val, # حداکثر کانفیگ برای هر پروتکل در این حالت
                    "flexible_max": True       # حداکثر تعداد منعطف است (تا max_configs)
                })
            else: # اگر پروتکل غیرفعال است، محدودیت‌ها 0 باشند
                self.SUPPORTED_PROTOCOLS[protocol].update({"min_configs": 0, "max_configs": 0, "flexible_max": False})

        self.MIN_CONFIGS_PER_CHANNEL = 1    # حداقل کانفیگ در هر کانال
        self.MAX_CONFIGS_PER_CHANNEL = max_configs_val # حداکثر کانفیگ در هر کانال
        self.MAX_RETRIES = min(10, max(1, 10)) # افزایش تلاش مجدد HTTP
        self.CHANNEL_RETRY_LIMIT = min(10, max(1, 10)) # افزایش تلاش مجدد کانال
        self.REQUEST_TIMEOUT = min(120, max(30, 90)) # افزایش مهلت زمانی درخواست

    def _set_specific_count_mode(self):
        """
        تنظیم محدودیت‌ها برای حالت "تعداد مشخص".
        سیستم سعی می‌کند تعداد کانفیگ‌ها را به specific_config_count نزدیک کند.
        این تعداد بین پروتکل‌های فعال تقسیم می‌شود.
        """
        if self.specific_config_count <= 0:
            self.specific_config_count = 50 # پیش فرض اگر عدد مشخص شده نامعتبر بود

        enabled_protocols_count = sum(1 for p in self.SUPPORTED_PROTOCOLS if self.SUPPORTED_PROTOCOLS[p]["enabled"])
        if enabled_protocols_count == 0:
            logger.warning("هیچ پروتکل فعالی یافت نشد. تنظیمات تعداد مشخص اعمال نمی‌شوند.")
            return

        # توزیع تقریبی بین پروتکل‌های فعال، با حداقل 1 کانفیگ برای هر پروتکل فعال
        base_per_protocol = max(1, self.specific_config_count // enabled_protocols_count)

        for protocol in self.SUPPORTED_PROTOCOLS:
            if self.SUPPORTED_PROTOCOLS[protocol]["enabled"]:
                self.SUPPORTED_PROTOCOLS[protocol].update({
                    "min_configs": max(1, base_per_protocol // 2), # حداقل 50% از سهم پایه پروتکل
                    "max_configs": base_per_protocol * 2, # تا 2 برابر سهم پایه
                    "flexible_max": True       # حداکثر تعداد منعطف است
                })
                # اطمینان از اینکه max_configs از یک مقدار منطقی تجاوز نکند (مثلا 1000)
                if self.SUPPORTED_PROTOCOLS[protocol]["max_configs"] > 1000:
                    self.SUPPORTED_PROTOCOLS[protocol]["max_configs"] = 1000
            else: # اگر پروتکل غیرفعال است، محدودیت‌ها 0 باشند
                self.SUPPORTED_PROTOCOLS[protocol].update({"min_configs": 0, "max_configs": 0, "flexible_max": False})

        self.MIN_CONFIGS_PER_CHANNEL = 1    # حداقل کانفیگ در هر کانال
        self.MAX_CONFIGS_PER_CHANNEL = min(max(5, self.specific_config_count // 2), 1000)

    def _normalize_url(self, url: str) -> str:
        """
        نرمال‌سازی یک URL به فرمت یکسان برای مقایسه (برای حذف تکراری‌ها)،
        به ویژه برای کانال‌های تلگرام.
        """
        try:
            if not url:
                raise ValueError("URL خالی است.")

            url = url.strip()
            # تبدیل ssconf:// به https://
            if url.startswith('ssconf://'):
                url = url.replace('ssconf://', 'https://', 1)

            parsed = urlparse(url)
            # اگر طرح یا netloc وجود نداشت، اما با t.me/ شروع می‌شد، آن را به عنوان یک لینک تلگرام خام در نظر بگیرید
            if not parsed.scheme or not parsed.netloc:
                if url.startswith('t.me/'):
                    channel_name = url.split('/')[-1].strip('/').lower()
                    return f"telegram:{channel_name}"
                raise ValueError("فرمت URL نامعتبر است.")

            path = parsed.path.rstrip('/') # حذف اسلش اضافی از انتهای مسیر

            # نرمال‌سازی خاص برای کانال‌های تلگرام (t.me/s/channelname یا t.me/channelname)
            if parsed.netloc.startswith('t.me'):
                channel_name = parsed.path.strip('/').lower()
                if channel_name.startswith('s/'): # اگر t.me/s/ بود، 's/' را حذف کنید
                    channel_name = channel_name[2:]
                return f"telegram:{channel_name}"

            # برای سایر URLها، طرح، netloc و مسیر تمیز شده را برگردانید
            return f"{parsed.scheme}://{parsed.netloc}{path}"
        except Exception as e:
            logger.error(f"خطا در نرمال‌سازی URL برای '{url}': {str(e)}")
            raise

    def _remove_duplicate_urls(self, channel_configs: List[ChannelConfig]) -> List[ChannelConfig]:
        """
        حذف URLهای تکراری کانال از لیست اولیه بر اساس URLهای نرمال شده.
        """
        logger.debug("در حال حذف URLهای تکراری از لیست کانال‌های اولیه...")
        try:
            seen_normalized_urls = {} # دیکشنری برای نگهداری URLهای نرمال شده دیده شده
            unique_configs = []      # لیست کانفیگ‌های منحصر به فرد

            for config_item in channel_configs:
                # اطمینان از اینکه آیتم یک شیء ChannelConfig است
                if not isinstance(config_item, ChannelConfig):
                    logger.warning(f"کانفیگ نامعتبر نادیده گرفته شد: {config_item}")
                    continue

                try:
                    normalized_url = self._normalize_url(config_item.url) # نرمال‌سازی URL
                    if normalized_url not in seen_normalized_urls:
                        seen_normalized_urls[normalized_url] = True
                        unique_configs.append(config_item)
                    else:
                        logger.debug(f"URL تکراری یافت شد و حذف شد: '{config_item.url}' (نرمال شده: {normalized_url})")
                except ValueError as ve:
                    logger.warning(f"URL کانال نامعتبر در هنگام حذف تکراری‌ها نادیده گرفته شد: '{config_item.url}' - {str(ve)}")
                except Exception as e:
                    logger.error(f"خطای ناشناخته در هنگام نرمال‌سازی URL در حذف تکراری‌ها: '{config_item.url}' - {str(e)}")
                    continue

            if not unique_configs:
                # اگر هیچ کانال معتبری یافت نشد، فایل کانفیگ خروجی را خالی ایجاد کنید.
                # (این مسئولیت به OutputManager منتقل شده است، اما برای جلوگیری از خطاهای اولیه این را نگه می‌داریم)
                # self.save_empty_config_file()
                logger.error("هیچ منبع معتبری یافت نشد.")
                return []

            logger.debug(f"حذف تکراری‌ها کامل شد. {len(unique_configs)} کانال منحصر به فرد باقی ماند.")
            return unique_configs
        except Exception as e:
            logger.error(f"خطا در حذف URLهای تکراری: {str(e)}")
            # self.save_empty_config_file()
            return []

    def is_protocol_enabled(self, protocol: str) -> bool:
        """
        بررسی می‌کند که آیا یک پروتکل (یا نام مستعار آن) در پیکربندی فعال است یا خیر.
        """
        try:
            if not protocol:
                return False

            protocol = protocol.lower().strip()

            if protocol in self.SUPPORTED_PROTOCOLS:
                return self.SUPPORTED_PROTOCOLS[protocol].get("enabled", False)

            for main_protocol, info in self.SUPPORTED_PROTOCOLS.items():
                if protocol in info.get("aliases", []):
                    return info.get("enabled", False)

            return False
        except Exception as e:
            logger.debug(f"خطا در بررسی فعال بودن پروتکل '{protocol}': {str(e)}")
            return False

    def get_enabled_channels(self) -> List[ChannelConfig]:
        """
        یک لیست از اشیاء ChannelConfig فعال فعلی را برمی‌گرداند.
        """
        channels = [channel for channel in self.SOURCE_URLS if channel.enabled]
        if not channels:
            logger.warning("هیچ کانال فعالی یافت نشد.")
        return channels

    def update_channel_stats(self, channel: ChannelConfig, success: bool, response_time: float = 0):
        """
        آمارهای یک کانال معین را بر اساس موفقیت عملیات واکشی آن به‌روز می‌کند.
        همچنین امتیاز کلی را محاسبه می‌کند و در صورت لزوم کانال را غیرفعال می‌کند.
        """
        if success:
            channel.metrics.success_count += 1
            channel.metrics.last_success_time = datetime.now()
            channel.error_count = 0 # بازنشانی شمارنده خطا در صورت موفقیت
        else:
            channel.metrics.fail_count += 1
            channel.error_count += 1 # افزایش شمارنده خطاهای متوالی

        if response_time > 0:
            if channel.metrics.avg_response_time == 0:
                channel.metrics.avg_response_time = response_time
            else:
                channel.metrics.avg_response_time = (channel.metrics.avg_response_time * 0.7) + (response_time * 0.3)

        channel.calculate_overall_score()

        if channel.metrics.overall_score < 25:
            if channel.enabled:
                channel.enabled = False
                logger.warning(f"کانال '{channel.url}' به دلیل امتیاز کلی پایین ({channel.metrics.overall_score}) غیرفعال شد.")
        elif not channel.enabled and channel.metrics.overall_score >= 50:
            channel.enabled = True
            logger.info(f"کانال '{channel.url}' به دلیل بهبود امتیاز کلی ({channel.metrics.overall_score}) دوباره فعال شد.")

    def adjust_protocol_limits(self, channel: ChannelConfig):
        """
        محدودیت‌های خاص پروتکل را بر اساس تعداد واقعی کانفیگ‌های یافت شده در یک کانال
        هنگامی که در حالت حداکثر قدرت نیست، تنظیم می‌کند.
        (این متد در حال حاضر کمتر استفاده می‌شود زیرا تنظیمات پروتکل‌ها جهانی است، اما برای آینده باقی می‌ماند.)
        """
        if self.use_maximum_power:
            return

        for protocol in channel.metrics.protocol_counts:
            if protocol in self.SUPPORTED_PROTOCOLS:
                current_count = channel.metrics.protocol_counts[protocol]
                if current_count > 0:
                    self.SUPPORTED_PROTOCOLS[protocol]["min_configs"] = min(
                        self.SUPPORTED_PROTOCOLS[protocol]["min_configs"],
                        current_count
                    )

