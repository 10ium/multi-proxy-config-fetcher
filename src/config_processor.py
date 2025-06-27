import logging
from typing import Dict, Optional, Any

# **تغییر یافته**: وارد کردن کلاس‌ها بدون پیشوند 'src.'
from config import ProxyConfig 
from config_validator import ConfigValidator 

logger = logging.getLogger(__name__)

class ConfigProcessor:
    """
    کلاس ConfigProcessor مسئول پردازش یک کانفیگ خام است:
    نرمال‌سازی، پاکسازی، اعتبارسنجی اولیه، و تولید شناسه کانونی.
    این کلاس مسئولیتی در قبال دریافت موقعیت جغرافیایی یا تکراری‌زدایی نهایی ندارد.
    """
    def __init__(self, config: ProxyConfig, validator: ConfigValidator):
        """
        سازنده ConfigProcessor.
        config: یک نمونه از ProxyConfig حاوی تنظیمات.
        validator: یک نمونه از ConfigValidator برای توابع اعتبارسنجی.
        """
        self.config = config
        self.validator = validator
        logger.info("ConfigProcessor با موفقیت مقداردهی اولیه شد.")

    def process_raw_config(self, raw_config_string: str) -> Optional[Dict[str, str]]:
        """
        پردازش یک رشته کانفیگ خام:
        - نرمال‌سازی پروتکل (مانند hy2:// به hysteria2://)
        - شناسایی پروتکل اصلی
        - بررسی فعال بودن پروتکل
        - پاکسازی و اعتبارسنجی فرمت کانفیگ
        - تولید شناسه کانونی (Canonical ID)
        در صورت موفقیت، یک دیکشنری حاوی کانفیگ پردازش شده و شناسه کانونی را برمی‌گرداند؛
        در غیر این صورت None.
        """
        if not raw_config_string:
            logger.debug("رشته کانفیگ ورودی خالی است. نادیده گرفته شد.")
            return None

        config_string_for_processing = raw_config_string 
        
        # نرمال‌سازی پروتکل‌های مستعار قبل از پردازش
        if config_string_for_processing.startswith('hy2://'):
            config_string_for_processing = self.validator.normalize_hysteria2_protocol(config_string_for_processing)
        elif config_string_for_processing.startswith('hy1://'):
            config_string_for_processing = config_string_for_processing.replace('hy1://', 'hysteria://', 1) 

        actual_protocol = None # پروتکل شناسایی شده برای کانفیگ
        found_protocol_match = False

        # شناسایی پروتکل اصلی از روی رشته کانفیگ (شامل بررسی نام‌های مستعار)
        for proto_prefix, proto_info in self.config.SUPPORTED_PROTOCOLS.items():
            if config_string_for_processing.startswith(proto_prefix):
                actual_protocol = proto_prefix
                found_protocol_match = True
                break
            for alias in proto_info.get('aliases', []):
                if config_string_for_processing.startswith(alias):
                    actual_protocol = proto_prefix 
                    # نرمال‌سازی رشته کانفیگ به پروتکل اصلی
                    config_string_for_processing = config_string_for_processing.replace(alias, proto_prefix, 1) 
                    found_protocol_match = True
                    break
            if found_protocol_match:
                break

        if not found_protocol_match:
            logger.debug(f"پروتکل برای کانفیگ خام شناسایی نشد: '{raw_config_string[:min(len(raw_config_string), 50)]}...'. نادیده گرفته شد.")
            return None

        # بررسی فعال بودن پروتکل در تنظیمات کلی
        if not self.config.is_protocol_enabled(actual_protocol):
            logger.debug(f"پروتکل '{actual_protocol}' فعال نیست. کانفیگ نادیده گرفته شد: '{config_string_for_processing[:min(len(config_string_for_processing), 50)]}...'.")
            return None 

        # پاکسازی‌های پروتکل خاص
        if actual_protocol == "vmess://":
            config_string_for_processing = self.validator.clean_vmess_config(config_string_for_processing)
        elif actual_protocol == "ssr://":
            config_string_for_processing = self.validator.clean_ssr_config(config_string_for_processing)

        # پاکسازی عمومی کاراکترهای اضافی
        clean_config = self.validator.clean_config(config_string_for_processing)

        # اعتبارسنجی دقیق پروتکل
        if self.validator.validate_protocol_config(clean_config, actual_protocol):
            # تولید شناسه کانونی
            canonical_id = self.validator.get_canonical_id(clean_config, actual_protocol)

            if canonical_id is None:
                logger.debug(f"شناسه کانونی برای کانفیگ '{actual_protocol}' تولید نشد. نادیده گرفته شد: '{clean_config[:min(len(clean_config), 50)]}...'.")
                return None

            # اطلاعات پرچم و کشور دیگر اینجا اضافه نمی‌شوند
            logger.debug(f"کانفیگ پردازش شد: '{clean_config[:min(len(clean_config), 50)]}...' (ID: {canonical_id[:min(len(canonical_id), 20)]}...).")

            return {
                'config': clean_config, 
                'protocol': actual_protocol,
                'canonical_id': canonical_id 
            }
        else:
            logger.debug(f"اعتبارسنجی پروتکل '{actual_protocol}' برای کانفیگ '{clean_config[:min(len(clean_config), 50)]}...' ناموفق بود. نادیده گرفته شد.")

        logger.debug(f"کانفیگ '{raw_config_string[:min(len(raw_config_string), 50)]}...' با هیچ پروتکل فعال یا معتبری مطابقت نداشت یا پردازش نشد. نادیده گرفته شد.")
        return None

