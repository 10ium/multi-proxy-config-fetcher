import logging
import re
import socket # برای حل کردن دامنه به IP
from typing import List, Dict, Any, Optional, Union
from urllib.parse import urlparse

# وابستگی‌های مورد نیاز
from config import ProxyConfig # برای دسترسی به تنظیمات کلی
from config_validator import ConfigValidator # برای استخراج آدرس سرور

logger = logging.getLogger(__name__)

class ConfigFilter:
    """
    کلاس ConfigFilter مسئول اعمال قوانین فیلترینگ پیشرفته بر روی لیست کانفیگ‌ها است.
    این شامل فیلتر بر اساس کلمات کلیدی، کشور، پروتکل، و لیست سیاه IP/دامنه می‌شود.
    """
    def __init__(self, config: ProxyConfig, validator: ConfigValidator):
        """
        سازنده ConfigFilter.
        config: یک نمونه از ProxyConfig برای دسترسی به تنظیمات عمومی.
        validator: یک نمونه از ConfigValidator برای استخراج آدرس‌های سرور.
        """
        self.config = config
        self.validator = validator
        logger.info("ConfigFilter با موفقیت مقداردهی اولیه شد.")

    def _match_keyword(self, text: str, keywords: List[str]) -> bool:
        """بررسی می‌کند که آیا متن شامل هر یک از کلمات کلیدی است یا خیر (case-insensitive)."""
        if not keywords:
            return False
        text_lower = text.lower()
        return any(keyword.lower() in text_lower for keyword in keywords)

    def _is_ip_in_range(self, ip_address: str, ip_range: str) -> bool:
        """
        بررسی می‌کند که آیا یک آدرس IP در یک رنج IP مشخص (مثلا CIDR) قرار دارد یا خیر.
        پیچیدگی کامل مدیریت رنج‌های IP در اینجا خارج از محدوده است،
        اما می‌توان آن را با کتابخانه‌هایی مانند `ipaddress` گسترش داد.
        فعلا برای IPهای دقیق یا CIDRهای ساده‌تر پیاده‌سازی می‌شود.
        """
        try:
            # اگر کتابخانه ipaddress در دسترس باشد، بهتر است از آن استفاده کنید.
            # import ipaddress
            # return ipaddress.ip_address(ip_address) in ipaddress.ip_network(ip_range)

            # پیاده‌سازی ساده برای IPهای دقیق و CIDR های /24
            if '/' in ip_range:
                range_ip, cidr_prefix_str = ip_range.split('/')
                cidr_prefix = int(cidr_prefix_str)
                
                # برای سادگی فعلا فقط /24 و /16 را هندل می‌کنیم،
                # برای CIDR کامل نیاز به تبدیل به باینری یا استفاده از ipaddress است.
                if cidr_prefix == 24: # مثلاً 192.168.1.0/24
                    return ip_address.startswith(range_ip.rsplit('.', 1)[0] + '.')
                elif cidr_prefix == 16: # مثلاً 192.168.0.0/16
                    return ip_address.startswith(range_ip.rsplit('.', 2)[0] + '.')
                else:
                    logger.warning(f"CIDR '{ip_range}' پشتیبانی نمی‌شود. فقط IP دقیق، /16 و /24 در پیاده‌سازی فعلی.")
                    return False
            else: # IP دقیق
                return ip_address == ip_range
        except ValueError: # اگر IP نامعتبر باشد
            logger.warning(f"فرمت IP یا CIDR نامعتبر برای فیلتر: '{ip_range}'")
            return False
        except Exception as e:
            logger.warning(f"خطا در بررسی IP در رنج '{ip_range}' برای IP '{ip_address}': {e}")
            return False


    def filter_configs(self, 
                       configs: List[Dict[str, str]], 
                       allowed_countries: Optional[List[str]] = None,
                       blocked_countries: Optional[List[str]] = None,
                       allowed_protocols: Optional[List[str]] = None,
                       blocked_keywords: Optional[List[str]] = None,
                       blocked_ips: Optional[List[str]] = None,
                       blocked_domains: Optional[List[str]] = None
                      ) -> List[Dict[str, str]]:
        """
        لیست کانفیگ‌ها را بر اساس معیارهای فیلترینگ مشخص شده فیلتر می‌کند.
        
        configs: لیستی از دیکشنری‌های کانفیگ، هر کدام شامل 'config', 'protocol', 'flag', 'country', 'canonical_id'.
        allowed_countries: لیست کدهای کشور (ISO 3166-1 alpha-2، lowercase) که مجاز هستند.
        blocked_countries: لیست کدهای کشور که مسدود هستند.
        allowed_protocols: لیست پروتکل‌ها (با '://') که مجاز هستند.
        blocked_keywords: لیستی از کلمات کلیدی که اگر در 'config' یا 'canonical_id' باشند، مسدود می‌شوند.
        blocked_ips: لیستی از آدرس‌های IP یا رنج‌های CIDR که مسدود هستند.
        blocked_domains: لیستی از دامنه‌ها که مسدود هستند.
        """
        filtered_list: List[Dict[str, str]] = []
        
        # پیش‌پردازش لیست‌ها برای جستجوی کارآمدتر
        allowed_countries_lower = {c.lower() for c in (allowed_countries or [])}
        blocked_countries_lower = {c.lower() for c in (blocked_countries or [])}
        allowed_protocols_lower = {p.lower() for p in (allowed_protocols or [])}
        blocked_keywords_lower = {k.lower() for k in (blocked_keywords or [])}
        blocked_ips_set = set(blocked_ips or [])
        blocked_domains_set = {d.lower() for d in (blocked_domains or [])}

        logger.info(f"شروع فیلترینگ {len(configs)} کانفیگ با معیارهای مشخص شده...")

        for cfg_dict in configs:
            config_string = cfg_dict['config']
            protocol = cfg_dict['protocol']
            # استخراج کد کشور از پرچم (مثل 🇺🇸 -> us)
            country_code = cfg_dict['flag'].strip('🇦🇧🇨🇩🇪🇫🇬🇭🇮🇯🇰🇱🇲🇳🇴🇵🇶🇷🇸🇹🇺🇻🇼🇽🇾🇿').lower() 
            server_address = self.validator.get_server_address(config_string, protocol)
            
            # --- قوانین مسدودسازی (Blocklist) ---
            # 1. مسدودسازی بر اساس کشور
            if blocked_countries_lower and country_code in blocked_countries_lower:
                logger.debug(f"کانفیگ به دلیل کشور مسدود شده '{country_code}' رد شد: {config_string[:50]}...")
                continue

            # 2. مسدودسازی بر اساس کلمه کلیدی در کانفیگ یا Canonical ID
            text_to_search = config_string
            if 'canonical_id' in cfg_dict:
                text_to_search += " " + cfg_dict['canonical_id'] # برای جستجو در canonical_id
            
            if blocked_keywords_lower and self._match_keyword(text_to_search, list(blocked_keywords_lower)):
                logger.debug(f"کانفیگ به دلیل کلمه کلیدی مسدود شده رد شد: {config_string[:50]}...")
                continue
            
            # 3. مسدودسازی بر اساس IP یا دامنه
            if server_address:
                resolved_ip = None
                try:
                    resolved_ip = socket.gethostbyname(server_address) # تلاش برای حل دامنه به IP
                except socket.gaierror:
                    resolved_ip = server_address # اگر حل نشد، خودش را در نظر بگیر (ممکن است IP باشد)

                # بررسی IP در لیست سیاه
                if blocked_ips_set and resolved_ip:
                    is_blocked_ip = False
                    for bl_ip in blocked_ips_set:
                        if self._is_ip_in_range(resolved_ip, bl_ip):
                            is_blocked_ip = True
                            break
                    if is_blocked_ip:
                        logger.debug(f"کانفیگ به دلیل IP مسدود شده '{resolved_ip}' رد شد: {config_string[:50]}...")
                        continue

                # بررسی دامنه در لیست سیاه
                parsed_host = urlparse(config_string).hostname
                if parsed_host and blocked_domains_set and parsed_host.lower() in blocked_domains_set:
                    logger.debug(f"کانفیگ به دلیل دامنه مسدود شده '{parsed_host}' رد شد: {config_string[:50]}...")
                    continue
            
            # --- قوانین مجازسازی (Allowlist) ---
            # 1. مجازسازی بر اساس کشور (اگر allowed_countries مشخص شده باشد، فقط آن کشورها مجازند)
            if allowed_countries_lower and country_code not in allowed_countries_lower:
                logger.debug(f"کانفیگ به دلیل عدم وجود در کشورهای مجاز رد شد: {config_string[:50]}...")
                continue

            # 2. مجازسازی بر اساس پروتکل (اگر allowed_protocols مشخص شده باشد، فقط آن پروتکل‌ها مجازند)
            # توجه: protocol در اینجا فرمت کامل "vless://" را دارد.
            if allowed_protocols_lower and protocol.lower() not in allowed_protocols_lower:
                logger.debug(f"کانفیگ به دلیل عدم وجود در پروتکل‌های مجاز رد شد: {config_string[:50]}...")
                continue

            # اگر کانفیگ از تمام فیلترها عبور کرد، آن را اضافه کن
            filtered_list.append(cfg_dict)

        logger.info(f"فیلترینگ کامل شد. {len(filtered_list)} کانفیگ باقی ماند.")
        return filtered_list

