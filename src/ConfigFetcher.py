import re
import os
import time
import json
import logging
import socket 
import requests
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Set, Tuple 
from bs4 import BeautifulSoup
import base64 

from config import ProxyConfig, ChannelConfig
from config_validator import ConfigValidator

# پیکربندی لاگ‌گیری (از config.py ارث می‌برد یا اینجا تنظیم می‌کند)
# اگر در config.py تنظیم شده باشد، این خطوط ممکن است تکراری باشند اما مشکلی ایجاد نمی‌کنند.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('proxy_fetcher.log'), # لاگ در فایل
        logging.StreamHandler() # لاگ در کنسول
    ]
)
logger = logging.getLogger(__name__)

class ConfigFetcher:
    """
    کلاس ConfigFetcher مسئول واکشی، پردازش، اعتبارسنجی و غنی‌سازی کانفیگ‌های پراکسی است.
    همچنین مدیریت کانال‌ها، Smart Retry و ذخیره خروجی‌ها را بر عهده دارد.
    """
    def __init__(self, config: ProxyConfig):
        """
        سازنده کلاس ConfigFetcher.
        """
        logger.info("در حال مقداردهی اولیه ConfigFetcher...")
        self.config = config
        self.validator = ConfigValidator()
        self.protocol_counts: Dict[str, int] = {p: 0 for p in config.SUPPORTED_PROTOCOLS}
        self.seen_configs: Set[str] = set() # مجموعه‌ای برای نگهداری تمام کانفیگ‌های منحصر به فرد دیده شده
        self.channel_protocol_counts: Dict[str, Dict[str, int]] = {} 
        self.session = requests.Session() # استفاده از Session برای بهره‌وری بهتر درخواست‌های HTTP
        self.session.headers.update(config.HEADERS) # تنظیم هدرهای پیش‌فرض برای Session

        # کش برای ذخیره موقعیت جغرافیایی IPها برای افزایش سرعت و جلوگیری از محدودیت‌ها
        self.ip_location_cache: Dict[str, Tuple[str, str]] = {} 

        # بازه‌های زمانی برای Smart Retry (تلاش مجدد هوشمند)
        self.retry_intervals = [
            timedelta(days=0),
            timedelta(days=3),
            timedelta(weeks=1),
            timedelta(days=30),
            timedelta(days=90),
            timedelta(days=240)
        ]
        self.max_retry_level = len(self.retry_intervals) - 1 # حداکثر سطح تلاش مجدد
        logger.info("مقداردهی اولیه ConfigFetcher با موفقیت انجام شد.")

    # --- متدهای دریافت موقعیت جغرافیایی IP (منتقل شده از ConfigToSingbox) ---
    def _get_location_from_ip_api(self, ip: str) -> Tuple[str, str]:
        """دریافت موقعیت جغرافیایی از ip-api.com"""
        try:
            response = requests.get(f'http://ip-api.com/json/{ip}', headers=self.session.headers, timeout=5)
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == 'success' and data.get('countryCode'):
                    return data['countryCode'].lower(), data['country']
        except Exception as e:
            logger.debug(f"خطا در API ip-api.com برای IP {ip}: {str(e)}")
        return '', ''

    def _get_location_from_ipapi_co(self, ip: str) -> Tuple[str, str]:
        """دریافت موقعیت جغرافیایی از ipapi.co"""
        try:
            response = requests.get(f'https://ipapi.co/{ip}/json/', headers=self.session.headers, timeout=5)
            if response.status_code == 200:
                data = response.json()
                if data.get('country_code') and data.get('country_name'):
                    return data['country_code'].lower(), data['country_name']
        except Exception as e:
            logger.debug(f"خطا در API ipapi.co برای IP {ip}: {str(e)}")
        return '', ''

    def _get_location_from_ipwhois(self, ip: str) -> Tuple[str, str]:
        """دریافت موقعیت جغرافیایی از ipwhois.app"""
        try:
            response = requests.get(f'https://ipwhois.app/json/{ip}', headers=self.session.headers, timeout=5)
            if response.status_code == 200:
                data = response.json()
                if data.get('country_code') and data.get('country'):
                    return data['country_code'].lower(), data['country']
        except Exception as e:
            logger.debug(f"خطا در API ipwhois.app برای IP {ip}: {str(e)}")
        return '', ''

    def _get_location_from_ipdata(self, ip: str) -> Tuple[str, str]:
        """دریافت موقعیت جغرافیایی از api.ipdata.co (نیاز به کلید API واقعی دارد)"""
        try:
            response = requests.get(f'https://api.ipdata.co/{ip}?api-key=test', headers=self.session.headers, timeout=5)
            if response.status_code == 200:
                data = response.json()
                if data.get('country_code') and data.get('country_name'):
                    return data['country_code'].lower(), data['country_name']
        except Exception as e:
            logger.debug(f"خطا در API ipdata.co برای IP {ip}: {str(e)}")
        return '', ''

    def _get_location_from_abstractapi(self, ip: str) -> Tuple[str, str]:
        """دریافت موقعیت جغرافیایی از ipgeolocation.abstractapi.com (نیاز به کلید API واقعی دارد)"""
        try:
            response = requests.get(f'https://ipgeolocation.abstractapi.com/v1/?api_key=test&ip_address={ip}', 
                                  headers=self.session.headers, timeout=5)
            if response.status_code == 200:
                data = response.json()
                if data.get('country_code') and data.get('country'):
                    return data['country_code'].lower(), data['country']
        except Exception as e:
            logger.debug(f"خطا در API abstractapi.com برای IP {ip}: {str(e)}")
        return '', ''

    def get_location(self, address: str) -> Tuple[str, str]:
        """
        موقعیت جغرافیایی (پرچم و نام کشور) را از یک آدرس (دامنه/IP) دریافت می‌کند.
        از کش برای افزایش سرعت استفاده می‌کند.
        """
        # WARP ممکن است آدرس Gateway مشخص نداشته باشد، اما هنوز یک مکان (Cloudflare) دارد
        if address == "162.159.192.1": # Cloudflare Anycast IP
             logger.debug(f"آدرس '{address}' به عنوان Cloudflare Anycast شناسایی شد. استفاده از موقعیت پیش‌فرض.")
             return "🇺🇸", "Cloudflare" # پرچم آمریکا برای Cloudflare

        try:
            ip = socket.gethostbyname(address)
            
            # بررسی کش
            if ip in self.ip_location_cache:
                logger.debug(f"موقعیت IP '{ip}' از کش بازیابی شد.")
                return self.ip_location_cache[ip]

            apis = [
                self._get_location_from_ip_api,
                self._get_location_from_ipapi_co,
                self._get_location_from_ipwhois,
                self._get_location_from_ipdata,
                self._get_location_from_abstractapi
            ]
            
            for api_func in apis:
                country_code, country = api_func(ip)
                if country_code and country and len(country_code) == 2:
                    flag = ''.join(chr(ord('🇦') + ord(c.upper()) - ord('A')) for c in country_code)
                    self.ip_location_cache[ip] = (flag, country) # ذخیره در کش
                    logger.debug(f"موقعیت IP '{ip}' از API {api_func.__name__} دریافت شد: {flag} {country}")
                    return flag, country
                
        except socket.gaierror:
            logger.warning(f"نام میزبان قابل حل نیست: '{address}'. موقعیت 'نامشخص' خواهد بود.")
        except Exception as e:
            logger.error(f"خطای کلی در دریافت موقعیت برای '{address}': {str(e)}")
            
        # ذخیره در کش حتی اگر ناموفق بود تا از تلاش‌های بعدی برای همین آدرس جلوگیری شود.
        self.ip_location_cache[address] = ("🏳️", "Unknown") 
        return "🏳️", "Unknown"
    # --- پایان متدهای دریافت موقعیت جغرافیایی IP ---


    def extract_config(self, text: str, start_index: int, protocol: str) -> Optional[str]:
        """
        تلاشی برای استخراج یک کانفیگ خاص (با پروتکل مشخص) از یک متن بزرگ.
        """
        try:
            remaining_text = text[start_index:]
            configs = self.validator.split_configs(remaining_text)
            
            for config_item in configs:
                if config_item.startswith(protocol):
                    clean_config = self.validator.clean_config(config_item)
                    if self.validator.validate_protocol_config(clean_config, protocol):
                        return clean_config
            return None
        except Exception as e:
            logger.error(f"خطا در extract_config: {str(e)}")
            return None

    def fetch_with_retry(self, url: str) -> Optional[requests.Response]:
        """
        واکشی URL با قابلیت تلاش مجدد و تأخیر افزایشی.
        """
        backoff = 1
        for attempt in range(self.config.MAX_RETRIES):
            try:
                logger.info(f"در حال تلاش برای واکشی '{url}' (تلاش {attempt + 1}/{self.config.MAX_RETRIES})")
                response = self.session.get(url, timeout=self.config.REQUEST_TIMEOUT)
                response.raise_for_status() # اگر وضعیت پاسخ خطا بود، استثنا ایجاد می‌کند.
                return response
            except requests.RequestException as e:
                if attempt == self.config.MAX_RETRIES - 1:
                    logger.error(f"واکشی '{url}' پس از {self.config.MAX_RETRIES} تلاش ناموفق بود: {str(e)}")
                    return None
                wait_time = min(self.config.RETRY_DELAY * backoff, 60)
                logger.warning(f"تلاش {attempt + 1} برای '{url}' ناموفق بود. تلاش مجدد در {wait_time} ثانیه: {str(e)}")
                time.sleep(wait_time)
                backoff *= 2 # افزایش ضریب تأخیر برای تلاش‌های بعدی
        return None

    def fetch_ssconf_configs(self, url: str) -> List[str]:
        """
        واکشی کانفیگ‌ها از URLهای ssconf:// با تبدیل آن‌ها به HTTPS و پردازش محتوا.
        """
        https_url = self.validator.convert_ssconf_to_https(url)
        configs = []
        logger.info(f"در حال واکشی کانفیگ‌های ssconf از: '{https_url}'")
        
        response = self.fetch_with_retry(https_url)
        if response and response.text.strip():
            text = response.text.strip()
            decoded_text = self.check_and_decode_base64(text)
            if decoded_text:
                logger.debug(f"محتوای ssconf از Base64 دیکد شد.")
                text = decoded_text
            
            found_configs = self.validator.split_configs(text)
            configs.extend(found_configs)
            logger.info(f"{len(found_configs)} کانفیگ از ssconf '{https_url}' یافت شد.")
        else:
            logger.warning(f"هیچ محتوایی از ssconf '{https_url}' دریافت نشد یا خالی بود.")
            
        return configs

    def check_and_decode_base64(self, text: str) -> Optional[str]:
        """
        بررسی می‌کند که آیا کل متن ورودی با Base64 کدگذاری شده و در صورت مثبت بودن، آن را دیکد می‌کند.
        """
        try:
            decoded_text = self.validator.decode_base64_text(text)
            if decoded_text:
                if any(p in decoded_text for p in self.config.SUPPORTED_PROTOCOLS):
                    logger.debug(f"متن با موفقیت به Base64 دیکد شد و شامل پروتکل‌های شناخته شده است.")
                    return decoded_text
            logger.debug(f"متن Base64 نیست یا شامل پروتکل‌های شناخته شده نیست.")
            return None
        except Exception as e:
            logger.debug(f"خطا در دیکد کردن Base64: {str(e)}")
            return None

    def add_new_telegram_channel(self, new_channel_url: str):
        """
        یک کانال تلگرام جدید را (در صورت عدم وجود) به لیست منابع اضافه می‌کند.
        """
        is_new_channel = True
        for existing_channel in self.config.SOURCE_URLS:
            if self.config._normalize_url(existing_channel.url) == self.config._normalize_url(new_channel_url):
                is_new_channel = False
                break
        
        if is_new_channel:
            try:
                new_channel_config = ChannelConfig(url=new_channel_url)
                self.config.SOURCE_URLS.append(new_channel_config)
                logger.info(f"کانال تلگرام جدید به صورت پویا اضافه شد: '{new_channel_url}'.")
            except ValueError as e:
                logger.warning(f"URL کانال تلگرام نامعتبر پیدا شد و نادیده گرفته شد: '{new_channel_url}' - {e}")


    def fetch_configs_from_source(self, channel: ChannelConfig) -> List[Dict[str, str]]:
        """
        واکشی کانفیگ‌ها از یک کانال منبع مشخص (تلگرام یا وب‌سایت).
        قابلیت‌ها: شناسایی لینک‌های کانال تلگرام از پیام‌ها و از مشخصات کانفیگ‌ها.
        """
        logger.info(f"شروع واکشی از منبع: '{channel.url}'")
        current_channel_valid_processed_configs: List[Dict[str, str]] = []
        
        channel.metrics.total_configs = 0
        channel.metrics.valid_configs = 0
        channel.metrics.unique_configs = 0
        channel.metrics.protocol_counts = {p: 0 for p in self.config.SUPPORTED_PROTOCOLS}
        
        start_time = time.time()
        
        if channel.url.startswith('ssconf://'):
            logger.debug(f"کانال '{channel.url}' به عنوان منبع ssconf:// شناسایی شد.")
            raw_ssconf_configs = self.fetch_ssconf_configs(channel.url)
            channel.metrics.total_configs += len(raw_ssconf_configs)
            logger.debug(f"در حال پردازش {len(raw_ssconf_configs)} کانفیگ خام از '{channel.url}'.")
            for raw_cfg in raw_ssconf_configs:
                processed_cfg_dict = self.process_config(raw_cfg, channel)
                if processed_cfg_dict:
                    current_channel_valid_processed_configs.append(processed_cfg_dict)
            
            if current_channel_valid_processed_configs:
                response_time = time.time() - start_time
                self.config.update_channel_stats(channel, True, response_time)
                channel.retry_level = 0
                channel.next_check_time = None
                logger.info(f"کانال '{channel.url}' با موفقیت {len(current_channel_valid_processed_configs)} کانفیگ معتبر ارائه داد. سطح تلاش مجدد بازنشانی شد.")
            else:
                self.config.update_channel_stats(channel, False)
                channel.retry_level = min(channel.retry_level + 1, self.max_retry_level)
                channel.next_check_time = datetime.now(timezone.utc) + self.retry_intervals[channel.retry_level]
                logger.warning(f"کانال '{channel.url}' کانفیگ معتبری نداشت. سطح تلاش مجدد به {channel.retry_level} افزایش یافت. بررسی بعدی در: {channel.next_check_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")

            return current_channel_valid_processed_configs

        response = self.fetch_with_retry(channel.url)
        if not response:
            self.config.update_channel_stats(channel, False)
            channel.retry_level = min(channel.retry_level + 1, self.max_retry_level)
            channel.next_check_time = datetime.now(timezone.utc) + self.retry_intervals[channel.retry_level]
            logger.warning(f"واکشی از کانال '{channel.url}' ناموفق بود. سطح تلاش مجدد به {channel.retry_level} افزایش یافت. بررسی بعدی در: {channel.next_check_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
            return current_channel_valid_processed_configs

        response_time = time.time() - start_time
        
        if channel.is_telegram:
            logger.debug(f"در حال تجزیه محتوای تلگرام برای کانال: '{channel.url}'.")
            soup = BeautifulSoup(response.text, 'html.parser')
            messages = soup.find_all('div', class_='tgme_widget_message_text')
            
            sorted_messages = sorted(
                messages,
                key=lambda message: self.extract_date_from_message(message) or datetime.min.replace(tzinfo=timezone.utc),
                reverse=True
            )
            logger.debug(f"{len(messages)} پیام تلگرام یافت شد. در حال پردازش پیام‌ها...")
            
            for message_div in sorted_messages:
                if not message_div or not message_div.text:
                    logger.debug("پیام تلگرام خالی یا بدون متن، نادیده گرفته شد.")
                    continue
                
                message_date = self.extract_date_from_message(message_div)
                if not self.is_config_valid(message_div.text, message_date):
                    logger.debug(f"پیام به دلیل تاریخ نامعتبر (تاریخ: {message_date}) نادیده گرفته شد. محتوا: '{message_div.text[:min(len(message_div.text), 50)]}...'.")
                    continue
                
                # --- شناسایی لینک‌های کانال تلگرام از پیام (لینک‌ها و منشن‌ها) ---
                links_and_mentions = message_div.find_all('a', href=True)
                for item in links_and_mentions:
                    href_url = item['href']
                    logger.debug(f"لینک یافت شد در پیام: '{href_url}'")
                    
                    # 1. تلاش برای استخراج کانفیگ‌ها مستقیماً از URL لینک
                    extracted_from_link = self.validator.split_configs(href_url)
                    channel.metrics.total_configs += len(extracted_from_link)
                    for cfg_from_link in extracted_from_link:
                        processed_cfg_dict = self.process_config(cfg_from_link, channel)
                        if processed_cfg_dict:
                            current_channel_valid_processed_configs.append(processed_cfg_dict)
                            logger.debug(f"کانفیگ از لینک استخراج شد: {processed_cfg_dict['protocol']}.")
                    
                    # 2. بررسی اینکه آیا href_url یک لینک کانال تلگرام است برای افزودن پویا
                    match_s = re.match(r'https?://t\.me/s/([a-zA-Z0-9_]+)', href_url)
                    match_direct = re.match(r'https?://t\.me/([a-zA-Z0-9_]+)', href_url)
                    
                    channel_name = None
                    if match_s:
                        channel_name = match_s.group(1)
                    elif match_direct:
                        channel_name = match_direct.group(1)
                    
                    if channel_name:
                        new_channel_url = f"https://t.me/s/{channel_name}"
                        self.add_new_telegram_channel(new_channel_url)
                # --- پایان شناسایی از لینک‌ها و منشن‌ها ---

                # --- منطق استخراج از محتوای متنی پیام ---
                text_content = message_div.text
                logger.debug(f"در حال پردازش محتوای متنی پیام: '{text_content[:min(len(text_content), 100)]}...'")
                
                decoded_full_text = self.check_and_decode_base64(text_content)
                if decoded_full_text:
                    raw_configs_from_decoded = self.validator.split_configs(decoded_full_text)
                    channel.metrics.total_configs += len(raw_configs_from_decoded)
                    for raw_cfg in raw_configs_from_decoded:
                        processed_cfg_dict = self.process_config(raw_cfg, channel)
                        if processed_cfg_dict:
                            current_channel_valid_processed_configs.append(processed_cfg_dict)
                            logger.debug(f"کانفیگ دیکد شده از متن پیام: {processed_cfg_dict['protocol']}.")
                else:
                    raw_configs_from_text = self.validator.split_configs(text_content)
                    channel.metrics.total_configs += len(raw_configs_from_text)
                    for raw_cfg in raw_configs_from_text:
                        processed_cfg_dict = self.process_config(raw_cfg, channel)
                        if processed_cfg_dict:
                            current_channel_valid_processed_configs.append(processed_cfg_dict)
                            logger.debug(f"کانفیگ از متن پیام: {processed_cfg_dict['protocol']}.")

        else: # برای کانال‌های غیرتلگرام (صفحات وب عمومی)
            logger.debug(f"در حال پردازش محتوای وب برای کانال: '{channel.url}'.")
            text_content = response.text
            decoded_full_text = self.check_and_decode_base64(text_content)
            if decoded_full_text:
                raw_configs_from_decoded = self.validator.split_configs(decoded_full_text)
                channel.metrics.total_configs += len(raw_configs_from_decoded)
                for raw_cfg in raw_configs_from_decoded:
                    processed_cfg_dict = self.process_config(raw_cfg, channel)
                    if processed_cfg_dict:
                        current_channel_valid_processed_configs.append(processed_cfg_dict)
            else:
                raw_configs_from_web = self.validator.split_configs(text_content)
                channel.metrics.total_configs += len(raw_configs_from_web)
                for raw_cfg in raw_configs_from_web:
                    processed_cfg_dict = self.process_config(raw_cfg, channel)
                    if processed_cfg_dict:
                        current_channel_valid_processed_configs.append(processed_cfg_dict)

        # منطق به‌روزرسانی retry_level و next_check_time
        if len(current_channel_valid_processed_configs) >= self.config.MIN_CONFIGS_PER_CHANNEL:
            self.config.update_channel_stats(channel, True, response_time)
            self.config.adjust_protocol_limits(channel)
            channel.retry_level = 0
            channel.next_check_time = None
            logger.info(f"کانال '{channel.url}' با موفقیت {len(current_channel_valid_processed_configs)} کانفیگ معتبر ارائه داد. سطح تلاش مجدد بازنشانی شد.")
        else:
            self.config.update_channel_stats(channel, False)
            logger.warning(f"تعداد کافی کانفیگ در کانال '{channel.url}' یافت نشد: {len(current_channel_valid_processed_configs)} کانفیگ. سطح تلاش مجدد به {channel.retry_level} افزایش یافت. بررسی بعدی در: {channel.next_check_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        
        logger.info(f"پایان واکشی از منبع: '{channel.url}'. مجموع کانفیگ‌های معتبر و پردازش شده: {len(current_channel_valid_processed_configs)}.")
        return current_channel_valid_processed_configs

    def process_config(self, config_string: str, channel: ChannelConfig) -> Optional[Dict[str, str]]:
        """
        یک کانفیگ را پردازش می‌کند: نرمال‌سازی، پاکسازی، اعتبارسنجی، استخراج لینک‌های تلگرام،
        و افزودن اطلاعات پرچم و کشور.
        """
        
        if not config_string:
            logger.debug("رشته کانفیگ ورودی خالی است. نادیده گرفته شد.")
            return None

        # نرمال‌سازی پروتکل Hysteria2 و Hysteria 1
        if config_string.startswith('hy2://'):
            config_string = self.validator.normalize_hysteria2_protocol(config_string)
            logger.debug(f"نرمال‌سازی 'hy2://' به 'hysteria2://' برای کانفیگ: '{config_string[:min(len(config_string), 50)]}...'")
        elif config_string.startswith('hy1://'):
            config_string = config_string.replace('hy1://', 'hysteria://', 1) # تبدیل alias به پروتکل اصلی
            logger.debug(f"نرمال‌سازی 'hy1://' به 'hysteria://' برای کانفیگ: '{config_string[:min(len(config_string), 50)]}...'")
            
        # استخراج لینک‌های کانال تلگرام از مشخصات خود کانفیگ
        discovered_channels_from_config = self.validator.extract_telegram_channels_from_config(config_string)
        for new_channel_url in discovered_channels_from_config:
            self.add_new_telegram_channel(new_channel_url)
            logger.debug(f"لینک کانال تلگرام از مشخصات کانفیگ استخراج شد: '{new_channel_url}'.")

        flag = "🏳️"
        country = "Unknown"
        actual_protocol = None

        # پیدا کردن پروتکل اصلی کانفیگ
        for protocol_prefix in self.config.SUPPORTED_PROTOCOLS:
            aliases = self.config.SUPPORTED_PROTOCOLS[protocol_prefix].get('aliases', [])
            protocol_match = False
            
            if config_string.startswith(protocol_prefix):
                protocol_match = True
                actual_protocol = protocol_prefix
            else:
                for alias in aliases:
                    if config_string.startswith(alias):
                        protocol_match = True
                        config_string = config_string.replace(alias, protocol_prefix, 1) # جایگزینی alias با پروتکل اصلی
                        actual_protocol = protocol_prefix
                        break
                        
            if protocol_match:
                # اگر پروتکل فعال نیست، کانفیگ را نادیده بگیرید
                if not self.config.is_protocol_enabled(actual_protocol):
                    logger.debug(f"پروتکل '{actual_protocol}' فعال نیست. کانفیگ نادیده گرفته شد: '{config_string[:min(len(config_string), 50)]}...'.")
                    return None 
                
                # پاکسازی خاص برای پروتکل‌های خاص (VMess و SSR)
                if actual_protocol == "vmess://":
                    config_string = self.validator.clean_vmess_config(config_string)
                    logger.debug(f"پاکسازی VMess: '{config_string[:min(len(config_string), 50)]}...'")
                elif actual_protocol == "ssr://":
                    config_string = self.validator.clean_ssr_config(config_string)
                    logger.debug(f"پاکسازی SSR: '{config_string[:min(len(config_string), 50)]}...'")
                
                # پاکسازی عمومی کانفیگ (حذف کاراکترهای نامرئی، ایموجی و فضاهای اضافی)
                clean_config = self.validator.clean_config(config_string)
                
                # اعتبارسنجی نهایی و دقیق پروتکل خاص
                if self.validator.validate_protocol_config(clean_config, actual_protocol):
                    # کانفیگ معتبر است، در حال دریافت آدرس سرور و موقعیت جغرافیایی
                    server_address = self.validator.get_server_address(clean_config, actual_protocol)
                    if server_address:
                        flag, country = self.get_location(server_address)
                        logger.debug(f"موقعیت برای '{server_address}' یافت شد: {flag} {country}")
                    else:
                        logger.debug(f"آدرس سرور برای پروتکل '{actual_protocol}' از کانفیگ استخراج نشد: '{clean_config[:min(len(clean_config), 50)]}...'.")
                    
                    # به‌روزرسانی معیارهای کانال و شمارش پروتکل
                    channel.metrics.valid_configs += 1
                    channel.metrics.protocol_counts[actual_protocol] = channel.metrics.protocol_counts.get(actual_protocol, 0) + 1
                    
                    # بررسی منحصر به فرد بودن در سطح کلی (بین تمام کانال‌ها)
                    if clean_config not in self.seen_configs:
                        channel.metrics.unique_configs += 1
                        self.seen_configs.add(clean_config)
                        self.protocol_counts[actual_protocol] += 1
                        logger.debug(f"کانفیگ منحصر به فرد '{actual_protocol}' یافت شد: '{clean_config[:min(len(clean_config), 50)]}...'")
                        
                        return {
                            'config': clean_config,
                            'protocol': actual_protocol,
                            'flag': flag,
                            'country': country
                        }
                    else:
                        logger.debug(f"کانفیگ تکراری '{actual_protocol}' نادیده گرفته شد: '{clean_config[:min(len(clean_config), 50)]}...'.")
                else:
                    logger.debug(f"اعتبارسنجی پروتکل '{actual_protocol}' برای کانفیگ '{clean_config[:min(len(clean_config), 50)]}...' ناموفق بود. نادیده گرفته شد.")
                break # پس از یافتن یک مطابقت پروتکل و پردازش، از حلقه خارج شوید
                
        logger.debug(f"کانفیگ '{config_string[:min(len(config_string), 50)]}...' با هیچ پروتکل فعال یا معتبری مطابقت نداشت.")
        return None

    def extract_date_from_message(self, message) -> Optional[datetime]:
        """
        تاریخ و زمان انتشار پیام را از عنصر <time> در HTML پیام تلگرام استخراج می‌کند.
        """
        try:
            time_element = message.find_parent('div', class_='tgme_widget_message').find('time')
            if time_element and 'datetime' in time_element.attrs:
                return datetime.fromisoformat(time_element['datetime'].replace('Z', '+00:00'))
        except Exception as e:
            logger.debug(f"خطا در استخراج تاریخ از پیام: {str(e)}")
            pass
        return None

    def is_config_valid(self, config_text: str, date: Optional[datetime]) -> bool:
        """
        بررسی می‌کند که آیا تاریخ کانفیگ به اندازه کافی جدید است (طبق MAX_CONFIG_AGE_DAYS).
        """
        if not date:
            logger.debug("تاریخ کانفیگ موجود نیست، معتبر فرض می‌شود.")
            return True
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=self.config.MAX_CONFIG_AGE_DAYS)
        if date >= cutoff_date:
            return True
        else:
            logger.debug(f"کانفیگ به دلیل قدیمی بودن تاریخ (تاریخ: {date}) نادیده گرفته شد.")
            return False

    def balance_protocols(self, configs: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """
        کانفیگ‌ها را بر اساس پروتکل سازماندهی و متعادل می‌کند تا توزیع مناسبی داشته باشند.
        این متد تضمین می‌کند که تعداد کانفیگ‌ها برای هر پروتکل از "max_configs" تعیین شده
        در تنظیمات (برای آن پروتکل) تجاوز نکند.
        """
        logger.info("شروع توازن پروتکل‌ها...")
        protocol_configs: Dict[str, List[Dict[str, str]]] = {p: [] for p in self.config.SUPPORTED_PROTOCOLS}
        for config_dict in configs:
            protocol = config_dict['protocol']
            # مطمئن شوید که پروتکل اصلی (نه alias) برای دسته‌بندی استفاده می‌شود
            if protocol.startswith('hy2://'):
                protocol = 'hysteria2://'
            elif protocol.startswith('hy1://'):
                protocol = 'hysteria://'
            
            if protocol in protocol_configs:
                protocol_configs[protocol].append(config_dict)
            else:
                logger.warning(f"پروتکل '{protocol}' در لیست پروتکل‌های پشتیبانی شده برای توازن یافت نشد. ممکن است به درستی تعریف نشده باشد.")

        total_configs = sum(len(configs_list) for configs_list in protocol_configs.values())
        if total_configs == 0:
            logger.info("هیچ کانفیگی برای توازن پروتکل وجود ندارد.")
            return []
            
        balanced_configs: List[Dict[str, str]] = []
        # مرتب‌سازی پروتکل‌ها بر اساس اولویت (بالاترین اولویت اول) و سپس تعداد موجود (بیشترین اول)
        sorted_protocols = sorted(
            protocol_configs.items(),
            key=lambda x: (
                self.config.SUPPORTED_PROTOCOLS.get(x[0], {"priority": 999})["priority"], # مدیریت پروتکل‌های ناشناخته
                len(x[1])
            ),
            reverse=True
        )
        logger.info(f"در حال توازن {total_configs} کانفیگ بر اساس {len(sorted_protocols)} پروتکل مرتب شده...")
        
        for protocol, protocol_config_list in sorted_protocols:
            protocol_info = self.config.SUPPORTED_PROTOCOLS.get(protocol)
            if not protocol_info:
                logger.warning(f"اطلاعات پیکربندی برای پروتکل '{protocol}' یافت نشد، نادیده گرفته شد.")
                continue

            if len(protocol_config_list) >= protocol_info["min_configs"]:
                # انتخاب تعداد کانفیگ‌ها بر اساس max_configs پروتکل مربوطه
                num_to_add = min(
                    protocol_info["max_configs"],  # حداکثر کانفیگ مجاز برای این پروتکل
                    len(protocol_config_list)     # تعداد کانفیگ‌های موجود
                )
                balanced_configs.extend(protocol_config_list[:num_to_add])
                logger.info(f"پروتکل '{protocol}': {num_to_add} کانفیگ اضافه شد (از {len(protocol_config_list)} موجود، حداکثر مجاز: {protocol_info['max_configs']}).")
            elif protocol_info["flexible_max"] and len(protocol_config_list) > 0:
                balanced_configs.extend(protocol_config_list)
                logger.info(f"پروتکل '{protocol}': {len(protocol_config_list)} کانفیگ اضافه شد (حالت flexible_max).")
            else:
                logger.debug(f"پروتکل '{protocol}': تعداد کانفیگ‌های کافی یافت نشد ({len(protocol_config_list)}).")
        
        logger.info(f"توازن پروتکل‌ها کامل شد. مجموعاً {len(balanced_configs)} کانفیگ نهایی پس از توازن.")
        return balanced_configs

    def fetch_all_configs(self) -> List[Dict[str, str]]:
        """
        واکشی کانفیگ‌ها از تمام کانال‌های فعال و اعمال توازن پروتکل.
        کانال‌هایی که در حالت Smart Retry هستند، نادیده گرفته می‌شوند تا زمان بررسی بعدی‌شان فرا رسد.
        """
        all_configs: List[Dict[str, str]] = []
        
        channels_to_process = []
        now = datetime.now(timezone.utc)
        
        logger.info(f"در حال فیلتر کردن کانال‌ها برای پردازش. زمان فعلی: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}.")
        for channel in list(self.config.SOURCE_URLS): # ایجاد کپی برای تکرار
            if not channel.enabled:
                logger.debug(f"کانال '{channel.url}' غیرفعال است و نادیده گرفته شد.")
                continue
            if channel.next_check_time and channel.next_check_time > now:
                logger.info(f"کانال '{channel.url}' به دلیل تلاش مجدد هوشمند نادیده گرفته شد. زمان بررسی بعدی: {channel.next_check_time.strftime('%Y-%m-%d %H:%M:%S UTC')}.")
                continue
            channels_to_process.append(channel)
            logger.debug(f"کانال '{channel.url}' برای پردازش انتخاب شد.")
        
        total_channels_to_process = len(channels_to_process)
        if total_channels_to_process == 0:
            logger.info("هیچ کانال فعالی برای پردازش وجود ندارد (یا همه در حالت تلاش مجدد هوشمند هستند). فرآیند واکشی به پایان رسید.")
            return []

        logger.info(f"شروع واکشی کانفیگ‌ها از {total_channels_to_process} کانال فعال...")
        for idx, channel in enumerate(channels_to_process, 1):
            logger.info(f"--- [ {idx}/{total_channels_to_process} ] در حال واکشی از کانال: '{channel.url}' ---")
            channel_configs_dicts = self.fetch_configs_from_source(channel)
            all_configs.extend(channel_configs_dicts)
            
            if idx < total_channels_to_process:
                logger.debug("مکث 2 ثانیه قبل از واکشی کانال بعدی...")
                time.sleep(2)

        if all_configs:
            logger.info(f"واکشی از همه کانال‌ها تکمیل شد. مجموعاً {len(all_configs)} کانفیگ خام جمع‌آوری شد.")
            # حذف تکراری‌ها از لیست کلی کانفیگ‌ها
            unique_configs_set = set()
            unique_configs_list_of_dicts = []
            for cfg_dict in all_configs:
                # استفاده از فقط string کانفیگ برای بررسی تکراری بودن
                if cfg_dict['config'] not in unique_configs_set:
                    unique_configs_set.add(cfg_dict['config'])
                    unique_configs_list_of_dicts.append(cfg_dict)

            logger.info(f"پس از حذف تکراری‌ها، {len(unique_configs_list_of_dicts)} کانفیگ منحصر به فرد باقی ماند.")
            # مرتب سازی بر اساس رشته کانفیگ قبل از توازن برای اطمینان از خروجی ثابت
            all_configs = self.balance_protocols(sorted(unique_configs_list_of_dicts, key=lambda x: x['config']))
            logger.info(f"فرآیند واکشی و توازن کامل شد. {len(all_configs)} کانفیگ نهایی آماده ذخیره.")
            return all_configs
        else:
            logger.warning("هیچ کانفیگ معتبری پس از واکشی و پردازش یافت نشد!")
            return []

def _save_base64_file(file_path: str, content: str):
    """یک محتوا را Base64 می‌کند و در یک فایل ذخیره می‌کند."""
    try:
        encoded_content = base64.b64encode(content.encode('utf-8')).decode('utf-8')
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(encoded_content)
        logger.info(f"محتوای Base64 شده در '{file_path}' ذخیره شد.")
    except Exception as e:
        logger.error(f"خطا در ذخیره فایل Base64 شده '{file_path}': {str(e)}")

def save_configs(configs: List[Dict[str, str]], config: ProxyConfig):
    """
    ذخیره لیست نهایی کانفیگ‌ها در فایل‌های مختلف در ساختار پوشه جدید.
    حالا کانفیگ‌ها شامل اطلاعات پرچم و کشور هستند.
    """
    logger.info("در حال آماده‌سازی دایرکتوری‌های خروجی برای ذخیره کانفیگ‌ها...")
    # ایجاد پوشه‌های اصلی و فرعی برای خروجی‌های متنی و Base64
    os.makedirs(config.TEXT_OUTPUT_DIR, exist_ok=True)
    os.makedirs(config.BASE64_OUTPUT_DIR, exist_ok=True)
    os.makedirs(config.SINGBOX_OUTPUT_DIR, exist_ok=True) # برای اطمینان که پوشه Singbox هم وجود دارد

    # هدر اشتراک (Subscription Header) برای کلاینت‌های پراکسی
    header = """//profile-title: base64:8J+RvUFub255bW91cy3wnZWP
//profile-update-interval: 1
//subscription-userinfo: upload=0; download=0; total=10737418240000000; expire=2546249531
//support-url: https://t.me/BXAMbot
//profile-web-page-url: https://github.com/4n0nymou3

"""
    
    # ساخت محتوای متنی کامل با پرچم‌ها
    full_text_lines = []
    for cfg_dict in configs:
        full_text_lines.append(f"{cfg_dict['flag']} {cfg_dict['country']} {cfg_dict['config']}")
    full_text_content = header + '\n\n'.join(full_text_lines) + '\n' # اضافه کردن خط جدید در انتها

    # --- 1. ذخیره فایل کامل (متنی) در subs/text/proxy_configs.txt ---
    full_file_path = os.path.join(config.TEXT_OUTPUT_DIR, 'proxy_configs.txt')
    try:
        with open(full_file_path, 'w', encoding='utf-8') as f:
            f.write(full_text_content)
        logger.info(f"با موفقیت {len(configs)} کانفیگ نهایی در '{full_file_path}' ذخیره شد.")
    except Exception as e:
        logger.error(f"خطا در ذخیره فایل کامل کانفیگ: {str(e)}")

    # --- 2. ذخیره فایل کامل (Base64) در subs/base64/proxy_configs_base64.txt ---
    base64_full_file_path = os.path.join(config.BASE64_OUTPUT_DIR, "proxy_configs_base64.txt")
    _save_base64_file(base64_full_file_path, full_text_content)

    # --- 3. تفکیک و ذخیره بر اساس پروتکل ---
    protocol_configs_separated: Dict[str, List[Dict[str, str]]] = {p: [] for p in config.SUPPORTED_PROTOCOLS}
    for cfg_dict in configs:
        protocol_full_name = cfg_dict['protocol']
        # مطمئن شوید که پروتکل اصلی (نه alias) برای دسته‌بندی استفاده می‌شود
        if protocol_full_name.startswith('hy2://'):
            protocol_full_name = 'hysteria2://'
        elif protocol_full_name.startswith('hy1://'):
            protocol_full_name = 'hysteria://'
        
        if protocol_full_name in protocol_configs_separated:
             protocol_configs_separated[protocol_full_name].append(cfg_dict)
        else:
            logger.warning(f"پروتکل '{protocol_full_name}' در لیست پروتکل‌های پشتیبانی شده برای تفکیک یافت نشد.")


    for protocol_full_name, cfg_list_of_dicts in protocol_configs_separated.items():
        if not cfg_list_of_dicts:
            continue

        # حذف "://" از نام پروتکل برای نام فایل
        protocol_name = protocol_full_name.replace('://', '')
        
        # ساخت محتوای متنی برای پروتکل خاص با پرچم‌ها
        protocol_text_lines = []
        for cfg_dict in cfg_list_of_dicts:
             protocol_text_lines.append(f"{cfg_dict['flag']} {cfg_dict['country']} {cfg_dict['config']}")
        protocol_text_content = header + '\n\n'.join(protocol_text_lines) + '\n'

        # --- 3a. ذخیره فایل متنی پروتکل خاص در subs/text/ ---
        protocol_file_name = f"{protocol_name}.txt"
        protocol_file_path = os.path.join(config.TEXT_OUTPUT_DIR, protocol_file_name)
        try:
            with open(protocol_file_path, 'w', encoding='utf-8') as f:
                f.write(protocol_text_content)
            logger.info(f"با موفقیت {len(cfg_list_of_dicts)} کانفیگ '{protocol_name}' در '{protocol_file_path}' ذخیره شد.")
        except Exception as e:
            logger.error(f"خطا در ذخیره فایل '{protocol_name}' کانفیگ: {str(e)}")

        # --- 3b. ذخیره فایل Base64 شده پروتکل خاص در subs/base64/ ---
        base64_protocol_file_name = f"{protocol_name}_base64.txt"
        base64_protocol_file_path = os.path.join(config.BASE64_OUTPUT_DIR, base64_protocol_file_name)
        _save_base64_file(base64_protocol_file_path, protocol_text_content)


def main():
    """
    تابع اصلی برای اجرای فرآیند واکشی و ذخیره کانفیگ‌ها.
    """
    try:
        logger.info("شروع فرآیند واکشی و پردازش کانفیگ‌ها...")
        config = ProxyConfig() # مقداردهی اولیه تنظیمات کلی
        fetcher = ConfigFetcher(config) # ایجاد نمونه از واکشی‌کننده کانفیگ
        
        configs = fetcher.fetch_all_configs() # واکشی و پردازش تمامی کانفیگ‌ها
        
        if configs:
            save_configs(configs, config) # ذخیره کانفیگ‌های نهایی در فایل‌ها
            logger.info(f"فرآیند با موفقیت در {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')} به پایان رسید. مجموعاً {len(configs)} کانفیگ پردازش شد.")
            
            logger.info("تعداد کانفیگ‌ها بر اساس پروتکل:")
            for protocol, count in fetcher.protocol_counts.items():
                logger.info(f"  {protocol}: {count} کانفیگ")
        else:
            logger.error("هیچ کانفیگ معتبری یافت نشد و هیچ فایلی تولید نشد!")
            
        save_channel_stats(config) # آمار کانال‌ها همیشه باید ذخیره شود
        logger.info("آمار کانال‌ها ذخیره شد.")
            
    except Exception as e:
        logger.critical(f"خطای بحرانی در اجرای اصلی: {str(e)}", exc_info=True) # exc_info=True برای نمایش traceback

if __name__ == '__main__':
    main()

