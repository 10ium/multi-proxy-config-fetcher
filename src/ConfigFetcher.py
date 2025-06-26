import re
import os
import time
import json
import logging
import socket 
import requests
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Set, Tuple, Any 
from bs4 import BeautifulSoup
import base64 

import concurrent.futures
import threading

from config import ProxyConfig, ChannelConfig
from config_validator import ConfigValidator
from user_settings import SOURCE_URLS 

# پیکربندی لاگ‌گیری (سطح پیش‌فرض INFO. برای دیدن جزئیات بیشتر به logging.DEBUG تغییر دهید.)
logging.basicConfig(
    level=logging.INFO, # <--- اینجا سطح لاگ‌گیری را به INFO تنظیم کنید (می‌توانید برای عیب‌یابی به DEBUG تغییر دهید).
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('proxy_fetcher.log'), 
        logging.StreamHandler()
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
        self.seen_configs: Set[str] = set() 
        self.channel_protocol_counts: Dict[str, Dict[str, int]] = {} 
        self.session = requests.Session() 
        self.session.headers.update(config.HEADERS) 

        self.ip_location_cache: Dict[str, Tuple[str, str]] = {} 

        self._lock = threading.Lock() 

        self.retry_intervals = [
            timedelta(days=0),
            timedelta(days=3),
            timedelta(weeks=1),
            timedelta(days=30),
            timedelta(days=90),
            timedelta(days=240)
        ]
        self.max_retry_level = len(self.retry_intervals) - 1 
        
        self.initial_user_settings_urls: Set[str] = {self.config._normalize_url(url) for url in SOURCE_URLS}
        self.previous_stats_urls: Set[str] = set()
        self._load_previous_stats_urls()
        
        logger.info("مقداردهی اولیه ConfigFetcher با موفقیت انجام شد.")

    def _load_previous_stats_urls(self):
        """
        بارگذاری URLهای کانال از channel_stats.json قبلی برای تشخیص کانال‌های جدید.
        """
        stats_file_path = os.path.join(self.config.OUTPUT_DIR, 'channel_stats.json')
        if os.path.exists(stats_file_path):
            try:
                with open(stats_file_path, 'r', encoding='utf-8') as f:
                    stats_data = json.load(f)
                for channel_data in stats_data.get('channels', []):
                    try:
                        self.previous_stats_urls.add(self.config._normalize_url(channel_data['url']))
                    except ValueError as e:
                        logger.warning(f"URL نامعتبر در stats.json قبلی یافت شد و نادیده گرفته شد: {channel_data.get('url', 'نامعلوم')} - {str(e)}")
                logger.debug(f"{len(self.previous_stats_urls)} URL از stats.json قبلی بارگذاری شد.")
            except Exception as e:
                logger.warning(f"خطا در بارگذاری URLها از stats.json قبلی: {str(e)}")

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
            response = requests.get(f'https://ipgeolocation.abstractapi.com/v1/?api_key=test', headers=self.session.headers, timeout=5)
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
        if address == "162.159.192.1": # Cloudflare Anycast IP
             logger.debug(f"آدرس '{address}' به عنوان Cloudflare Anycast شناسایی شد. استفاده از موقعیت پیش‌فرض.")
             return "🇺🇸", "Cloudflare"

        try:
            ip = socket.gethostbyname(address)
            
            with self._lock: 
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
                    with self._lock: 
                        self.ip_location_cache[ip] = (flag, country)
                    logger.debug(f"موقعیت IP '{ip}' از API {api_func.__name__} دریافت شد: {flag} {country}")
                    return flag, country
                
        except socket.gaierror:
            logger.debug(f"نام میزبان قابل حل نیست: '{address}'. موقعیت 'نامشخص' خواهد بود.") 
        except Exception as e:
            logger.error(f"خطای کلی در دریافت موقعیت برای '{address}': {str(e)}")
            
        with self._lock: 
            self.ip_location_cache[address] = ("🏳️", "Unknown") 
        return "🏳️", "Unknown"


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
                logger.debug(f"در حال تلاش برای واکشی '{url}' (تلاش {attempt + 1}/{self.config.MAX_RETRIES})")
                response = self.session.get(url, timeout=self.config.REQUEST_TIMEOUT)
                response.raise_for_status() 
                return response
            except requests.RequestException as e:
                if attempt == self.config.MAX_RETRIES - 1:
                    logger.error(f"واکشی '{url}' پس از {self.config.MAX_RETRIES} تلاش ناموفق بود: {str(e)}")
                    return None
                wait_time = min(self.config.RETRY_DELAY * backoff, 60)
                logger.warning(f"تلاش {attempt + 1} برای '{url}' ناموفق بود. تلاش مجدد در {wait_time} ثانیه: {str(e)}")
                time.sleep(wait_time)
                backoff *= 2 
        return None

    def fetch_ssconf_configs(self, url: str) -> List[str]:
        """
        واکشی کانفیگ‌ها از URLهای ssconf:// با تبدیل آن‌ها به HTTPS و پردازش محتوا.
        """
        https_url = self.validator.convert_ssconf_to_https(url)
        configs = []
        logger.debug(f"در حال واکشی کانفیگ‌های ssconf از: '{https_url}'") 
        
        response = self.fetch_with_retry(https_url)
        if response and response.text.strip():
            text = response.text.strip()
            decoded_text = self.check_and_decode_base64(text)
            if decoded_text:
                logger.debug(f"محتوای ssconf از Base64 دیکد شد.")
                text = decoded_text
            
            found_configs = self.validator.split_configs(text)
            configs.extend(found_configs)
            logger.debug(f"{len(found_configs)} کانفیگ از ssconf '{https_url}' یافت شد.") 
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
        فیلتر کردن کانال‌هایی که به "bot" ختم می‌شوند.
        """
        # استخراج نام کاربری از URL
        channel_name_match = re.search(r't\.me/(?:s/)?([a-zA-Z0-9_]+)', new_channel_url)
        channel_name = channel_name_match.group(1) if channel_name_match else None

        if channel_name and channel_name.lower().endswith('bot'):
            logger.debug(f"URL تلگرام به یک ربات ختم می‌شود: '{new_channel_url}'. نادیده گرفته شد.")
            return # اگر بات بود، اضافه نکن

        is_new_channel = True
        with self._lock: 
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


    def _fetch_raw_data_for_channel(self, channel: ChannelConfig) -> Tuple[List[str], List[str], Dict[str, Any]]:
        """
        واکشی داده‌های خام (رشته‌های کانفیگ) و لینک‌های کانال جدید از یک کانال.
        این متد برای اجرای موازی طراحی شده است.
        """
        raw_configs_from_channel: List[str] = []
        new_channel_urls_from_channel: List[str] = []
        
        channel_status_info: Dict[str, Any] = {
            'url': channel.url,
            'success': False,
            'response_time': 0,
            'valid_configs_count': 0, 
            'total_configs_raw': 0,
            'error_message': None 
        }

        start_time = time.time()
        
        try:
            if channel.url.startswith('ssconf://'):
                logger.debug(f"کانال '{channel.url}' به عنوان منبع ssconf:// شناسایی شد.")
                raw_configs_from_channel = self.fetch_ssconf_configs(channel.url)
            else:
                response = self.fetch_with_retry(channel.url)
                if not response:
                    channel_status_info['error_message'] = "واکشی HTTP ناموفق بود."
                    return raw_configs_from_channel, new_channel_urls_from_channel, channel_status_info

                if channel.is_telegram:
                    logger.debug(f"در حال تجزیه محتوای تلگرام برای کانال: '{channel.url}'.")
                    soup = BeautifulSoup(response.text, 'html.parser')
                    messages = soup.find_all('div', class_='tgme_widget_message_text')
                    
                    sorted_messages = sorted(
                        messages,
                        key=lambda message: self.extract_date_from_message(message) or datetime.min.replace(tzinfo=timezone.utc),
                        reverse=True
                    )
                    
                    for message_div in sorted_messages:
                        if not message_div or not message_div.text:
                            continue
                        
                        message_date = self.extract_date_from_message(message_div)
                        if not self.is_config_valid(message_div.text, message_date):
                            continue
                        
                        links_and_mentions = message_div.find_all('a', href=True)
                        for item in links_and_mentions:
                            href_url = item['href']
                            match_s = re.match(r'https?://t\.me/s/([a-zA-Z0-9_]+)', href_url)
                            match_direct = re.match(r'https?://t\.me/([a-zA-Z0-9_]+)', href_url)
                            
                            if match_s:
                                new_channel_urls_from_channel.append(f"https://t.me/s/{match_s.group(1)}")
                            elif match_direct:
                                new_channel_urls_from_channel.append(f"https://t.me/s/{match_direct.group(1)}")
                            
                            raw_configs_from_channel.extend(self.validator.split_configs(href_url))

                        text_content = message_div.text
                        if self.check_and_decode_base64(text_content):
                            raw_configs_from_channel.extend(self.validator.split_configs(self.check_and_decode_base64(text_content)))
                        else:
                            raw_configs_from_channel.extend(self.validator.split_configs(text_content))

                else: # برای کانال‌های غیرتلگرام
                    text_content = response.text
                    if self.check_and_decode_base64(text_content):
                        raw_configs_from_channel.extend(self.validator.split_configs(self.check_and_decode_base64(text_content)))
                    else:
                        raw_configs_from_channel.extend(self.validator.split_configs(text_content))
            
            channel_status_info['success'] = True
        except Exception as e:
            channel_status_info['success'] = False
            channel_status_info['error_message'] = str(e)
            logger.error(f"خطا در _fetch_raw_data_for_channel برای '{channel.url}': {str(e)}", exc_info=True)

        channel_status_info['response_time'] = time.time() - start_time
        channel_status_info['total_configs_raw'] = len(raw_configs_from_channel)
        
        return raw_configs_from_channel, new_channel_urls_from_channel, channel_status_info

    def _process_single_raw_config(self, raw_config_string: str) -> Optional[Dict[str, str]]:
        """
        پردازش یک کانفیگ خام: نرمال‌سازی، پاکسازی، اعتبارسنجی و افزودن اطلاعات پرچم و کشور.
        همچنین، تکراری‌زدایی دقیق را بر اساس شناسه کانونی انجام می‌دهد.
        این متد برای اجرای موازی طراحی شده و هیچ پارامتر کانالی را تغییر نمی‌دهد.
        """
        if not raw_config_string:
            logger.debug("رشته کانفیگ ورودی خالی است. نادیده گرفته شد.")
            return None

        config_string_for_processing = raw_config_string 
        if config_string_for_processing.startswith('hy2://'):
            config_string_for_processing = self.validator.normalize_hysteria2_protocol(config_string_for_processing)
        elif config_string_for_processing.startswith('hy1://'):
            config_string_for_processing = config_string_for_processing.replace('hy1://', 'hysteria://', 1) 
            
        flag = "🏳️"
        country = "Unknown"
        actual_protocol = None

        found_protocol_match = False
        
        # این لوپ برای پیدا کردن پروتکل اصلی کانفیگ است.
        for proto_prefix in self.config.SUPPORTED_PROTOCOLS:
            if config_string_for_processing.startswith(proto_prefix):
                actual_protocol = proto_prefix
                found_protocol_match = True
                break
            for alias in self.config.SUPPORTED_PROTOCOLS[proto_prefix].get('aliases', []):
                if config_string_for_processing.startswith(alias):
                    actual_protocol = proto_prefix 
                    config_string_for_processing = config_string_for_processing.replace(alias, proto_prefix, 1) 
                    found_protocol_match = True
                    break
            if found_protocol_match:
                break
        
        if not found_protocol_match:
            logger.debug(f"پروتکل برای کانفیگ خام شناسایی نشد: '{raw_config_string[:min(len(raw_config_string), 50)]}...'. نادیده گرفته شد.")
            return None


        if not self.config.is_protocol_enabled(actual_protocol):
            logger.debug(f"پروتکل '{actual_protocol}' فعال نیست. کانفیگ نادیده گرفته شد: '{config_string_for_processing[:min(len(config_string_for_processing), 50)]}...'.")
            return None 
        
        if actual_protocol == "vmess://":
            config_string_for_processing = self.validator.clean_vmess_config(config_string_for_processing)
        elif actual_protocol == "ssr://":
            config_string_for_processing = self.validator.clean_ssr_config(config_string_for_processing)
        
        clean_config = self.validator.clean_config(config_string_for_processing)
        
        if self.validator.validate_protocol_config(clean_config, actual_protocol):
            canonical_id = self.validator.get_canonical_id(clean_config, actual_protocol)
            
            if canonical_id is None:
                logger.debug(f"شناسه کانونی برای کانفیگ '{actual_protocol}' تولید نشد. نادیده گرفته شد: '{clean_config[:min(len(clean_config), 50)]}...'.")
                return None
                        
            with self._lock: 
                if canonical_id not in self.seen_configs:
                    server_address = self.validator.get_server_address(clean_config, actual_protocol)
                    if server_address:
                        flag, country = self.get_location(server_address)
                    
                    self.seen_configs.add(canonical_id) 
                    self.protocol_counts[actual_protocol] = self.protocol_counts.get(actual_protocol, 0) + 1 
                    
                    logger.debug(f"کانفیگ منحصر به فرد '{actual_protocol}' یافت شد: '{clean_config[:min(len(clean_config), 50)]}...' (ID: {canonical_id[:min(len(canonical_id), 20)]}...).")
                    
                    return {
                        'config': clean_config, 
                        'protocol': actual_protocol,
                        'flag': flag,
                        'country': country,
                        'canonical_id': canonical_id 
                    }
                else:
                    logger.debug(f"کانفیگ تکراری '{actual_protocol}' با شناسه کانونی {canonical_id[:min(len(canonical_id), 20)]}... نادیده گرفته شد: '{clean_config[:min(len(clean_config), 50)]}...'.")
            
        else: # این `else` مربوط به `if self.validator.validate_protocol_config`
            logger.debug(f"اعتبارسنجی پروتکل '{actual_protocol}' برای کانفیگ '{clean_config[:min(len(clean_config), 50)]}...' ناموفق بود. نادیده گرفته شد.")
            
        logger.debug(f"کانفیگ '{raw_config_string[:min(len(raw_config_string), 50)]}...' با هیچ پروتکل فعال یا معتبری مطابقت نداشت یا پردازش نشد. نادیده گرفته شد.")
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
        sorted_protocols = sorted(
            protocol_configs.items(),
            key=lambda x: (
                self.config.SUPPORTED_PROTOCOLS.get(x[0], {"priority": 999})["priority"], 
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
                num_to_add = min(
                    protocol_info["max_configs"],  
                    len(protocol_config_list)     
                )
                balanced_configs.extend(protocol_config_list[:num_to_add])
                logger.info(f"پروتکل '{protocol}': {num_to_add} کانفیگ اضافه شد (از {len(protocol_config_list)} موجود، حداکثر مجاز: {protocol_info['max_configs']}).")
            elif protocol_info["flexible_max"] and len(protocol_config_list) > 0:
                balanced_configs.extend(protocol_config_list)
                logger.info(f"پروتکل '{protocol}': {len(protocol_config_list)} کانفیگ اضافه شد (حالت flexible_max).")
            else:
                logger.debug(f"پروتکل '{protocol}': تعداد کانفیگ‌های کافی یافت نشد ({len(protocol_config_list)}).")
        
        logger.info(f"توازن پروتکل‌ها کامل شد. مجموعاً {len(balanced_configs)} کانفیگ نهایی.")
        return balanced_configs

    def run_full_pipeline(self):
        """
        متد اصلی برای اجرای کامل pipeline واکشی، پردازش، توازن و ذخیره کانفیگ‌ها.
        """
        all_raw_configs_collected: List[str] = []
        all_new_channel_urls_discovered: Set[str] = set()
        
        channels_to_process = []
        now = datetime.now(timezone.utc)
        
        logger.info(f"در حال فیلتر کردن کانال‌ها برای پردازش. زمان فعلی: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}.")
        for channel in list(self.config.SOURCE_URLS):
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

        logger.info(f"شروع فاز ۱: واکشی موازی داده‌های خام و کشف اولیه کانال‌ها از {total_channels_to_process} کانال فعال...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(10, total_channels_to_process + 1)) as executor:
            futures = {executor.submit(self._fetch_raw_data_for_channel, channel): channel for channel in channels_to_process}
            
            processed_channels_count = 0
            for future in concurrent.futures.as_completed(futures):
                channel_processed = futures[future]
                processed_channels_count += 1
                progress_percentage = (processed_channels_count / total_channels_to_process) * 100
                
                try:
                    raw_configs, new_channel_urls, channel_status_info = future.result()
                    
                    channel_processed.metrics.total_configs = channel_status_info['total_configs_raw']
                    
                    if channel_status_info['success']:
                        self.config.update_channel_stats(channel_processed, True, channel_status_info['response_time'])
                        self.config.adjust_protocol_limits(channel_processed)
                        channel_processed.retry_level = 0
                        channel_processed.next_check_time = None
                        logger.info(f"پیشرفت: {progress_percentage:.2f}% ({processed_channels_count}/{total_channels_to_process}) - کانال '{channel_processed.url}' واکشی شد. ({len(raw_configs)} کانفیگ خام، {len(new_channel_urls)} کانال جدید پیدا شد).")
                    else:
                        self.config.update_channel_stats(channel_processed, False, channel_status_info['response_time'])
                        channel_processed.retry_level = min(channel_processed.retry_level + 1, self.max_retry_level)
                        channel_processed.next_check_time = datetime.now(timezone.utc) + self.retry_intervals[channel_processed.retry_level]
                        logger.warning(f"پیشرفت: {progress_percentage:.2f}% ({processed_channels_count}/{total_channels_to_process}) - کانال '{channel_processed.url}' واکشی ناموفق بود. خطا: {channel_status_info.get('error_message', 'نامعلوم')}. (بررسی بعدی: {channel_processed.next_check_time.strftime('%Y-%m-%d %H:%M:%S UTC')})")
                        
                    all_raw_configs_collected.extend(raw_configs)
                    for url in new_channel_urls:
                        all_new_channel_urls_discovered.add(url) 
                        
                except Exception as exc:
                    logger.error(f"پیشرفت: {progress_percentage:.2f}% ({processed_channels_count}/{total_channels_to_process}) - کانال '{channel_processed.url}' در حین واکشی موازی با خطا مواجه شد: {exc}", exc_info=True)

        logger.info(f"فاز ۱ تکمیل شد. مجموعاً {len(all_raw_configs_collected)} کانفیگ خام و {len(all_new_channel_urls_discovered)} URL کانال جدید کشف شد.")

        logger.info("شروع فاز ۲: کشف کانال‌ها از تمامی کانفیگ‌های خام و اضافه کردن به لیست منابع اصلی...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures_phase2 = []
            for raw_cfg_string in all_raw_configs_collected:
                futures_phase2.append(executor.submit(self.validator.extract_telegram_channels_from_config, raw_cfg_string))
            
            for future in concurrent.futures.as_completed(futures_phase2):
                try:
                    discovered_from_config = future.result()
                    for new_url in discovered_from_config:
                        all_new_channel_urls_discovered.add(new_url)
                except Exception as exc:
                    logger.error(f"خطا در استخراج کانال از کانفیگ خام (فاز 2): {exc}", exc_info=True)

        for new_url in all_new_channel_urls_discovered:
            self.add_new_telegram_channel(new_url)
        logger.info(f"فاز ۲ تکمیل شد. لیست منابع اصلی اکنون شامل {len(self.config.SOURCE_URLS)} کانال است (پس از اضافه شدن موارد جدید).")

        logger.info("شروع فاز ۳: پردازش و حذف دقیق تکراری‌ها (بر اساس شناسه کانونی) به صورت موازی...")
        final_enriched_configs: List[Dict[str, str]] = []
        
        if not all_raw_configs_collected:
            logger.info("هیچ کانفیگ خامی برای پردازش در فاز ۳ یافت نشد. فاز ۳ نادیده گرفته شد.")
            logger.info("فاز ۳ تکمیل شد. مجموعاً 0 کانفیگ منحصر به فرد و غنی شده آماده توازن.")
            return [] 


        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor: 
            futures = {executor.submit(self._process_single_raw_config, cfg_str): cfg_str for cfg_str in all_raw_configs_collected}
            
            processed_configs_count_phase3 = 0
            if len(all_raw_configs_collected) > 0:
                for future in concurrent.futures.as_completed(futures):
                    processed_configs_count_phase3 += 1
                    progress_percentage_phase3 = (processed_configs_count_phase3 / len(all_raw_configs_collected)) * 100
                    
                    try:
                        enriched_config_dict = future.result()
                        if enriched_config_dict:
                            final_enriched_configs.append(enriched_config_dict)
                        
                        if processed_configs_count_phase3 % 100 == 0 or processed_configs_count_phase3 == len(all_raw_configs_collected):
                             logger.info(f"پیشرفت فاز ۳: {progress_percentage_phase3:.2f}% ({processed_configs_count_phase3}/{len(all_raw_configs_collected)}) کانفیگ خام پردازش شد. (کانفیگ‌های منحصر به فرد تاکنون: {len(final_enriched_configs)})")
                    except Exception as exc:
                        logger.error(f"خطا در پردازش موازی کانفیگ خام: '{futures[future][:min(len(futures[future]), 50)]}...': {exc}", exc_info=True)
            else:
                logger.info("هیچ کانفیگ خامی برای پردازش در فاز ۳ یافت نشد.")

        logger.info(f"فاز ۳ تکمیل شد. مجموعاً {len(final_enriched_configs)} کانفیگ منحصر به فرد و غنی شده آماده توازن.")

        logger.info("شروع فاز ۴: توازن پروتکل و ذخیره خروجی‌ها...")
        final_configs_balanced = self.balance_protocols(final_enriched_configs)
        logger.info(f"فاز ۴ تکمیل شد. {len(final_configs_balanced)} کانفیگ نهایی پس از توازن آماده ذخیره.")

        return final_configs_balanced

    def _process_single_raw_config(self, raw_config_string: str) -> Optional[Dict[str, str]]:
        """
        پردازش یک کانفیگ خام (برای فاز 3).
        این متد کاملا مستقل از شیء 'channel' است و فقط کانفیگ رشته را پردازش می‌کند.
        """
        if not raw_config_string:
            logger.debug("رشته کانفیگ ورودی خالی است. نادیده گرفته شد.")
            return None

        config_string_for_processing = raw_config_string 
        if config_string_for_processing.startswith('hy2://'):
            config_string_for_processing = self.validator.normalize_hysteria2_protocol(config_string_for_processing)
        elif config_string_for_processing.startswith('hy1://'):
            config_string_for_processing = config_string_for_processing.replace('hy1://', 'hysteria://', 1) 
            
        flag = "🏳️"
        country = "Unknown"
        actual_protocol = None

        found_protocol_match = False
        
        # این لوپ برای پیدا کردن پروتکل اصلی کانفیگ است.
        # پروتکل‌های پشتیبانی شده در config.py تعریف شده‌اند.
        for proto_prefix in self.config.SUPPORTED_PROTOCOLS:
            if config_string_for_processing.startswith(proto_prefix):
                actual_protocol = proto_prefix
                found_protocol_match = True
                break
            for alias in self.config.SUPPORTED_PROTOCOLS[proto_prefix].get('aliases', []):
                if config_string_for_processing.startswith(alias):
                    actual_protocol = proto_prefix 
                    config_string_for_processing = config_string_for_processing.replace(alias, proto_prefix, 1) 
                    found_protocol_match = True
                    break
            if found_protocol_match:
                break
        
        if not found_protocol_match:
            logger.debug(f"پروتکل برای کانفیگ خام شناسایی نشد: '{raw_config_string[:min(len(raw_config_string), 50)]}...'. نادیده گرفته شد.")
            return None


        if not self.config.is_protocol_enabled(actual_protocol):
            logger.debug(f"پروتکل '{actual_protocol}' فعال نیست. کانفیگ نادیده گرفته شد: '{config_string_for_processing[:min(len(config_string_for_processing), 50)]}...'.")
            return None 
        
        if actual_protocol == "vmess://":
            config_string_for_processing = self.validator.clean_vmess_config(config_string_for_processing)
        elif actual_protocol == "ssr://":
            config_string_for_processing = self.validator.clean_ssr_config(config_string_for_processing)
        
        clean_config = self.validator.clean_config(config_string_for_processing)
        
        if self.validator.validate_protocol_config(clean_config, actual_protocol):
            canonical_id = self.validator.get_canonical_id(clean_config, actual_protocol)
            
            if canonical_id is None:
                logger.debug(f"شناسه کانونی برای کانفیگ '{actual_protocol}' تولید نشد. نادیده گرفته شد: '{clean_config[:min(len(clean_config), 50)]}...'.")
                return None
                        
            with self._lock: 
                if canonical_id not in self.seen_configs:
                    server_address = self.validator.get_server_address(clean_config, actual_protocol)
                    if server_address:
                        flag, country = self.get_location(server_address)
                    
                    self.seen_configs.add(canonical_id) 
                    self.protocol_counts[actual_protocol] = self.protocol_counts.get(actual_protocol, 0) + 1 
                    
                    logger.debug(f"کانفیگ منحصر به فرد '{actual_protocol}' یافت شد: '{clean_config[:min(len(clean_config), 50)]}...' (ID: {canonical_id[:min(len(canonical_id), 20)]}...).")
                    
                    return {
                        'config': clean_config, 
                        'protocol': actual_protocol,
                        'flag': flag,
                        'country': country,
                        'canonical_id': canonical_id 
                    }
                else:
                    logger.debug(f"کانفیگ تکراری '{actual_protocol}' با شناسه کانونی {canonical_id[:min(len(canonical_id), 20)]}... نادیده گرفته شد: '{clean_config[:min(len(clean_config), 50)]}...'.")
            
        else: # این `else` مربوط به `if self.validator.validate_protocol_config`
            logger.debug(f"اعتبارسنجی پروتکل '{actual_protocol}' برای کانفیگ '{clean_config[:min(len(clean_config), 50)]}...' ناموفق بود. نادیده گرفته شد.")
            
        logger.debug(f"کانفیگ '{raw_config_string[:min(len(raw_config_string), 50)]}...' با هیچ پروتکل فعال یا معتبری مطابقت نداشت یا پردازش نشد. نادیده گرفته شد.")
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
        sorted_protocols = sorted(
            protocol_configs.items(),
            key=lambda x: (
                self.config.SUPPORTED_PROTOCOLS.get(x[0], {"priority": 999})["priority"], 
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
                num_to_add = min(
                    protocol_info["max_configs"],  
                    len(protocol_config_list)     
                )
                balanced_configs.extend(protocol_config_list[:num_to_add])
                logger.info(f"پروتکل '{protocol}': {num_to_add} کانفیگ اضافه شد (از {len(protocol_config_list)} موجود، حداکثر مجاز: {protocol_info['max_configs']}).")
            elif protocol_info["flexible_max"] and len(protocol_config_list) > 0:
                balanced_configs.extend(protocol_config_list)
                logger.info(f"پروتکل '{protocol}': {len(protocol_config_list)} کانفیگ اضافه شد (حالت flexible_max).")
            else:
                logger.debug(f"پروتکل '{protocol}': تعداد کانفیگ‌های کافی یافت نشد ({len(protocol_config_list)}).")
        
        logger.info(f"توازن پروتکل‌ها کامل شد. مجموعاً {len(balanced_configs)} کانفیگ نهایی.")
        return balanced_configs

    def run_full_pipeline(self):
        """
        متد اصلی برای اجرای کامل pipeline واکشی، پردازش، توازن و ذخیره کانفیگ‌ها.
        """
        all_raw_configs_collected: List[str] = []
        all_new_channel_urls_discovered: Set[str] = set()
        
        channels_to_process = []
        now = datetime.now(timezone.utc)
        
        logger.info(f"در حال فیلتر کردن کانال‌ها برای پردازش. زمان فعلی: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}.")
        for channel in list(self.config.SOURCE_URLS):
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

        logger.info(f"شروع فاز ۱: واکشی موازی داده‌های خام و کشف اولیه کانال‌ها از {total_channels_to_process} کانال فعال...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(10, total_channels_to_process + 1)) as executor:
            futures = {executor.submit(self._fetch_raw_data_for_channel, channel): channel for channel in channels_to_process}
            
            processed_channels_count = 0
            for future in concurrent.futures.as_completed(futures):
                channel_processed = futures[future]
                processed_channels_count += 1
                progress_percentage = (processed_channels_count / total_channels_to_process) * 100
                
                try:
                    raw_configs, new_channel_urls, channel_status_info = future.result()
                    
                    channel_processed.metrics.total_configs = channel_status_info['total_configs_raw']
                    
                    if channel_status_info['success']:
                        self.config.update_channel_stats(channel_processed, True, channel_status_info['response_time'])
                        self.config.adjust_protocol_limits(channel_processed)
                        channel_processed.retry_level = 0
                        channel_processed.next_check_time = None
                        logger.info(f"پیشرفت: {progress_percentage:.2f}% ({processed_channels_count}/{total_channels_to_process}) - کانال '{channel_processed.url}' واکشی شد. ({len(raw_configs)} کانفیگ خام، {len(new_channel_urls)} کانال جدید پیدا شد).")
                    else:
                        self.config.update_channel_stats(channel_processed, False, channel_status_info['response_time'])
                        channel_processed.retry_level = min(channel_processed.retry_level + 1, self.max_retry_level)
                        channel_processed.next_check_time = datetime.now(timezone.utc) + self.retry_intervals[channel_processed.retry_level]
                        logger.warning(f"پیشرفت: {progress_percentage:.2f}% ({processed_channels_count}/{total_channels_to_process}) - کانال '{channel_processed.url}' واکشی ناموفق بود. خطا: {channel_status_info.get('error_message', 'نامعلوم')}. (بررسی بعدی: {channel_processed.next_check_time.strftime('%Y-%m-%d %H:%M:%S UTC')})")
                        
                    all_raw_configs_collected.extend(raw_configs)
                    for url in new_channel_urls:
                        all_new_channel_urls_discovered.add(url) 
                        
                except Exception as exc:
                    logger.error(f"پیشرفت: {progress_percentage:.2f}% ({processed_channels_count}/{total_channels_to_process}) - کانال '{channel_processed.url}' در حین واکشی موازی با خطا مواجه شد: {exc}", exc_info=True)

        logger.info(f"فاز ۱ تکمیل شد. مجموعاً {len(all_raw_configs_collected)} کانفیگ خام و {len(all_new_channel_urls_discovered)} URL کانال جدید کشف شد.")

        logger.info("شروع فاز ۲: کشف کانال‌ها از تمامی کانفیگ‌های خام و اضافه کردن به لیست منابع اصلی...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures_phase2 = []
            for raw_cfg_string in all_raw_configs_collected:
                futures_phase2.append(executor.submit(self.validator.extract_telegram_channels_from_config, raw_cfg_string))
            
            for future in concurrent.futures.as_completed(futures_phase2):
                try:
                    discovered_from_config = future.result()
                    for new_url in discovered_from_config:
                        all_new_channel_urls_discovered.add(new_url)
                except Exception as exc:
                    logger.error(f"خطا در استخراج کانال از کانفیگ خام (فاز 2): {exc}", exc_info=True)

        for new_url in all_new_channel_urls_discovered:
            self.add_new_telegram_channel(new_url)
        logger.info(f"فاز ۲ تکمیل شد. لیست منابع اصلی اکنون شامل {len(self.config.SOURCE_URLS)} کانال است (پس از اضافه شدن موارد جدید).")

        logger.info("شروع فاز ۳: پردازش و حذف دقیق تکراری‌ها (بر اساس شناسه کانونی) به صورت موازی...")
        final_enriched_configs: List[Dict[str, str]] = []
        
        if not all_raw_configs_collected:
            logger.info("هیچ کانفیگ خامی برای پردازش در فاز ۳ یافت نشد. فاز ۳ نادیده گرفته شد.")
            logger.info("فاز ۳ تکمیل شد. مجموعاً 0 کانفیگ منحصر به فرد و غنی شده آماده توازن.")
            return [] 


        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor: 
            futures = {executor.submit(self._process_single_raw_config, cfg_str): cfg_str for cfg_str in all_raw_configs_collected}
            
            processed_configs_count_phase3 = 0
            if len(all_raw_configs_collected) > 0:
                for future in concurrent.futures.as_completed(futures):
                    processed_configs_count_phase3 += 1
                    progress_percentage_phase3 = (processed_configs_count_phase3 / len(all_raw_configs_collected)) * 100
                    
                    try:
                        enriched_config_dict = future.result()
                        if enriched_config_dict:
                            final_enriched_configs.append(enriched_config_dict)
                        
                        if processed_configs_count_phase3 % 100 == 0 or processed_configs_count_phase3 == len(all_raw_configs_collected):
                             logger.info(f"پیشرفت فاز ۳: {progress_percentage_phase3:.2f}% ({processed_configs_count_phase3}/{len(all_raw_configs_collected)}) کانفیگ خام پردازش شد. (کانفیگ‌های منحصر به فرد تاکنون: {len(final_enriched_configs)})")
                    except Exception as exc:
                        logger.error(f"خطا در پردازش موازی کانفیگ خام: '{futures[future][:min(len(futures[future]), 50)]}...': {exc}", exc_info=True)
            else:
                logger.info("هیچ کانفیگ خامی برای پردازش در فاز ۳ یافت نشد.")

        logger.info(f"فاز ۳ تکمیل شد. مجموعاً {len(final_enriched_configs)} کانفیگ منحصر به فرد و غنی شده آماده توازن.")

        logger.info("شروع فاز ۴: توازن پروتکل و ذخیره خروجی‌ها...")
        final_configs_balanced = self.balance_protocols(final_enriched_configs)
        logger.info(f"فاز ۴ تکمیل شد. {len(final_configs_balanced)} کانفیگ نهایی پس از توازن آماده ذخیره.")

        return final_configs_balanced

    def _process_single_raw_config(self, raw_config_string: str) -> Optional[Dict[str, str]]:
        """
        **تغییر یافته**: پردازش یک کانفیگ خام (برای فاز 3).
        این متد کاملا مستقل از شیء 'channel' است و فقط کانفیگ رشته را پردازش می‌کند.
        """
        if not raw_config_string:
            logger.debug("رشته کانفیگ ورودی خالی است. نادیده گرفته شد.")
            return None

        config_string_for_processing = raw_config_string 
        if config_string_for_processing.startswith('hy2://'):
            config_string_for_processing = self.validator.normalize_hysteria2_protocol(config_string_for_processing)
        elif config_string_for_processing.startswith('hy1://'):
            config_string_for_processing = config_string_for_processing.replace('hy1://', 'hysteria://', 1) 
            
        flag = "🏳️"
        country = "Unknown"
        actual_protocol = None

        found_protocol_match = False
        
        # این لوپ برای پیدا کردن پروتکل اصلی کانفیگ است.
        for proto_prefix in self.config.SUPPORTED_PROTOCOLS:
            if config_string_for_processing.startswith(proto_prefix):
                actual_protocol = proto_prefix
                found_protocol_match = True
                break
            for alias in self.config.SUPPORTED_PROTOCOLS[proto_prefix].get('aliases', []):
                if config_string_for_processing.startswith(alias):
                    actual_protocol = proto_prefix 
                    config_string_for_processing = config_string_for_processing.replace(alias, proto_prefix, 1) 
                    found_protocol_match = True
                    break
            if found_protocol_match:
                break
        
        if not found_protocol_match:
            logger.debug(f"پروتکل برای کانفیگ خام شناسایی نشد: '{raw_config_string[:min(len(raw_config_string), 50)]}...'. نادیده گرفته شد.")
            return None


        if not self.config.is_protocol_enabled(actual_protocol):
            logger.debug(f"پروتکل '{actual_protocol}' فعال نیست. کانفیگ نادیده گرفته شد: '{config_string_for_processing[:min(len(config_string_for_processing), 50)]}...'.")
            return None 
        
        if actual_protocol == "vmess://":
            config_string_for_processing = self.validator.clean_vmess_config(config_string_for_processing)
        elif actual_protocol == "ssr://":
            config_string_for_processing = self.validator.clean_ssr_config(config_string_for_processing)
        
        clean_config = self.validator.clean_config(config_string_for_processing)
        
        if self.validator.validate_protocol_config(clean_config, actual_protocol):
            canonical_id = self.validator.get_canonical_id(clean_config, actual_protocol)
            
            if canonical_id is None:
                logger.debug(f"شناسه کانونی برای کانفیگ '{actual_protocol}' تولید نشد. نادیده گرفته شد: '{clean_config[:min(len(clean_config), 50)]}...'.")
                return None
                        
            with self._lock: 
                if canonical_id not in self.seen_configs:
                    server_address = self.validator.get_server_address(clean_config, actual_protocol)
                    if server_address:
                        flag, country = self.get_location(server_address)
                    
                    self.seen_configs.add(canonical_id) 
                    self.protocol_counts[actual_protocol] = self.protocol_counts.get(actual_protocol, 0) + 1 
                    
                    logger.debug(f"کانفیگ منحصر به فرد '{actual_protocol}' یافت شد: '{clean_config[:min(len(clean_config), 50)]}...' (ID: {canonical_id[:min(len(canonical_id), 20)]}...).")
                    
                    return {
                        'config': clean_config, 
                        'protocol': actual_protocol,
                        'flag': flag,
                        'country': country,
                        'canonical_id': canonical_id 
                    }
                else:
                    logger.debug(f"کانفیگ تکراری '{actual_protocol}' با شناسه کانونی {canonical_id[:min(len(canonical_id), 20)]}... نادیده گرفته شد: '{clean_config[:min(len(clean_config), 50)]}...'.")
            
        else: # این `else` مربوط به `if self.validator.validate_protocol_config`
            logger.debug(f"اعتبارسنجی پروتکل '{actual_protocol}' برای کانفیگ '{clean_config[:min(len(clean_config), 50)]}...' ناموفق بود. نادیده گرفته شد.")
            
        logger.debug(f"کانفیگ '{raw_config_string[:min(len(raw_config_string), 50)]}...' با هیچ پروتکل فعال یا معتبری مطابقت نداشت یا پردازش نشد. نادیده گرفته شد.")
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
        sorted_protocols = sorted(
            protocol_configs.items(),
            key=lambda x: (
                self.config.SUPPORTED_PROTOCOLS.get(x[0], {"priority": 999})["priority"], 
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
                num_to_add = min(
                    protocol_info["max_configs"],  
                    len(protocol_config_list)     
                )
                balanced_configs.extend(protocol_config_list[:num_to_add])
                logger.info(f"پروتکل '{protocol}': {num_to_add} کانفیگ اضافه شد (از {len(protocol_config_list)} موجود، حداکثر مجاز: {protocol_info['max_configs']}).")
            elif protocol_info["flexible_max"] and len(protocol_config_list) > 0:
                balanced_configs.extend(protocol_config_list)
                logger.info(f"پروتکل '{protocol}': {len(protocol_config_list)} کانفیگ اضافه شد (حالت flexible_max).")
            else:
                logger.debug(f"پروتکل '{protocol}': تعداد کانفیگ‌های کافی یافت نشد ({len(protocol_config_list)}).")
        
        logger.info(f"توازن پروتکل‌ها کامل شد. مجموعاً {len(balanced_configs)} کانفیگ نهایی.")
        return balanced_configs

    def run_full_pipeline(self):
        """
        متد اصلی برای اجرای کامل pipeline واکشی، پردازش، توازن و ذخیره کانفیگ‌ها.
        """
        all_raw_configs_collected: List[str] = []
        all_new_channel_urls_discovered: Set[str] = set()
        
        channels_to_process = []
        now = datetime.now(timezone.utc)
        
        logger.info(f"در حال فیلتر کردن کانال‌ها برای پردازش. زمان فعلی: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}.")
        for channel in list(self.config.SOURCE_URLS):
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

        logger.info(f"شروع فاز ۱: واکشی موازی داده‌های خام و کشف اولیه کانال‌ها از {total_channels_to_process} کانال فعال...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(10, total_channels_to_process + 1)) as executor:
            futures = {executor.submit(self._fetch_raw_data_for_channel, channel): channel for channel in channels_to_process}
            
            processed_channels_count = 0
            for future in concurrent.futures.as_completed(futures):
                channel_processed = futures[future]
                processed_channels_count += 1
                progress_percentage = (processed_channels_count / total_channels_to_process) * 100
                
                try:
                    raw_configs, new_channel_urls, channel_status_info = future.result()
                    
                    channel_processed.metrics.total_configs = channel_status_info['total_configs_raw']
                    
                    if channel_status_info['success']:
                        self.config.update_channel_stats(channel_processed, True, channel_status_info['response_time'])
                        self.config.adjust_protocol_limits(channel_processed)
                        channel_processed.retry_level = 0
                        channel_processed.next_check_time = None
                        logger.info(f"پیشرفت: {progress_percentage:.2f}% ({processed_channels_count}/{total_channels_to_process}) - کانال '{channel_processed.url}' واکشی شد. ({len(raw_configs)} کانفیگ خام، {len(new_channel_urls)} کانال جدید پیدا شد).")
                    else:
                        self.config.update_channel_stats(channel_processed, False, channel_status_info['response_time'])
                        channel_processed.retry_level = min(channel_processed.retry_level + 1, self.max_retry_level)
                        channel_processed.next_check_time = datetime.now(timezone.utc) + self.retry_intervals[channel_processed.retry_level]
                        logger.warning(f"پیشرفت: {progress_percentage:.2f}% ({processed_channels_count}/{total_channels_to_process}) - کانال '{channel_processed.url}' واکشی ناموفق بود. خطا: {channel_status_info.get('error_message', 'نامعلوم')}. (بررسی بعدی: {channel_processed.next_check_time.strftime('%Y-%m-%d %H:%M:%S UTC')})")
                        
                    all_raw_configs_collected.extend(raw_configs)
                    for url in new_channel_urls:
                        all_new_channel_urls_discovered.add(url) 
                        
                except Exception as exc:
                    logger.error(f"پیشرفت: {progress_percentage:.2f}% ({processed_channels_count}/{total_channels_to_process}) - کانال '{channel_processed.url}' در حین واکشی موازی با خطا مواجه شد: {exc}", exc_info=True)

        logger.info(f"فاز ۱ تکمیل شد. مجموعاً {len(all_raw_configs_collected)} کانفیگ خام و {len(all_new_channel_urls_discovered)} URL کانال جدید کشف شد.")

        logger.info("شروع فاز ۲: کشف کانال‌ها از تمامی کانفیگ‌های خام و اضافه کردن به لیست منابع اصلی...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures_phase2 = []
            for raw_cfg_string in all_raw_configs_collected:
                futures_phase2.append(executor.submit(self.validator.extract_telegram_channels_from_config, raw_cfg_string))
            
            for future in concurrent.futures.as_completed(futures_phase2):
                try:
                    discovered_from_config = future.result()
                    for new_url in discovered_from_config:
                        all_new_channel_urls_discovered.add(new_url)
                except Exception as exc:
                    logger.error(f"خطا در استخراج کانال از کانفیگ خام (فاز 2): {exc}", exc_info=True)

        for new_url in all_new_channel_urls_discovered:
            self.add_new_telegram_channel(new_url)
        logger.info(f"فاز ۲ تکمیل شد. لیست منابع اصلی اکنون شامل {len(self.config.SOURCE_URLS)} کانال است (پس از اضافه شدن موارد جدید).")

        logger.info("شروع فاز ۳: پردازش و حذف دقیق تکراری‌ها (بر اساس شناسه کانونی) به صورت موازی...")
        final_enriched_configs: List[Dict[str, str]] = []
        
        if not all_raw_configs_collected:
            logger.info("هیچ کانفیگ خامی برای پردازش در فاز ۳ یافت نشد. فاز ۳ نادیده گرفته شد.")
            logger.info("فاز ۳ تکمیل شد. مجموعاً 0 کانفیگ منحصر به فرد و غنی شده آماده توازن.")
            return [] 


        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor: 
            futures = {executor.submit(self._process_single_raw_config, cfg_str): cfg_str for cfg_str in all_raw_configs_collected}
            
            processed_configs_count_phase3 = 0
            if len(all_raw_configs_collected) > 0:
                for future in concurrent.futures.as_completed(futures):
                    processed_configs_count_phase3 += 1
                    progress_percentage_phase3 = (processed_configs_count_phase3 / len(all_raw_configs_collected)) * 100
                    
                    try:
                        enriched_config_dict = future.result()
                        if enriched_config_dict:
                            final_enriched_configs.append(enriched_config_dict)
                        
                        if processed_configs_count_phase3 % 100 == 0 or processed_configs_count_phase3 == len(all_raw_configs_collected):
                             logger.info(f"پیشرفت فاز ۳: {progress_percentage_phase3:.2f}% ({processed_configs_count_phase3}/{len(all_raw_configs_collected)}) کانفیگ خام پردازش شد. (کانفیگ‌های منحصر به فرد تاکنون: {len(final_enriched_configs)})")
                    except Exception as exc:
                        logger.error(f"خطا در پردازش موازی کانفیگ خام: '{futures[future][:min(len(futures[future]), 50)]}...': {exc}", exc_info=True)
            else:
                logger.info("هیچ کانفیگ خامی برای پردازش در فاز ۳ یافت نشد.")

        logger.info(f"فاز ۳ تکمیل شد. مجموعاً {len(final_enriched_configs)} کانفیگ منحصر به فرد و غنی شده آماده توازن.")

        logger.info("شروع فاز ۴: توازن پروتکل و ذخیره خروجی‌ها...")
        final_configs_balanced = self.balance_protocols(final_enriched_configs)
        logger.info(f"فاز ۴ تکمیل شد. {len(final_configs_balanced)} کانفیگ نهایی پس از توازن آماده ذخیره.")

        return final_configs_balanced

    def _process_single_raw_config(self, raw_config_string: str) -> Optional[Dict[str, str]]:
        """
        پردازش یک کانفیگ خام (برای فاز 3).
        این متد کاملا مستقل از شیء 'channel' است و فقط کانفیگ رشته را پردازش می‌کند.
        """
        if not raw_config_string:
            logger.debug("رشته کانفیگ ورودی خالی است. نادیده گرفته شد.")
            return None

        config_string_for_processing = raw_config_string 
        if config_string_for_processing.startswith('hy2://'):
            config_string_for_processing = self.validator.normalize_hysteria2_protocol(config_string_for_processing)
        elif config_string_for_processing.startswith('hy1://'):
            config_string_for_processing = config_string_for_processing.replace('hy1://', 'hysteria://', 1) 
            
        flag = "🏳️"
        country = "Unknown"
        actual_protocol = None

        found_protocol_match = False
        
        # **تغییر یافته**: بهبود حلقه پیدا کردن پروتکل اصلی.
        # ابتدا پروتکل را از رشته خام پیدا می‌کنیم.
        for proto_prefix in self.config.SUPPORTED_PROTOCOLS:
            if config_string_for_processing.startswith(proto_prefix):
                actual_protocol = proto_prefix
                found_protocol_match = True
                break
            for alias in self.config.SUPPORTED_PROTOCOLS[proto_prefix].get('aliases', []):
                if config_string_for_processing.startswith(alias):
                    actual_protocol = proto_prefix 
                    # اگر از alias استفاده شد، رشته را نرمال‌سازی کنید تا در ادامه کار درست باشد
                    config_string_for_processing = config_string_for_processing.replace(alias, proto_prefix, 1) 
                    found_protocol_match = True
                    break
            if found_protocol_match:
                break
        
        if not found_protocol_match:
            logger.debug(f"پروتکل برای کانفیگ خام شناسایی نشد: '{raw_config_string[:min(len(raw_config_string), 50)]}...'. نادیده گرفته شد.")
            return None


        if not self.config.is_protocol_enabled(actual_protocol):
            logger.debug(f"پروتکل '{actual_protocol}' فعال نیست. کانفیگ نادیده گرفته شد: '{config_string_for_processing[:min(len(config_string_for_processing), 50)]}...'.")
            return None 
        
        if actual_protocol == "vmess://":
            config_string_for_processing = self.validator.clean_vmess_config(config_string_for_processing)
        elif actual_protocol == "ssr://":
            config_string_for_processing = self.validator.clean_ssr_config(config_string_for_processing)
        
        clean_config = self.validator.clean_config(config_string_for_processing)
        
        if self.validator.validate_protocol_config(clean_config, actual_protocol):
            canonical_id = self.validator.get_canonical_id(clean_config, actual_protocol)
            
            if canonical_id is None:
                logger.debug(f"شناسه کانونی برای کانفیگ '{actual_protocol}' تولید نشد. نادیده گرفته شد: '{clean_config[:min(len(clean_config), 50)]}...'.")
                return None
                        
            with self._lock: 
                if canonical_id not in self.seen_configs:
                    server_address = self.validator.get_server_address(clean_config, actual_protocol)
                    if server_address:
                        flag, country = self.get_location(server_address)
                    
                    self.seen_configs.add(canonical_id) 
                    self.protocol_counts[actual_protocol] = self.protocol_counts.get(actual_protocol, 0) + 1 
                    
                    logger.debug(f"کانفیگ منحصر به فرد '{actual_protocol}' یافت شد: '{clean_config[:min(len(clean_config), 50)]}...' (ID: {canonical_id[:min(len(canonical_id), 20)]}...).")
                    
                    return {
                        'config': clean_config, 
                        'protocol': actual_protocol,
                        'flag': flag,
                        'country': country,
                        'canonical_id': canonical_id 
                    }
                else:
                    logger.debug(f"کانفیگ تکراری '{actual_protocol}' با شناسه کانونی {canonical_id[:min(len(canonical_id), 20)]}... نادیده گرفته شد: '{clean_config[:min(len(clean_config), 50)]}...'.")
            
        else: # این `else` مربوط به `if self.validator.validate_protocol_config`
            logger.debug(f"اعتبارسنجی پروتکل '{actual_protocol}' برای کانفیگ '{clean_config[:min(len(clean_config), 50)]}...' ناموفق بود. نادیده گرفته شد.")
            
        logger.debug(f"کانفیگ '{raw_config_string[:min(len(raw_config_string), 50)]}...' با هیچ پروتکل فعال یا معتبری مطابقت نداشت یا پردازش نشد. نادیده گرفته شد.")
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
        sorted_protocols = sorted(
            protocol_configs.items(),
            key=lambda x: (
                self.config.SUPPORTED_PROTOCOLS.get(x[0], {"priority": 999})["priority"], 
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
                num_to_add = min(
                    protocol_info["max_configs"],  
                    len(protocol_config_list)     
                )
                balanced_configs.extend(protocol_config_list[:num_to_add])
                logger.info(f"پروتکل '{protocol}': {num_to_add} کانفیگ اضافه شد (از {len(protocol_config_list)} موجود، حداکثر مجاز: {protocol_info['max_configs']}).")
            elif protocol_info["flexible_max"] and len(protocol_config_list) > 0:
                balanced_configs.extend(protocol_config_list)
                logger.info(f"پروتکل '{protocol}': {len(protocol_config_list)} کانفیگ اضافه شد (حالت flexible_max).")
            else:
                logger.debug(f"پروتکل '{protocol}': تعداد کانفیگ‌های کافی یافت نشد ({len(protocol_config_list)}).")
        
        logger.info(f"توازن پروتکل‌ها کامل شد. مجموعاً {len(balanced_configs)} کانفیگ نهایی.")
        return balanced_configs

    def _save_base64_file(self, file_path: str, content: str):
        """یک محتوا را Base64 می‌کند و در یک فایل ذخیره می‌کند."""
        try:
            encoded_content = base64.b64encode(content.encode('utf-8')).decode('utf-8')
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(encoded_content)
            logger.info(f"محتوای Base64 شده در '{file_path}' ذخیره شد.")
        except Exception as e:
            logger.error(f"خطا در ذخیره فایل Base64 شده '{file_path}': {str(e)}")

    def save_configs(self, configs: List[Dict[str, str]]):
        """
        ذخیره لیست نهایی کانفیگ‌ها در فایل‌های مختلف در ساختار پوشه جدید.
        حالا کانفیگ‌ها شامل اطلاعات پرچم و کشور هستند.
        """
        logger.info("در حال آماده‌سازی دایرکتوری‌های خروجی برای ذخیره کانفیگ‌ها...")
        os.makedirs(self.config.TEXT_OUTPUT_DIR, exist_ok=True)
        os.makedirs(self.config.BASE64_OUTPUT_DIR, exist_ok=True)
        os.makedirs(self.config.SINGBOX_OUTPUT_DIR, exist_ok=True)

        header = """//profile-title: base64:8J+RvUFub255bW91cy3wnZWP
//profile-update-interval: 1
//subscription-userinfo: upload=0; download=0; total=10737418240000000; expire=2546249531
//support-url: https://t.me/BXAMbot
//profile-web-page-url: https://github.com/4n0nymou3

"""
    
        full_text_lines = []
        for cfg_dict in configs:
            full_text_lines.append(f"{cfg_dict['flag']} {cfg_dict['country']} {cfg_dict['config']}")
        full_text_content = header + '\n\n'.join(full_text_lines) + '\n'

        full_file_path = os.path.join(self.config.TEXT_OUTPUT_DIR, 'proxy_configs.txt')
        try:
            with open(full_file_path, 'w', encoding='utf-8') as f:
                f.write(full_text_content)
            logger.info(f"با موفقیت {len(configs)} کانفیگ نهایی در '{full_file_path}' ذخیره شد.")
        except Exception as e:
            logger.error(f"خطا در ذخیره فایل کامل کانفیگ: {str(e)}")

        base64_full_file_path = os.path.join(self.config.BASE64_OUTPUT_DIR, "proxy_configs_base64.txt")
        self._save_base64_file(base64_full_file_path, full_text_content)

        protocol_configs_separated: Dict[str, List[Dict[str, str]]] = {p: [] for p in self.config.SUPPORTED_PROTOCOLS}
        for cfg_dict in configs:
            protocol_full_name = cfg_dict['protocol']
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

            protocol_name = protocol_full_name.replace('://', '')
            protocol_text_lines = []
            for cfg_dict in cfg_list_of_dicts:
                 protocol_text_lines.append(f"{cfg_dict['flag']} {cfg_dict['country']} {cfg_dict['config']}")
            protocol_text_content = header + '\n\n'.join(protocol_text_lines) + '\n'

            protocol_file_name = f"{protocol_name}.txt"
            protocol_file_path = os.path.join(config.TEXT_OUTPUT_DIR, protocol_file_name)
            try:
                with open(protocol_file_path, 'w', encoding='utf-8') as f:
                    f.write(protocol_text_content)
                logger.info(f"با موفقیت {len(cfg_list_of_dicts)} کانفیگ '{protocol_name}' در '{protocol_file_path}' ذخیره شد.")
            except Exception as e:
                logger.error(f"خطا در ذخیره فایل '{protocol_name}' کانفیگ: {str(e)}")

            base64_protocol_file_name = f"{protocol_name}_base64.txt"
            base64_protocol_file_path = os.path.join(config.BASE64_OUTPUT_DIR, base64_protocol_file_name)
            self._save_base64_file(base64_protocol_file_path, protocol_text_content)

def main():
    """
    تابع اصلی برای اجرای فرآیند واکشی و ذخیره کانفیگ‌ها.
    """
    try:
        logger.info("شروع فرآیند واکشی و پردازش کانفیگ‌ها...")
        config = ProxyConfig() 
        fetcher = ConfigFetcher(config) 
        
        configs = fetcher.run_full_pipeline() 

        if configs:
            fetcher.save_configs(configs)
            logger.info(f"فرآیند با موفقیت در {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')} به پایان رسید. مجموعاً {len(configs)} کانفیگ پردازش شد.")
            
            logger.info("تعداد کانفیگ‌ها بر اساس پروتکل:")
            for protocol, count in fetcher.protocol_counts.items():
                logger.info(f"  {protocol}: {count} کانفیگ")
        else:
            logger.error("هیچ کانفیگ معتبری یافت نشد و هیچ فایلی تولید نشد!")
            
        fetcher.save_channel_stats()
        logger.info("آمار کانال‌ها ذخیره شد.")

        fetcher.generate_channel_status_report()
            
    except Exception as e:
        logger.critical(f"خطای بحرانی در اجرای اصلی: {str(e)}", exc_info=True)

if __name__ == '__main__':
    main()

