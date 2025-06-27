import re
import os
import time
import json
import logging 
import socket 
import requests
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Set, Tuple, Any 

import concurrent.futures
import threading
from collections import defaultdict 

# **تغییر یافته**: وارد کردن کلاس‌ها بدون پیشوند 'src.'
from config import ProxyConfig, ChannelConfig
from config_validator import ConfigValidator
from source_fetcher import SourceFetcher 
from config_processor import ConfigProcessor 
from deduplicator import Deduplicator 
from connection_tester import ConnectionTester 
from output_manager import OutputManager 
from config_filter import ConfigFilter 
from user_settings import SOURCE_URLS, SPECIFIC_CONFIG_COUNT, BLOCKED_KEYWORDS, BLOCKED_IPS, BLOCKED_DOMAINS 
from user_settings import ALLOWED_COUNTRIES, BLOCKED_COUNTRIES, ALLOWED_PROTOCOLS 

# پیکربندی لاگ‌گیری (این پیکربندی در main.py انجام می‌شود)
logger = logging.getLogger(__name__)

class ConfigFetcher:
    """
    کلاس ConfigFetcher مسئول هماهنگی کلی فرآیند واکشی، پردازش، اعتبارسنجی و غنی‌سازی کانفیگ‌های پراکسی است.
    این کلاس اکنون Pipeline را به صورت هوشمند و مرحله‌ای برای تست و جمع‌آوری کانفیگ‌ها مدیریت می‌کند.
    """
    def __init__(self, config: ProxyConfig):
        """
        سازنده کلاس ConfigFetcher.
        """
        logger.info("در حال مقداردهی اولیه ConfigFetcher...")
        self.config = config
        self.validator = ConfigValidator()
        self.source_fetcher = SourceFetcher(config, self.validator) 
        self.deduplicator = Deduplicator() 
        self.config_processor = ConfigProcessor(config, self.validator) 
        self.connection_tester = ConnectionTester(config, self.validator, self.get_location) 
        self.output_manager = OutputManager(config) 
        self.config_filter = ConfigFilter(config, self.validator) 
        
        self.protocol_counts: Dict[str, int] = defaultdict(int) 
        self.channel_protocol_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int)) 
        
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
            response = self.source_fetcher.session.get(f'http://ip-api.com/json/{ip}', timeout=5)
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
            response = self.source_fetcher.session.get(f'https://ipapi.co/{ip}/json/', timeout=5)
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
            response = self.source_fetcher.session.get(f'https://ipwhois.app/json/{ip}', timeout=5)
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
            response = self.source_fetcher.session.get(f'https://api.ipdata.co/{ip}?api-key=test', timeout=5)
            if response.status_code == 200:
                data = response.json()
                if data.get('country_code') and data.get('country_name'):
                    return data['country_code'].lower(), data['country']
        except Exception as e:
            logger.debug(f"خطا در API ipdata.co برای IP {ip}: {str(e)}")
        return '', ''

    def _get_location_from_abstractapi(self, ip: str) -> Tuple[str, str]:
        """دریافت موقعیت جغرافیایی از ipgeolocation.abstractapi.com (نیاز به کلید API واقعی دارد)"""
        try:
            response = self.source_fetcher.session.get(f'https://ipgeolocation.abstractapi.com/v1/?api_key=test', timeout=5)
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
        with self._lock: # محافظت از دسترسی به self.config.SOURCE_URLS
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
        این متد اکنون واکشی واقعی را به SourceFetcher محول می‌کند.
        """
        return self.source_fetcher.fetch_channel_data(channel)

    def _select_batch_for_testing(self, 
                                  unique_processed_configs_pool: List[Dict[str, str]], 
                                  tested_protocol_counts: Dict[str, int],
                                  batch_size_multiplier: int = 3) -> List[Dict[str, str]]:
        """
        یک دسته از کانفیگ‌ها را برای تست انتخاب می‌کند.
        اولویت با پروتکل‌هایی است که هنوز به تعداد مورد نیاز خود نرسیده‌اند.
        batch_size_multiplier: چند برابر تعداد مورد نیاز در هر پروتکل، برای تست انتخاب شود.
        """
        selected_batch: List[Dict[str, str]] = []
        configs_by_protocol: Dict[str, List[Dict[str, str]]] = defaultdict(list)
        
        # دسته‌بندی کانفیگ‌های باقی‌مانده بر اساس پروتکل
        for cfg in unique_processed_configs_pool:
            configs_by_protocol[cfg['protocol']].append(cfg)

        # مرتب‌سازی پروتکل‌ها بر اساس اولویت و نیاز (پروتکل‌هایی که به تعداد کمتری رسیده‌اند، اولویت بالاتری دارند)
        # و پروتکل‌هایی با اولویت بالاتر (عدد کمتر)
        sorted_protocols_by_priority_and_need = sorted(
            self.config.SUPPORTED_PROTOCOLS.items(),
            key=lambda item: (
                tested_protocol_counts.get(item[0], 0), # کمتر تست شده‌ها اول
                item[1]["priority"] # سپس اولویت بالاتر
            )
        )

        # محاسبه سایز هدف برای کل دسته، حداقل 100 یا (تعداد پروتکل‌های فعال * تعداد کانفیگ مشخص)
        target_total_batch_size = max(self.config.specific_config_count * len([p for p in self.config.SUPPORTED_PROTOCOLS if self.config.SUPPORTED_PROTOCOLS[p]['enabled']]), 100) 
        current_batch_size = 0
        
        for protocol_prefix, protocol_info in sorted_protocols_by_priority_and_need:
            # اگر پروتکل غیرفعال است یا قبلاً به تعداد کافی رسیده است، رد کن
            if not protocol_info["enabled"] or \
               tested_protocol_counts.get(protocol_prefix, 0) >= protocol_info["max_configs"]:
                continue

            needed_for_protocol = protocol_info["max_configs"] - tested_protocol_counts.get(protocol_prefix, 0)
            if needed_for_protocol <= 0: # اگر پروتکل به حد نصاب رسیده باشد
                continue

            available_for_protocol = configs_by_protocol[protocol_prefix]
            
            # تعداد کانفیگی که می‌خواهیم از این پروتکل برای تست انتخاب کنیم
            num_to_select = min(len(available_for_protocol), needed_for_protocol * batch_size_multiplier)
            
            selected_batch.extend(available_for_protocol[:num_to_select])
            
            current_batch_size += num_to_select
            if current_batch_size >= target_total_batch_size and len(selected_batch) > 0:
                break 
        
        selected_ids = {cfg['canonical_id'] for cfg in selected_batch}
        unique_processed_configs_pool[:] = [cfg for cfg in unique_processed_configs_pool if cfg['canonical_id'] not in selected_ids]


        logger.info(f"یک دسته {len(selected_batch)} کانفیگ برای تست انتخاب شد. {len(unique_processed_configs_pool)} کانفیگ تست‌نشده باقی ماند.")
        return selected_batch


    def balance_protocols(self, configs: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """
        کانفیگ‌ها را بر اساس پروتکل سازماندهی و متعادل می‌کند تا توزیع مناسبی داشته باشند.
        این متد تضمین می‌کند که تعداد کانفیگ‌ها برای هر پروتکل از "max_configs" تعیین شده
        در تنظیمات (برای آن پروتکل) تجاوز نکند.
        """
        logger.info("شروع توازن پروتکل‌ها...")
        protocol_configs: Dict[str, List[Dict[str, str]]] = defaultdict(list)
        for config_dict in configs:
            protocol = config_dict['protocol']
            # نرمال‌سازی پروتکل‌های مستعار برای توازن
            if protocol.startswith('hy2://'):
                protocol = 'hysteria2://'
            elif protocol.startswith('hy1://'):
                protocol = 'hysteria://'

            if protocol in self.config.SUPPORTED_PROTOCOLS: # اطمینان از اینکه پروتکل پشتیبانی می‌شود
                protocol_configs[protocol].append(config_dict)
            else:
                logger.warning(f"پروتکل '{protocol}' در لیست پروتکل‌های پشتیبانی شده برای توازن یافت نشد. ممکن است به درستی تعریف نشده باشد.")

        total_configs = sum(len(configs_list) for configs_list in protocol_configs.values())
        if total_configs == 0:
            logger.info("هیچ کانفیگی برای توازن پروتکل وجود ندارد.")
            return []

        balanced_configs: List[Dict[str, str]] = []
        # مرتب‌سازی پروتکل‌ها بر اساس اولویت و سپس تعداد کانفیگ‌های موجود
        sorted_protocols = sorted(
            protocol_configs.items(),
            key=lambda x: (
                self.config.SUPPORTED_PROTOCOLS.get(x[0], {"priority": 999})["priority"], 
                len(x[1])
            ),
            reverse=True # پروتکل‌های با اولویت بالاتر (عدد کمتر) و تعداد بیشتر، زودتر پردازش شوند
        )
        logger.info(f"در حال توازن {total_configs} کانفیگ بر اساس {len(sorted_protocols)} پروتکل مرتب شده...")

        for protocol, protocol_config_list in sorted_protocols:
            protocol_info = self.config.SUPPORTED_PROTOCOLS.get(protocol)
            if not protocol_info:
                logger.warning(f"اطلاعات پیکربندی برای پروتکل '{protocol}' یافت نشد، نادیده گرفته شد.")
                continue

            # اگر تعداد کانفیگ‌های موجود برای پروتکل بیشتر یا مساوی حداقل مورد نیاز باشد
            if len(protocol_config_list) >= protocol_info["min_configs"]:
                # تعداد کانفیگ‌ها برای اضافه کردن را بر اساس حداکثر مجاز یا موجودی واقعی انتخاب می‌کند
                num_to_add = min(
                    protocol_info["max_configs"],  # حداکثر مجاز توسط تنظیمات
                    len(protocol_config_list)     # تعداد واقعی موجود
                )
                balanced_configs.extend(protocol_config_list[:num_to_add])
                logger.info(f"پروتکل '{protocol}': {num_to_add} کانفیگ اضافه شد (از {len(protocol_config_list)} موجود، حداکثر مجاز: {protocol_info['max_configs']}).")
            # اگر حالت flexible_max فعال باشد و حداقل یک کانفیگ موجود باشد، همه را اضافه می‌کند
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

        logger.info("شروع فاز ۳: پردازش و حذف اولیه تکراری‌ها (بر اساس شناسه کانونی) از کل کانفیگ‌های خام به صورت موازی...")
        unique_processed_configs_pool: List[Dict[str, str]] = [] 

        if not all_raw_configs_collected:
            logger.info("هیچ کانفیگ خامی برای پردازش در فاز ۳ یافت نشد. فاز ۳ نادیده گرفته شد.")
            logger.info("فاز ۳ تکمیل شد. مجموعاً 0 کانفیگ منحصر به فرد و غنی شده آماده توازن.")
            return [] 

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor: 
            futures = {executor.submit(self.config_processor.process_raw_config, cfg_str): cfg_str for cfg_str in all_raw_configs_collected}

            processed_raw_count_phase3 = 0
            for future in concurrent.futures.as_completed(futures):
                processed_raw_count_phase3 += 1
                progress_percentage_phase3 = (processed_raw_count_phase3 / len(all_raw_configs_collected)) * 100

                try:
                    processed_cfg_data = future.result() 
                    if processed_cfg_data:
                        is_unique = self.deduplicator.is_unique_and_add(processed_cfg_data['canonical_id'])
                        if is_unique:
                            unique_processed_configs_pool.append(processed_cfg_data)
                    if processed_raw_count_phase3 % 100 == 0 or processed_raw_count_phase3 == len(all_raw_configs_collected):
                        logger.info(f"پیشرفت فاز ۳: {progress_percentage_phase3:.2f}% ({processed_raw_count_phase3}/{len(all_raw_configs_collected)}) کانفیگ خام پردازش شد. (کانفیگ‌های منحصر به فرد اولیه: {len(unique_processed_configs_pool)})")
                except Exception as exc:
                    original_raw_config = futures[future]
                    logger.error(f"خطا در پردازش موازی کانفیگ خام در فاز ۳: '{original_raw_config[:min(len(original_raw_config), 50)]}...': {exc}", exc_info=True)
            
        logger.info(f"فاز ۳ تکمیل شد. مجموعاً {len(unique_processed_configs_pool)} کانفیگ منحصر به فرد (بدون پرچم/کشور) آماده تست.")

        logger.info("شروع فاز ۴: تست تدریجی، فیلترینگ و غنی‌سازی (پرچم/کشور) کانفیگ‌ها به صورت موازی...")
        final_tested_and_enriched_configs: List[Dict[str, str]] = []
        tested_protocol_counts: Dict[str, int] = defaultdict(int) 

        max_attempts_per_batch_loop = 5 
        current_attempt = 0

        while unique_processed_configs_pool and \
              any(tested_protocol_counts.get(p, 0) < self.config.SUPPORTED_PROTOCOLS[p]["max_configs"] 
                  for p in self.config.SUPPORTED_PROTOCOLS if self.config.SUPPORTED_PROTOCOLS[p]["enabled"]) and \
              current_attempt < max_attempts_per_batch_loop: 
            
            batch_to_test = self._select_batch_for_testing(unique_processed_configs_pool, tested_protocol_counts)
            
            if not batch_to_test:
                logger.info("هیچ کانفیگ جدیدی برای تست در دسته فعلی انتخاب نشد. اتمام فاز ۴.")
                current_attempt += 1 
                continue 
            
            current_attempt = 0 

            logger.info(f"شروع تست دسته جدید: {len(batch_to_test)} کانفیگ.")
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor_test:
                futures_test = {executor_test.submit(self.connection_tester.test_and_enrich_config, cfg_data): cfg_data for cfg_data in batch_to_test}

                processed_batch_count = 0
                for future_test in concurrent.futures.as_completed(futures_test):
                    processed_batch_count += 1
                    try:
                        enriched_config_dict = future_test.result()
                        if enriched_config_dict:
                            filtered_result = self.config_filter.filter_configs(
                                [enriched_config_dict], 
                                allowed_countries=ALLOWED_COUNTRIES,
                                blocked_countries=BLOCKED_COUNTRIES,
                                allowed_protocols=ALLOWED_PROTOCOLS,
                                blocked_keywords=BLOCKED_KEYWORDS,
                                blocked_ips=BLOCKED_IPS,
                                blocked_domains=BLOCKED_DOMAINS
                            )
                            if filtered_result: 
                                final_tested_and_enriched_configs.append(filtered_result[0]) 
                                self.protocol_counts[filtered_result[0]['protocol']] += 1
                                tested_protocol_counts[filtered_result[0]['protocol']] += 1
                                logger.debug(f"کانفیگ پروتکل '{filtered_result[0]['protocol']}' با موفقیت به لیست نهایی اضافه شد. تعداد فعلی: {tested_protocol_counts[filtered_result[0]['protocol']]}/{self.config.SUPPORTED_PROTOCOLS[filtered_result[0]['protocol']]['max_configs']}")
                            else:
                                logger.debug(f"کانفیگ '{enriched_config_dict.get('config', '')[:min(len(enriched_config_dict.get('config', '')), 50)]}...' توسط فیلتر رد شد.")

                    except Exception as exc_test:
                        original_cfg_data = futures_test[future_test]
                        logger.error(f"خطا در تست موازی کانفیگ: '{original_cfg_data.get('config', '')[:min(len(original_cfg_data.get('config', '')), 50)]}...': {exc_test}", exc_info=True)
            
            logger.info(f"دسته {len(batch_to_test)} کانفیگ تست شد. مجموع کانفیگ‌های فعال تا کنون: {len(final_tested_and_enriched_configs)}.")

        logger.info(f"فاز ۴ تکمیل شد. مجموعاً {len(final_tested_and_enriched_configs)} کانفیگ تست شده و غنی شده آماده توازن.")

        total_valid_configs_global = len(final_tested_and_enriched_configs)
        for channel in self.config.SOURCE_URLS:
            channel.metrics.valid_configs = 0 
            channel.metrics.unique_configs = 0 
        
        for protocol, count in self.protocol_counts.items():
            if protocol in self.config.SUPPORTED_PROTOCOLS:
                self.config.SUPPORTED_PROTOCOLS[protocol]["actual_count"] = count 
        
        logger.info("شروع فاز ۵: توازن نهایی پروتکل‌ها...")
        final_configs_balanced = self.balance_protocols(final_tested_and_enriched_configs)
        logger.info(f"فاز ۵ تکمیل شد. {len(final_configs_balanced)} کانفیگ نهایی پس از توازن آماده ذخیره.")

        return final_configs_balanced

