import re
import os
import time
import json
import logging 
import requests
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Tuple, Any 
from bs4 import BeautifulSoup
import base64 

# وابستگی‌های مورد نیاز برای SourceFetcher
from config import ProxyConfig, ChannelConfig 
from config_validator import ConfigValidator 

logger = logging.getLogger(__name__)

class SourceFetcher:
    """
    کلاس SourceFetcher مسئول واکشی محتوای خام از منابع مختلف (HTTP، تلگرام، ssconf) است.
    همچنین مدیریت تلاش مجدد، دیکد کردن Base64، و فیلتر کردن محتوای قدیمی را انجام می‌دهد.
    """
    def __init__(self, config: ProxyConfig, validator: ConfigValidator):
        """
        سازنده SourceFetcher.
        config: یک نمونه از ProxyConfig حاوی تنظیمات.
        validator: یک نمونه از ConfigValidator برای توابع اعتبارسنجی.
        """
        self.config = config
        self.validator = validator
        self.session = requests.Session()
        self.session.headers.update(config.HEADERS)
        logger.info("SourceFetcher با موفقیت مقداردهی اولیه شد.")

    def fetch_with_retry(self, url: str) -> Optional[requests.Response]:
        """
        واکشی URL با قابلیت تلاش مجدد و تأخیر افزایشی.
        در صورت موفقیت، شیء Response را برمی‌گرداند؛ در غیر این صورت None.
        """
        backoff = 1
        for attempt in range(self.config.MAX_RETRIES):
            try:
                logger.debug(f"در حال تلاش برای واکشی '{url}' (تلاش {attempt + 1}/{self.config.MAX_RETRIES})")
                response = self.session.get(url, timeout=self.config.REQUEST_TIMEOUT)
                response.raise_for_status()  # خطا را برای کدهای وضعیت HTTP ناموفق بالا می‌اندازد
                return response
            except requests.RequestException as e:
                if attempt == self.config.MAX_RETRIES - 1:
                    logger.error(f"واکشی '{url}' پس از {self.config.MAX_RETRIES} تلاش ناموفق بود: {str(e)}")
                    return None
                # محاسبه زمان انتظار با تأخیر تصاعدی (exponential backoff)
                wait_time = min(self.config.RETRY_DELAY * backoff, 60) # حداکثر 60 ثانیه
                logger.warning(f"تلاش {attempt + 1} برای '{url}' ناموفق بود. تلاش مجدد در {wait_time} ثانیه: {str(e)}")
                time.sleep(wait_time)
                backoff *= 2  # افزایش فاکتور backoff

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
            # بررسی می‌کند که آیا محتوا Base64 است و دیکدش می‌کند
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
        فقط اگر محتوای دیکد شده شامل پروتکل‌های پشتیبانی شده باشد، آن را برمی‌گرداند.
        """
        try:
            decoded_text = self.validator.decode_base64_text(text)
            if decoded_text:
                # بررسی می‌کند که آیا متن دیکد شده شامل هر یک از پروتکل‌های پشتیبانی شده است
                # برای این کار از لیست پروتکل‌های اصلی در config استفاده می‌کنیم
                if any(p in decoded_text for p in self.config.SUPPORTED_PROTOCOLS):
                    logger.debug(f"متن با موفقیت به Base64 دیکد شد و شامل پروتکل‌های شناخته شده است.")
                    return decoded_text
            logger.debug(f"متن Base64 نیست یا شامل پروتکل‌های شناخته شده نیست.")
            return None
        except Exception as e:
            logger.debug(f"خطا در دیکد کردن Base64: {str(e)}")
            return None

    def extract_date_from_message(self, message) -> Optional[datetime]:
        """
        تاریخ و زمان انتشار پیام را از عنصر <time> در HTML پیام تلگرام استخراج می‌کند.
        """
        try:
            # پیدا کردن عنصر <time> در والد پیام
            time_element = message.find_parent('div', class_='tgme_widget_message').find('time')
            if time_element and 'datetime' in time_element.attrs:
                # تبدیل رشته datetime به شیء datetime با در نظر گرفتن منطقه زمانی
                return datetime.fromisoformat(time_element['datetime'].replace('Z', '+00:00'))
        except Exception as e:
            logger.debug(f"خطا در استخراج تاریخ از پیام: {str(e)}")
            pass # ادامه دادن در صورت بروز خطا
        return None

    def is_config_valid_by_date(self, config_text: str, date: Optional[datetime]) -> bool:
        """
        بررسی می‌کند که آیا تاریخ کانفیگ به اندازه کافی جدید است (طبق MAX_CONFIG_AGE_DAYS).
        """
        if not date:
            # اگر تاریخ موجود نیست، فرض می‌کنیم که معتبر است
            logger.debug("تاریخ کانفیگ موجود نیست، معتبر فرض می‌شود.")
            return True
        # تاریخ برش (cutoff) را بر اساس حداکثر عمر کانفیگ‌ها محاسبه می‌کند
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=self.config.MAX_CONFIG_AGE_DAYS)
        if date >= cutoff_date:
            return True
        else:
            logger.debug(f"کانفیگ به دلیل قدیمی بودن تاریخ (تاریخ: {date}) نادیده گرفته شد.")
            return False

    def fetch_channel_data(self, channel: ChannelConfig) -> Tuple[List[str], List[str], Dict[str, Any]]:
        """
        واکشی داده‌های خام (رشته‌های کانفیگ) و لینک‌های کانال جدید از یک کانال مشخص.
        این متد برای اجرای موازی طراحی شده است.
        """
        raw_configs_from_channel: List[str] = []
        new_channel_urls_from_channel: List[str] = []

        channel_status_info: Dict[str, Any] = {
            'url': channel.url,
            'success': False,
            'response_time': 0,
            'valid_configs_count': 0, # تعداد کانفیگ‌های معتبر (بعد از پاکسازی) از این کانال
            'total_configs_raw': 0,  # تعداد کل کانفیگ‌های خام پیدا شده
            'error_message': None 
        }

        start_time = time.time() # شروع زمان‌سنجی واکشی

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
                    # استفاده از BeautifulSoup برای تجزیه HTML پیام‌های تلگرام
                    soup = BeautifulSoup(response.text, 'html.parser')
                    messages = soup.find_all('div', class_='tgme_widget_message_text')

                    # مرتب‌سازی پیام‌ها بر اساس تاریخ به صورت نزولی (جدیدترین اول)
                    sorted_messages = sorted(
                        messages,
                        key=lambda message: self.extract_date_from_message(message) or datetime.min.replace(tzinfo=timezone.utc),
                        reverse=True
                    )

                    for message_div in sorted_messages:
                        if not message_div or not message_div.text:
                            continue

                        message_date = self.extract_date_from_message(message_div)
                        # فیلتر کردن پیام‌ها بر اساس تاریخ (قدیمی بودن)
                        if not self.is_config_valid_by_date(message_div.text, message_date):
                            continue

                        # استخراج لینک‌های کانال تلگرام از پیام
                        links_and_mentions = message_div.find_all('a', href=True)
                        for item in links_and_mentions:
                            href_url = item['href']
                            match_s = re.match(r'https?://t\.me/s/([a-zA-Z0-9_]+)', href_url)
                            match_direct = re.match(r'https?://t\.me/([a-zA-Z0-9_]+)', href_url)

                            if match_s:
                                new_channel_urls_from_channel.append(f"https://t.me/s/{match_s.group(1)}")
                            elif match_direct:
                                # اطمینان از اینکه لینک‌های مستقیم نیز به فرمت t.me/s/ تبدیل شوند.
                                new_channel_urls_from_channel.append(f"https://t.me/s/{match_direct.group(1)}")

                            # استخراج کانفیگ‌ها از خود لینک‌ها (ممکن است لینک‌ها حاوی کانفیگ باشند)
                            raw_configs_from_channel.extend(self.validator.split_configs(href_url))

                        # استخراج کانفیگ‌ها از محتوای متنی پیام
                        text_content = message_div.text
                        decoded_from_base64 = self.check_and_decode_base64(text_content)
                        if decoded_from_base64:
                            raw_configs_from_channel.extend(self.validator.split_configs(decoded_from_base64))
                        else:
                            raw_configs_from_channel.extend(self.validator.split_configs(text_content))

                else: # برای کانال‌های غیرتلگرام (فقط محتوای متنی)
                    text_content = response.text
                    decoded_from_base64 = self.check_and_decode_base64(text_content)
                    if decoded_from_base64:
                        raw_configs_from_channel.extend(self.validator.split_configs(decoded_from_base64))
                    else:
                        raw_configs_from_channel.extend(self.validator.split_configs(text_content))

            channel_status_info['success'] = True
        except Exception as e:
            channel_status_info['success'] = False
            channel_status_info['error_message'] = str(e)
            logger.error(f"خطا در fetch_channel_data برای '{channel.url}': {str(e)}", exc_info=True)

        channel_status_info['response_time'] = time.time() - start_time
        channel_status_info['total_configs_raw'] = len(raw_configs_from_channel)

        return raw_configs_from_channel, new_channel_urls_from_channel, channel_status_info

