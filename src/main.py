import sys
import os
import logging
from datetime import datetime, timezone

# **تغییر یافته**: اضافه کردن مسیر دایرکتوری src به sys.path
# این کار تضمین می کند که پایتون می تواند ماژول ها را مستقیماً از داخل src/ پیدا کند.
script_dir = os.path.dirname(__file__) # مسیر فعلی فایل main.py (یعنی /home/runner/work/.../src)
if script_dir not in sys.path:
    sys.path.insert(0, script_dir) # اضافه کردن دایرکتوری src/ به sys.path

# وارد کردن کلاس‌های اصلی پروژه **بدون پیشوند src.**.
# اکنون که src/ در sys.path است، پایتون باید بتواند آنها را پیدا کند.
from config import ProxyConfig
from config_fetcher import ConfigFetcher 

# پیکربندی لاگ‌گیری (این پیکربندی در تمام ماژول‌ها اعمال می‌شود)
logging.basicConfig(
    level=logging.INFO, # <--- می‌توانید برای جزئیات بیشتر به logging.DEBUG تغییر دهید
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('proxy_fetcher.log', encoding='utf-8'), # ذخیره لاگ‌ها در فایل
        logging.StreamHandler() # نمایش لاگ‌ها در کنسول
    ]
)
logger = logging.getLogger(__name__)

print("--- DEBUG: main.py started ---") # برای عیب‌یابی

def main():
    """
    تابع اصلی برای اجرای فرآیند واکشی، پردازش و ذخیره کانفیگ‌های پروکسی.
    """
    logger.info("شروع فرآیند واکشی و پردازش کانفیگ‌ها...")
    try:
        config = ProxyConfig() 
        fetcher = ConfigFetcher(config) 
        final_configs = fetcher.run_full_pipeline() 

        if final_configs:
            fetcher.output_manager.save_configs(final_configs)
            logger.info(f"فرآیند با موفقیت در {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')} به پایان رسید. مجموعاً {len(final_configs)} کانفیگ فعال و تست شده پردازش شد.")
            logger.info("تعداد کانفیگ‌های نهایی بر اساس پروتکل:")
            for protocol, count in fetcher.protocol_counts.items():
                logger.info(f"  {protocol}: {count} کانفیگ")
        else:
            logger.error("هیچ کانفیگ فعال و معتبری یافت نشد و هیچ فایلی تولید نشد!")

        fetcher.output_manager.save_channel_stats(fetcher.config.SOURCE_URLS, fetcher.deduplicator.get_total_unique_count())
        logger.info("آمار کانال‌ها ذخیره شد.")
        fetcher.output_manager.generate_overall_report(fetcher.config.SOURCE_URLS, fetcher.protocol_counts)
        logger.info("گزارش وضعیت کانال‌ها تولید شد.")

    except Exception as e:
        logger.critical(f"خطای بحرانی در اجرای اصلی: {str(e)}", exc_info=True)

if __name__ == '__main__':
    main()

