import logging
from datetime import datetime, timezone

# وارد کردن کلاس‌های اصلی پروژه
from config import ProxyConfig
from config_fetcher import ConfigFetcher # ConfigFetcher اکنون Orchestrator اصلی است

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

def main():
    """
    تابع اصلی برای اجرای فرآیند واکشی، پردازش و ذخیره کانفیگ‌های پروکسی.
    """
    logger.info("شروع فرآیند واکشی و پردازش کانفیگ‌ها...")
    try:
        # 1. مقداردهی اولیه پیکربندی‌ها
        config = ProxyConfig() 
        
        # 2. مقداردهی اولیه ConfigFetcher (که سایر ماژول‌ها را در خود دارد)
        fetcher = ConfigFetcher(config) 

        # 3. اجرای Pipeline اصلی واکشی و پردازش
        final_configs = fetcher.run_full_pipeline() 

        # 4. ذخیره نتایج و تولید گزارش‌ها
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
        # در صورت بروز خطای بحرانی، پیام لاگ می‌شود. (بدون اطلاع‌رسانی تلگرام)

if __name__ == '__main__':
    main()

