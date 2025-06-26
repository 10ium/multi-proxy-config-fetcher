import logging
import threading
from typing import Set, Dict

logger = logging.getLogger(__name__)

class Deduplicator:
    """
    کلاس Deduplicator مسئول مدیریت شناسه‌های کانونی (canonical IDs) کانفیگ‌ها
    برای حذف تکراری‌ها در فرآیند واکشی.
    """
    def __init__(self):
        """
        سازنده Deduplicator.
        seen_configs: مجموعه‌ای برای نگهداری شناسه‌های کانونی کانفیگ‌های دیده شده.
        _lock: قفل برای اطمینان از ایمنی thread در هنگام دسترسی به seen_configs.
        """
        self.seen_configs: Set[str] = set()
        self._lock = threading.Lock() # برای دسترسی ایمن به seen_configs در محیط موازی
        logger.info("Deduplicator با موفقیت مقداردهی اولیه شد.")

    def is_unique_and_add(self, canonical_id: str) -> bool:
        """
        بررسی می‌کند که آیا یک شناسه کانونی قبلاً دیده شده است یا خیر.
        اگر دیده نشده باشد، آن را به مجموعه اضافه کرده و True را برمی‌گرداند.
        اگر قبلاً دیده شده باشد، False را برمی‌گرداند.
        """
        with self._lock: # استفاده از قفل برای جلوگیری از شرایط مسابقه (race conditions)
            if canonical_id not in self.seen_configs:
                self.seen_configs.add(canonical_id)
                logger.debug(f"شناسه کانونی جدید به مجموعه اضافه شد: {canonical_id[:min(len(canonical_id), 50)]}...")
                return True
            else:
                logger.debug(f"شناسه کانونی تکراری شناسایی شد: {canonical_id[:min(len(canonical_id), 50)]}...")
                return False

    def get_total_unique_count(self) -> int:
        """
        تعداد کل کانفیگ‌های منحصر به فردی که تا کنون دیده شده‌اند را برمی‌گرداند.
        """
        with self._lock: # دسترسی ایمن به تعداد
            return len(self.seen_configs)

