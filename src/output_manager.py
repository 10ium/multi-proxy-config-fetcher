import os
import json
import logging
import base64
from datetime import datetime, timezone
from typing import List, Dict, Any, TYPE_CHECKING
from collections import defaultdict # برای استفاده از defaultdict در نمودار پروتکل‌ها

# برای جلوگیری از خطای import دایره‌ای (circular import)، از TYPE_CHECKING استفاده می‌کنیم
if TYPE_CHECKING:
    from config import ProxyConfig, ChannelConfig

# وارد کردن Matplotlib برای تولید نمودارها
import matplotlib.pyplot as plt
import numpy as np

logger = logging.getLogger(__name__)

class OutputManager:
    """
    کلاس OutputManager مسئول تمامی عملیات ذخیره‌سازی فایل‌ها (کانفیگ‌ها، آمار)
    و تولید گزارش‌های بصری (مانند نمودارها و گزارش HTML).
    """
    def __init__(self, config: 'ProxyConfig'):
        """
        سازنده OutputManager.
        config: یک نمونه از ProxyConfig حاوی تنظیمات مسیرهای خروجی.
        """
        self.config = config
        logger.info("OutputManager با موفقیت مقداردهی اولیه شد.")

    def _save_base64_file(self, file_path: str, content: str):
        """یک محتوا را Base64 می‌کند و در یک فایل ذخیره می‌کند."""
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(base64.b64encode(content.encode('utf-8')).decode('utf-8'))
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

        # هدر استاندارد برای فایل‌های سابسکریپشن
        header = """//profile-title: base64:8J+RvUFub255bW91cy3wnZWP
//profile-update-interval: 1
//subscription-userinfo: upload=0; download=0; total=10737418240000000; expire=2546249531
//support-url: https://t.me/BXAMbot
//profile-web-page-url: https://github.com/4n0nymou3

"""
        # ساخت محتوای فایل اصلی کانفیگ‌های متنی
        full_text_lines = []
        for cfg_dict in configs:
            full_text_lines.append(f"{cfg_dict['flag']} {cfg_dict['country']} {cfg_dict['config']}")
        full_text_content = header + '\n\n'.join(full_text_lines) + '\n'

        # ذخیره فایل اصلی متنی
        full_file_path = os.path.join(self.config.TEXT_OUTPUT_DIR, 'proxy_configs.txt')
        try:
            with open(full_file_path, 'w', encoding='utf-8') as f:
                f.write(full_text_content)
            logger.info(f"با موفقیت {len(configs)} کانفیگ نهایی در '{full_file_path}' ذخیره شد.")
        except Exception as e:
            logger.error(f"خطا در ذخیره فایل کامل کانفیگ: {str(e)}")

        # ذخیره فایل اصلی Base64 شده
        base64_full_file_path = os.path.join(self.config.BASE64_OUTPUT_DIR, "proxy_configs_base64.txt")
        self._save_base64_file(base64_full_file_path, full_text_content)

        # تفکیک و ذخیره کانفیگ‌ها بر اساس پروتکل
        # از defaultdict استفاده می‌کنیم تا نیازی به مقداردهی اولیه برای همه پروتکل‌ها نباشد.
        protocol_configs_separated: Dict[str, List[Dict[str, str]]] = defaultdict(list)
        for cfg_dict in configs:
            protocol_full_name = cfg_dict['protocol']
            # نرمال‌سازی پروتکل‌های مستعار برای تفکیک فایل‌ها
            if protocol_full_name.startswith('hy2://'):
                protocol_full_name = 'hysteria2://'
            elif protocol_full_name.startswith('hy1://'):
                protocol_full_name = 'hysteria://'
            
            # فقط پروتکل‌های پشتیبانی شده را در نظر می‌گیریم
            if protocol_full_name in self.config.SUPPORTED_PROTOCOLS:
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
            protocol_file_path = os.path.join(self.config.TEXT_OUTPUT_DIR, protocol_file_name)
            try:
                with open(protocol_file_path, 'w', encoding='utf-8') as f:
                    f.write(protocol_text_content)
                logger.info(f"با موفقیت {len(cfg_list_of_dicts)} کانفیگ '{protocol_name}' در '{protocol_file_path}' ذخیره شد.")
            except Exception as e:
                logger.error(f"خطا در ذخیره فایل '{protocol_name}' کانفیگ: {str(e)}")

            base64_protocol_file_name = f"{protocol_name}_base64.txt"
            base64_protocol_file_path = os.path.join(self.config.BASE64_OUTPUT_DIR, base64_protocol_file_name)
            self._save_base64_file(base64_protocol_file_path, protocol_text_content)

    def save_channel_stats(self, source_channels: List['ChannelConfig'], total_unique_configs_overall: int):
        """
        آمار مربوط به کانال‌ها و آمارهای کلی را در یک فایل JSON ذخیره می‌کند.
        source_channels: لیستی از ChannelConfigها.
        total_unique_configs_overall: تعداد کل کانفیگ‌های منحصر به فرد (از Deduplicator).
        """
        stats_data = {
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "total_channels": len(source_channels),
            "enabled_channels": sum(1 for c in source_channels if c.enabled),
            "total_unique_configs_found_overall": total_unique_configs_overall, 
            "channels": []
        }
        for channel in source_channels:
            stats_data["channels"].append({
                "url": channel.url,
                "enabled": channel.enabled,
                "total_configs_fetched_raw": channel.metrics.total_configs,
                # valid_configs_processed و unique_configs_found در حال حاضر به طور دقیق برای هر کانال بروز نمی‌شوند.
                # اینها باید در فاز تست و فیلترینگ با ردیابی دقیق منبع کانفیگ بروز شوند.
                "valid_configs_processed": channel.metrics.valid_configs, 
                "unique_configs_found": channel.metrics.unique_configs, 
                "avg_response_time": round(channel.metrics.avg_response_time, 2),
                "last_success_time": channel.metrics.last_success_time.isoformat() if channel.metrics.last_success_time else None,
                "fail_count": channel.metrics.fail_count,
                "success_count": channel.metrics.success_count,
                "overall_score": channel.metrics.overall_score,
                "error_count_consecutive": channel.error_count,
                "retry_level": channel.retry_level,
                "next_check_time": channel.next_check_time.isoformat() if channel.next_check_time else None
            })

        stats_file_path = os.path.join(self.config.OUTPUT_DIR, 'channel_stats.json')
        try:
            os.makedirs(os.path.dirname(stats_file_path), exist_ok=True) # اطمینان از وجود دایرکتوری
            with open(stats_file_path, 'w', encoding='utf-8') as f:
                json.dump(stats_data, f, indent=4, ensure_ascii=False)
            logger.info(f"آمار کانال‌ها با موفقیت در '{stats_file_path}' ذخیره شد.")
        except Exception as e:
            logger.error(f"خطا در ذخیره آمار کانال‌ها: {str(e)}")

    def _generate_protocol_distribution_chart(self, protocol_counts: Dict[str, int]) -> str:
        """
        نمودار توزیع پروتکل‌ها (تعداد کانفیگ‌های فعال هر پروتکل) را تولید و ذخیره می‌کند.
        protocol_counts: دیکشنری با نام پروتکل و تعداد کانفیگ‌های فعال آن.
        خروجی: مسیر فایل نمودار SVG.
        """
        chart_path = os.path.join("assets", "protocol_distribution_chart.svg")
        os.makedirs(os.path.dirname(chart_path), exist_ok=True)

        protocols_present = [p for p in protocol_counts.keys() if protocol_counts[p] > 0]
        counts_present = [protocol_counts[p] for p in protocols_present]

        if not protocols_present or sum(counts_present) == 0:
            logger.warning("داده‌ای برای تولید نمودار توزیع پروتکل‌ها وجود ندارد.")
            # ایجاد یک نمودار خالی یا پیام‌دهنده
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.text(0.5, 0.5, "هیچ داده‌ای برای نمودار پروتکل یافت نشد.", 
                    horizontalalignment='center', verticalalignment='center', 
                    transform=ax.transAxes, fontsize=14, color='gray')
            ax.set_xticks([])
            ax.set_yticks([])
            ax.set_title("توزیع کانفیگ‌های پروتکل")
            plt.savefig(chart_path, format='svg', bbox_inches='tight')
            plt.close(fig)
            return chart_path


        # مرتب‌سازی برای نمایش بهتر در نمودار (پروتکل‌های با تعداد بیشتر اول)
        sorted_data = sorted(zip(protocols_present, counts_present), key=lambda item: item[1], reverse=True)
        # حذف '://' از نام پروتکل برای نمایش تمیزتر در نمودار
        sorted_protocols = [item[0].replace('://', '') for item in sorted_data] 
        sorted_counts = [item[1] for item in sorted_data]

        fig, ax = plt.subplots(figsize=(12, 7))
        bars = ax.bar(sorted_protocols, sorted_counts, color='skyblue')

        ax.set_xlabel("پروتکل", fontsize=12)
        ax.set_ylabel("تعداد کانفیگ‌های فعال", fontsize=12)
        ax.set_title("توزیع کانفیگ‌های فعال بر اساس پروتکل", fontsize=14)
        ax.tick_params(axis='x', rotation=45) # چرخش لیبل‌های X برای خوانایی بهتر

        # افزودن مقدار بالای هر ستون
        for bar in bars:
            yval = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2, yval + 0.5, yval, ha='center', va='bottom', fontsize=10)

        plt.tight_layout() # تنظیم layout برای جلوگیری از همپوشانی
        plt.savefig(chart_path, format='svg', bbox_inches='tight')
        plt.close(fig) # بستن شکل برای آزادسازی حافظه
        logger.info(f"نمودار توزیع پروتکل‌ها با موفقیت در '{chart_path}' ذخیره شد.")
        return chart_path

    def generate_overall_report(self, source_channels: List['ChannelConfig'], protocol_counts: Dict[str, int]):
        """
        یک گزارش وضعیت کلی را به فرمت HTML تولید می‌کند که شامل جدول کانال‌ها و نمودارها است.
        source_channels: لیستی از ChannelConfigها برای جدول.
        protocol_counts: دیکشنری با تعداد نهایی کانفیگ‌های فعال هر پروتکل.
        """
        report_path = os.path.join("assets", "performance_report.html")
        os.makedirs(os.path.dirname(report_path), exist_ok=True)

        # تولید نمودارها
        protocol_chart_path = self._generate_protocol_distribution_chart(protocol_counts)
        # می‌توانید نمودارهای دیگری را نیز اینجا اضافه کنید (مثلاً روند زمان پاسخ کانال‌ها)

        # داده‌های جدول کانال‌ها
        channels_data = sorted(source_channels, key=lambda c: c.metrics.overall_score, reverse=True)

        html_content = f"""
<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>گزارش عملکرد کلی پروکسی</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;700&display=swap" rel="stylesheet">
    <style>
        body {{ 
            font-family: 'Inter', Arial, sans-serif; 
            margin: 0; padding: 20px; 
            background-color: #f0f2f5; 
            color: #333; 
            direction: rtl; /* RTL */
            text-align: right; /* RTL */
        }}
        .container {{ 
            max-width: 1000px; 
            margin: auto; 
            background: #fff; 
            padding: 25px; 
            border-radius: 12px; 
            box-shadow: 0 4px 15px rgba(0,0,0,0.1); 
        }}
        h1, h2 {{ 
            color: #2c3e50; 
            text-align: center; 
            margin-bottom: 25px; 
            font-weight: bold;
        }}
        .last-updated {{ 
            text-align: center; 
            margin-bottom: 25px; 
            font-style: italic; 
            color: #7f8c8d; 
            font-size: 0.9em; 
        }}
        .chart-container {{ 
            width: 100%; 
            max-width: 800px; 
            margin: 30px auto; 
            background: #fdfdfd;
            border-radius: 10px;
            padding: 15px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.08);
            text-align: center;
        }}
        .chart-container img {{ max-width: 100%; height: auto; }}
        table {{ 
            width: 100%; 
            border-collapse: collapse; 
            margin-top: 30px; 
            box-shadow: 0 2px 8px rgba(0,0,0,0.05); 
            border-radius: 8px; 
            overflow: hidden; /* For rounded corners */
        }}
        th, td {{ 
            border: 1px solid #e0e0e0; 
            padding: 12px 15px; 
            text-align: right; /* RTL */
        }}
        th {{ 
            background-color: #34495e; 
            color: white; 
            font-weight: bold; 
            position: sticky; top: 0; 
            z-index: 1; 
        }}
        tr:nth-child(even) {{ background-color: #f9f9f9; }}
        tr:hover {{ background-color: #f1f1f1; }}
        .score-good {{ color: #27ae60; font-weight: bold; }}
        .score-medium {{ color: #e67e22; font-weight: bold; }}
        .score-bad {{ color: #c0392b; font-weight: bold; }}
        .status-enabled {{ color: #27ae60; font-weight: bold; }}
        .status-disabled {{ color: #c0392b; font-weight: bold; }}
        a {{ color: #3498db; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>گزارش عملکرد کلی پروکسی</h1>
        <p class="last-updated">آخرین به‌روزرسانی: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        
        <h2>توزیع کانفیگ‌های فعال بر اساس پروتکل</h2>
        <div class="chart-container">
            <img src="{os.path.basename(protocol_chart_path)}?v={int(datetime.now().timestamp())}" alt="Protocol Distribution Chart">
        </div>

        <h2>وضعیت کانال‌های منبع</h2>
        <table>
            <thead>
                <tr>
                    <th>URL کانال</th>
                    <th>وضعیت</th>
                    <th>امتیاز کلی</th>
                    <th>واکشی موفق</th>
                    <th>واکشی ناموفق</th>
                    <th>میانگین زمان پاسخ</th>
                    <th>کانفیگ خام</th>
                    <th>تلاش مجدد</th>
                    <th>بررسی بعدی</th>
                </tr>
            </thead>
            <tbody>
        """

        for channel in channels_data:
            status_class = "status-enabled" if channel.enabled else "status-disabled"
            score_class = "score-good" if channel.metrics.overall_score >= 70 else \
                          ("score-medium" if channel.metrics.overall_score >= 40 else "score-bad")
            
            last_success = channel.metrics.last_success_time.strftime('%Y-%m-%d %H:%M') if channel.metrics.last_success_time else "N/A"
            next_check = channel.next_check_time.strftime('%Y-%m-%d %H:%M') if channel.next_check_time else "N/A"

            html_content += f"""
                <tr>
                    <td><a href="{channel.url}" target="_blank">{channel.url}</a></td>
                    <td class="{status_class}">{"فعال" if channel.enabled else "غیرفعال"}</td>
                    <td class="{score_class}">{channel.metrics.overall_score:.2f}</td>
                    <td>{channel.metrics.success_count}</td>
                    <td>{channel.metrics.fail_count}</td>
                    <td>{channel.metrics.avg_response_time:.2f}s</td>
                    <td>{channel.metrics.total_configs}</td>
                    <td>سطح {channel.retry_level} ({channel.error_count} متوالی)</td>
                    <td>{next_check}</td>
                </tr>
            """
        html_content += """
            </tbody>
        </table>
    </div>
</body>
</html>
        """
        try:
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            logger.info(f"گزارش عملکرد کلی با موفقیت در '{report_path}' ذخیره شد.")
        except Exception as e:
            logger.error(f"خطا در تولید گزارش عملکرد کلی: {str(e)}")

