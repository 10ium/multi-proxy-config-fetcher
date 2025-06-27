import logging
import socket
import subprocess
import tempfile
import os
import json
import time
from typing import Dict, Optional, Tuple, Callable, List, Any
from urllib.parse import urlparse

# وارد کردن کلاس‌ها با مسیر پکیج 'src'
from src.config import ProxyConfig 
from src.config_validator import ConfigValidator 
import requests 

logger = logging.getLogger(__name__)

class ConnectionTester:
    """
    کلاس ConnectionTester مسئول تست اتصال کانفیگ‌ها به صورت دو مرحله‌ای:
    ۱. تست پورت سبک و سریع
    ۲. تست عملکردی با Mihomo (برای پروتکل‌های پشتیبانی شده)
    همچنین موقعیت جغرافیایی (پرچم و کشور) را به کانفیگ‌های فعال اضافه می‌کند.
    """
    def __init__(self, config: ProxyConfig, validator: ConfigValidator, get_location_func: Callable[[str], Tuple[str, str]]):
        """
        سازنده ConnectionTester.
        config: یک نمونه از ProxyConfig.
        validator: یک نمونه از ConfigValidator.
        get_location_func: تابعی که آدرس سرور را می‌گیرد و پرچم و کشور (str, str) را برمی‌گرداند.
                           این تابع از ConfigFetcher به عنوان GeoIPResolver تزریق می‌شود.
        """
        self.config = config
        self.validator = validator
        self.get_location = get_location_func 
        
        self.mihomo_testable_protocols = {
            "vmess://", "vless://", "trojan://", "ss://", "hysteria2://", "hysteria://", "tuic://", "juicity://",
        }
        logger.info("ConnectionTester با موفقیت مقداردهی اولیه شد.")

    def _ping_test(self, address: str, port: int, timeout: int = 5) -> bool:
        """
        تست پینگ سبک و سریع با بررسی باز بودن پورت TCP.
        از سوکت برای تلاش برای اتصال به IP:Port استفاده می‌کند.
        """
        if not address or not port:
            logger.debug(f"آدرس یا پورت برای تست پینگ نامعتبر است: {address}:{port}")
            return False
        try:
            ip = socket.gethostbyname(address)
            with socket.create_connection((ip, port), timeout=timeout) as sock:
                logger.debug(f"تست پینگ موفق برای {address}:{port} (IP: {ip}).")
                return True
        except (socket.gaierror, socket.timeout, ConnectionRefusedError, OSError) as e:
            logger.debug(f"تست پینگ ناموفق برای {address}:{port}: {e}")
            return False
        except Exception as e:
            logger.error(f"خطای ناشناخته در تست پینگ برای {address}:{port}: {e}")
            return False

    def _wait_for_port(self, host: str, port: int, timeout: int = 10, interval: float = 0.5) -> bool:
        """
        منتظر می‌ماند تا یک پورت خاص روی یک هاست باز شود.
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                with socket.create_connection((host, port), timeout=1) as s:
                    return True
            except (socket.timeout, ConnectionRefusedError, OSError):
                time.sleep(interval)
            except Exception as e:
                logger.debug(f"خطا در انتظار برای پورت {host}:{port}: {e}")
                return False
        logger.debug(f"پورت {host}:{port} پس از {timeout} ثانیه باز نشد.")
        return False

    def _test_with_mihomo(self, config_string: str) -> bool:
        """
        تست عملکردی یک کانفیگ با استفاده از Mihomo CLI.
        کانفیگ موقت ایجاد کرده، Mihomo را اجرا و لاگ‌ها را برای موفقیت بررسی می‌کند.
        """
        logger.debug(f"شروع تست Mihomo برای کانفیگ: {config_string[:min(len(config_string), 50)]}...")
        
        mihomo_config_content = {
            "port": 7890, 
            "log-level": "info", 
            "mode": "rule",
            "proxies": [
                {
                    "name": "test-proxy",
                    "type": "external",
                    "url": config_string
                }
            ],
            "proxy-groups": [
                {
                    "name": "proxy",
                    "type": "select",
                    "proxies": ["test-proxy"]
                }
            ],
            "rules": [
                "MATCH,proxy" 
            ]
        }

        temp_config_file_path = None
        process = None 
        try:
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.yaml', encoding='utf-8') as temp_config_file:
                json.dump(mihomo_config_content, temp_config_file, indent=2) 
                temp_config_file_path = temp_config_file.name
            
            mihomo_command = [
                "mihomo", 
                "-f", temp_config_file_path, 
                "-d", tempfile.gettempdir(), 
            ]

            process = subprocess.Popen(mihomo_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, preexec_fn=os.setsid)
            
            if not self._wait_for_port("127.0.0.1", 7890, timeout=10): 
                logger.warning(f"پورت Mihomo باز نشد. تست Mihomo ناموفق برای کانفیگ: {config_string[:min(len(config_string), 50)]}...")
                return False

            test_proxy_url = "http://127.0.0.1:7890" 
            test_target_url = "http://cp.cloudflare.com/"
            
            try:
                test_response = requests.get(test_target_url, proxies={"http": test_proxy_url, "https": test_proxy_url}, timeout=self.config.REQUEST_TIMEOUT / 2)
                
                if test_response.status_code == 200:
                    logger.debug(f"تست Mihomo موفق برای کانفیگ: {config_string[:min(len(config_string), 50)]}...")
                    return True
                else:
                    logger.debug(f"تست Mihomo ناموفق برای کانفیگ (کد وضعیت: {test_response.status_code}): {config_string[:min(len(config_string), 50)]}...")
                    return False
            except requests.exceptions.RequestException as e:
                logger.debug(f"خطا در درخواست از طریق Mihomo برای کانفیگ '{config_string[:min(len(config_string), 50)]}...': {e}")
                return False
        except FileNotFoundError:
            logger.error("اجرایی Mihomo یافت نشد. لطفاً مطمئن شوید که Mihomo در PATH سیستم شما نصب و قابل دسترس است.")
            return False
        except Exception as e:
            logger.error(f"خطا در اجرای تست Mihomo برای کانفیگ '{config_string[:min(len(config_string), 50)]}...': {e}", exc_info=True)
            return False
        finally:
            if process:
                stdout, stderr = process.communicate(timeout=5) 
                if stdout:
                    logger.debug(f"Mihomo stdout: {stdout}")
                if stderr:
                    logger.debug(f"Mihomo stderr: {stderr}")

                if process.poll() is None: 
                    try:
                        os.killpg(os.getpgid(process.pid), 9) 
                        process.wait(timeout=1) 
                    except OSError as e:
                        logger.debug(f"خطا در kill کردن فرآیند Mihomo: {e}")
            
            if temp_config_file_path and os.path.exists(temp_config_file_path):
                os.remove(temp_config_file_path)

        return False

    def test_and_enrich_config(self, config_data: Dict[str, str]) -> Optional[Dict[str, str]]:
        """
        کانفیگ را تست می‌کند و در صورت فعال بودن، اطلاعات پرچم و کشور را اضافه می‌کند.
        این متد برای استفاده موازی طراحی شده است.
        config_data: دیکشنری حاوی 'config', 'protocol', 'canonical_id'.
        """
        config_string = config_data['config']
        protocol = config_data['protocol']
        
        server_address = self.validator.get_server_address(config_string, protocol)
        
        port = None
        try:
            parsed_url = urlparse(config_string)
            port = parsed_url.port
        except ValueError:
            pass 
        
        if not port:
            if protocol == "warp://":
                port = 80 
            
        if not server_address: 
            logger.debug(f"آدرس سرور برای کانفیگ '{config_string[:min(len(config_string), 50)]}...' یافت نشد. تست نادیده گرفته شد.")
            return None
        
        if port and protocol not in ["warp://"]: 
            if not self._ping_test(server_address, port):
                logger.debug(f"کانفیگ '{config_string[:min(len(config_string), 50)]}...' تست پینگ/پورت را رد کرد. نادیده گرفته شد.")
                return None
        elif not port:
            logger.debug(f"پورت برای کانفیگ '{config_string[:min(len(config_string), 50)]}...' یافت نشد. تست پینگ نادیده گرفته شد.")


        if protocol in self.mihomo_testable_protocols:
            if not self._test_with_mihomo(config_string):
                logger.debug(f"کانفیگ '{config_string[:min(len(config_string), 50)]}...' تست Mihomo را رد کرد. نادیده گرفته شد.")
                return None
            logger.debug(f"کانفیg '{config_string[:min(len(config_string), 50)]}...' تست Mihomo را با موفقیت پشت سر گذاشت.")
        else:
            logger.debug(f"پروتکل '{protocol}' توسط Mihomo تست نمی‌شود. از تست Mihomo صرف نظر شد و معتبر فرض شد.")

        flag, country = self.get_location(server_address) 
        
        enriched_config = {
            'config': config_string,
            'protocol': protocol,
            'flag': flag,
            'country': country,
            'canonical_id': config_data['canonical_id'] 
        }
        logger.debug(f"کانفیگ '{config_string[:min(len(config_string), 50)]}...' با موفقیت تست و غنی‌سازی شد.")
        return enriched_config

