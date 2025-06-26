import json
import base64
import uuid
import time
import socket 
import requests
from typing import Dict, Optional, Tuple, List
from urllib.parse import urlparse, parse_qs
import os
import re 
import logging 

# پیکربندی لاگ‌گیری
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ConfigToSingbox:
    """
    کلاس ConfigToSingbox مسئول تبدیل کانفیگ‌های پراکسی به فرمت قابل استفاده برای Singbox است.
    """
    def __init__(self):
        # مسیرهای خروجی برای فایل‌های Singbox
        self.output_dir = os.path.join('subs', 'singbox')
        self.output_file = os.path.join(self.output_dir, 'singbox_configs.json')
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
    def decode_vmess(self, config: str) -> Optional[Dict]:
        """رمزگشایی کانفیگ VMess."""
        try:
            encoded = config.replace('vmess://', '')
            decoded = base64.b64decode(encoded).decode('utf-8')
            return json.loads(decoded)
        except Exception as e:
            logger.debug(f"خطا در دیکد کردن VMess: {str(e)}")
            return None

    def parse_vless(self, config: str) -> Optional[Dict]:
        """تجزیه کانفیگ Vless."""
        try:
            url = urlparse(config)
            if url.scheme.lower() != 'vless' or not url.hostname:
                return None
            netloc = url.netloc.split('@')[-1]
            address, port = netloc.split(':') if ':' in netloc else (netloc, '443')
            params = parse_qs(url.query)
            return {
                'uuid': url.username,
                'address': address,
                'port': int(port),
                'flow': params.get('flow', [''])[0],
                'sni': params.get('sni', [address])[0],
                'type': params.get('type', ['tcp'])[0],
                'path': params.get('path', [''])[0],
                'host': params.get('host', [''])[0]
            }
        except Exception as e:
            logger.debug(f"خطا در تجزیه Vless: {str(e)}")
            return None

    def parse_trojan(self, config: str) -> Optional[Dict]:
        """تجزیه کانفیگ Trojan."""
        try:
            url = urlparse(config)
            if url.scheme.lower() != 'trojan' or not url.hostname:
                return None
            port = url.port or 443
            params = parse_qs(url.query)
            return {
                'password': url.username,
                'address': url.hostname,
                'port': port,
                'sni': params.get('sni', [url.hostname])[0],
                'alpn': params.get('alpn', [''])[0],
                'type': params.get('type', ['tcp'])[0],
                'path': params.get('path', [''])[0]
            }
        except Exception as e:
            logger.debug(f"خطا در تجزیه Trojan: {str(e)}")
            return None

    def parse_hysteria2(self, config: str) -> Optional[Dict]:
        """تجزیه کانفیگ Hysteria2."""
        try:
            url = urlparse(config)
            if url.scheme.lower() not in ['hysteria2', 'hy2'] or not url.hostname or not url.port:
                return None
            query = dict(pair.split('=') for pair in url.query.split('&')) if url.query else {}
            return {
                'address': url.hostname,
                'port': url.port,
                'password': url.username or query.get('password', ''),
                'sni': query.get('sni', url.hostname)
            }
        except Exception as e:
            logger.debug(f"خطا در تجزیه Hysteria2: {str(e)}")
            return None

    def parse_shadowsocks(self, config: str) -> Optional[Dict]:
        """تجزیه کانفیگ Shadowsocks."""
        try:
            parts = config.replace('ss://', '').split('@')
            if len(parts) != 2:
                return None
            method_pass = base64.b64decode(parts[0]).decode('utf-8')
            method, password = method_pass.split(':')
            server_parts = parts[1].split('#')[0]
            host, port = server_parts.split(':')
            return {
                'method': method,
                'password': password,
                'address': host,
                'port': int(port)
            }
        except Exception as e:
            logger.debug(f"خطا در تجزیه Shadowsocks: {str(e)}")
            return None
            
    def convert_to_singbox(self, config_line_with_flag: str) -> Optional[Dict]:
        """
        تبدیل یک خط کانفیگ کامل (شامل پرچم و کشور) به فرمت خروجی Singbox.
        فقط پروتکل‌های خاصی را تبدیل می‌کند (VMess, Vless, Trojan, Hysteria2, Shadowsocks).
        """
        try:
            # جداسازی پرچم، کشور و کانفیگ اصلی از خط ورودی
            parts = config_line_with_flag.strip().split(' ', 2)
            if len(parts) < 3: 
                flag = "🏳️"
                country = "Unknown"
                config = config_line_with_flag
            else:
                flag = parts[0]
                country = parts[1]
                config = parts[2] # این کانفیگ واقعی است (vmess://..., vless://..., etc.)
                
            config_lower = config.lower()

            # منطق تبدیل برای پروتکل‌های پشتیبانی شده توسط Singbox
            if config_lower.startswith('vmess://'):
                vmess_data = self.decode_vmess(config)
                if not vmess_data or not vmess_data.get('add') or not vmess_data.get('port') or not vmess_data.get('id'):
                    return None
                transport = {}
                if vmess_data.get('net') in ['ws', 'h2']:
                    if vmess_data.get('path', ''):
                        transport["path"] = vmess_data.get('path')
                    if vmess_data.get('host', ''):
                        transport["headers"] = {"Host": vmess_data.get('host')}
                    transport["type"] = vmess_data.get('net', 'tcp')
                
                return {
                    "type": "vmess",
                    "tag": f"{flag} {vmess_data.get('ps', '') or 'vmess'}-{str(uuid.uuid4())[:8]} ({country})",
                    "server": vmess_data['add'],
                    "server_port": int(vmess_data['port']),
                    "uuid": vmess_data['id'],
                    "security": vmess_data.get('scy', 'auto'),
                    "alter_id": int(vmess_data.get('aid', 0)),
                    "transport": transport,
                    "tls": {
                        "enabled": vmess_data.get('tls') == 'tls',
                        "insecure": True, # معمولا insecure=true برای دور زدن مشکلات گواهی
                        "server_name": vmess_data.get('sni', vmess_data['add'])
                    }
                }
            elif config_lower.startswith('vless://'):
                vless_data = self.parse_vless(config)
                if not vless_data:
                    return None
                transport = {}
                if vless_data['type'] == 'ws':
                    if vless_data.get('path', ''):
                        transport["path"] = vless_data.get('path')
                    if vless_data.get('host', ''):
                        transport["headers"] = {"Host": vless_data.get('host')}
                    transport["type"] = "ws"
                
                return {
                    "type": "vless",
                    "tag": f"{flag} vless-{str(uuid.uuid4())[:8]} ({country})",
                    "server": vless_data['address'],
                    "server_port": vless_data['port'],
                    "uuid": vless_data['uuid'],
                    "flow": vless_data['flow'],
                    "tls": {
                        "enabled": True,
                        "server_name": vless_data['sni'],
                        "insecure": True
                    },
                    "transport": transport
                }
            elif config_lower.startswith('trojan://'):
                trojan_data = self.parse_trojan(config)
                if not trojan_data:
                    return None
                transport = {}
                if trojan_data['type'] != 'tcp' and trojan_data.get('path', ''):
                    transport["path"] = trojan_data.get('path')
                    transport["type"] = trojan_data['type']
                
                return {
                    "type": "trojan",
                    "tag": f"{flag} trojan-{str(uuid.uuid4())[:8]} ({country})",
                    "server": trojan_data['address'],
                    "server_port": trojan_data['port'],
                    "password": trojan_data['password'],
                    "tls": {
                        "enabled": True,
                        "server_name": trojan_data['sni'],
                        "alpn": trojan_data['alpn'].split(',') if trojan_data['alpn'] else [],
                        "insecure": True
                    },
                    "transport": transport
                }
            elif config_lower.startswith('hysteria2://') or config_lower.startswith('hy2://'):
                hy2_data = self.parse_hysteria2(config)
                if not hy2_data or not hy2_data.get('address') or not hy2_data.get('port'):
                    return None
                
                return {
                    "type": "hysteria2",
                    "tag": f"{flag} hysteria2-{str(uuid.uuid4())[:8]} ({country})",
                    "server": hy2_data['address'],
                    "server_port": hy2_data['port'],
                    "password": hy2_data['password'],
                    "tls": {
                        "enabled": True,
                        "insecure": True,
                        "server_name": hy2_data['sni']
                    }
                }
            elif config_lower.startswith('ss://'):
                ss_data = self.parse_shadowsocks(config)
                if not ss_data or not ss_data.get('address') or not ss_data.get('port'):
                    return None
                
                return {
                    "type": "shadowsocks",
                    "tag": f"{flag} ss-{str(uuid.uuid4())[:8]} ({country})",
                    "server": ss_data['address'],
                    "server_port": ss_data['port'],
                    "method": ss_data['method'],
                    "password": ss_data['password']
                }
            # --- پروتکل‌های جدید در اینجا تبدیل نمی‌شوند ---
            else:
                logger.warning(f"پروتکل '{config_lower.split('://')[0]}' برای تبدیل به Singbox پشتیبانی نمی‌شود. کانفیگ نادیده گرفته شد: '{config_line_with_flag[:min(len(config_line_with_flag), 100)]}...'")
                return None
        except Exception as e:
            logger.error(f"خطا در تبدیل کانفیگ به Singbox: '{config_line_with_flag[:min(len(config_line_with_flag), 100)]}...' - {str(e)}")
            return None

    def process_configs(self):
        """
        کانفیگ‌ها را از فایل متنی می‌خواند، تبدیل می‌کند و کانفیگ‌های Singbox را در فایل خروجی ذخیره می‌کند.
        """
        try:
            # مسیر فایل متنی که توسط ConfigFetcher تولید می‌شود.
            config_file_path = os.path.join('subs', 'text', 'proxy_configs.txt')
            if not os.path.exists(config_file_path):
                logger.error(f"خطا: فایل کانفیگ '{config_file_path}' یافت نشد. لطفا ابتدا ConfigFetcher را اجرا کنید.")
                return

            with open(config_file_path, 'r', encoding='utf-8') as f:
                # خطوطی که با // شروع می‌شوند یا خالی هستند را نادیده بگیرید (هدر و خطوط خالی).
                configs_with_flags = [line.strip() for line in f if line.strip() and not line.strip().startswith('//')]
            
            outbounds = []
            valid_tags = []
            logger.info(f"در حال تبدیل {len(configs_with_flags)} کانفیگ به فرمت Singbox...")
            for config_line_with_flag in configs_with_flags:
                converted = self.convert_to_singbox(config_line_with_flag)
                if converted:
                    outbounds.append(converted)
                    valid_tags.append(converted['tag'])
            
            if not outbounds:
                logger.warning("هیچ کانفیگ معتبری برای تبدیل به Singbox یافت نشد.")
                # اطمینان از وجود پوشه خروجی و ایجاد فایل JSON خالی
                os.makedirs(self.output_dir, exist_ok=True)
                with open(self.output_file, 'w', encoding='utf-8') as f:
                    f.write("{}") # فایل JSON خالی
                return
            
            # پیکربندی بخش DNS برای Singbox
            dns_config = {
                "dns": {
                    "final": "local-dns",
                    "rules": [
                        {"clash_mode": "Global", "server": "proxy-dns", "source_ip_cidr": ["172.19.0.0/30"]},
                        {"server": "proxy-dns", "source_ip_cidr": ["172.19.0.0/30"]},
                        {"clash_mode": "Direct", "server": "direct-dns"}
                    ],
                    "servers": [
                        {"address": "tls://208.67.222.123", "address_resolver": "local-dns", "detour": "proxy", "tag": "proxy-dns"},
                        {"address": "local", "detour": "direct", "tag": "local-dns"},
                        {"address": "rcode://success", "tag": "block"},
                        {"address": "local", "detour": "direct", "tag": "direct-dns"}
                    ],
                    "strategy": "prefer_ipv4"
                }
            }
            # پیکربندی بخش Inbounds (ورودی‌ها) برای Singbox
            inbounds_config = [
                {"address": ["172.19.0.1/30", "fdfe:dcba:9876::1/126"], "auto_route": True, "endpoint_independent_nat": False, "mtu": 9000, "platform": {"http_proxy": {"enabled": True, "server": "127.0.0.1", "server_port": 2080}}, "sniff": True, "stack": "system", "strict_route": False, "type": "tun"},
                {"listen": "127.0.0.1", "listen_port": 2080, "sniff": True, "type": "mixed", "users": []}
            ]
            # پیکربندی بخش Outbounds (خروجی‌ها) برای Singbox
            outbounds_config = [
                {"tag": "proxy", "type": "selector", "outbounds": ["auto"] + valid_tags + ["direct"]},
                {"tag": "auto", "type": "urltest", "outbounds": valid_tags, "url": "http://www.gstatic.com/generate_204", "interval": "10m", "tolerance": 50},
                {"tag": "direct", "type": "direct"}
            ] + outbounds
            # پیکربندی بخش Route (مسیریابی) برای Singbox
            route_config = {
                "auto_detect_interface": True,
                "final": "proxy",
                "rules": [
                    {"clash_mode": "Direct", "outbound": "direct"},
                    {"clash_mode": "Global", "outbound": "proxy"},
                    {"protocol": "dns", "action": "hijack-dns"}
                ]
            }
            # ترکیب همه بخش‌ها در یک پیکربندی Singbox کامل JSON
            singbox_config = {**dns_config, "inbounds": inbounds_config, "outbounds": outbounds_config, "route": route_config}
            
            # ایجاد پوشه خروجی اگر وجود نداشت و ذخیره فایل
            os.makedirs(self.output_dir, exist_ok=True)
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(singbox_config, f, indent=2, ensure_ascii=False)
            logger.info(f"با موفقیت {len(outbounds)} کانفیگ Singbox در '{self.output_file}' ذخیره شد.")

        except Exception as e:
            logger.critical(f"خطا در پردازش کانفیگ‌ها برای Singbox: {str(e)}", exc_info=True) # نمایش traceback برای خطاهای بحرانی

def main():
    converter = ConfigToSingbox()
    converter.process_configs()

if __name__ == '__main__':
    main()

