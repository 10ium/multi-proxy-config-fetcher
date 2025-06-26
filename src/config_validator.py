import re
import base64
import json
from typing import Optional, Tuple, List, Dict, Any
from urllib.parse import unquote, urlparse, parse_qs

class ConfigValidator:
    """
    کلاس ConfigValidator برای اعتبارسنجی، پاکسازی و استخراج اطلاعات از رشته‌های کانفیگ پراکسی.
    این کلاس مسئول شناسایی، تجزیه و اعتبارسنجی پروتکل‌های مختلف و همچنین استخراج
    شناسه‌های کانونی برای حذف دقیق تکراری‌ها است.
    """
    @staticmethod
    def is_base64(s: str) -> bool:
        """بررسی می‌کند که آیا یک رشته Base64 معتبر است یا خیر."""
        try:
            s = s.rstrip('=') # حذف پدینگ برای بررسی راحت‌تر
            return bool(re.match(r'^[A-Za-z0-9+/\-_]*$', s))
        except:
            return False

    @staticmethod
    def decode_base64_url(s: str) -> Optional[bytes]:
        """رمزگشایی یک رشته Base64 URL-safe."""
        try:
            s = s.replace('-', '+').replace('_', '/') # تبدیل فرمت URL-safe به فرمت استاندارد Base64
            padding = 4 - (len(s) % 4)
            if padding != 4: # اضافه کردن پدینگ '=' در صورت نیاز
                s += '=' * padding
            return base64.b64decode(s)
        except:
            return None

    @staticmethod
    def decode_base64_text(text: str) -> Optional[str]:
        """رمزگشایی یک رشته متنی که ممکن است Base64 باشد."""
        try:
            if ConfigValidator.is_base64(text):
                decoded = ConfigValidator.decode_base64_url(text)
                if decoded:
                    return decoded.decode('utf-8')
            return None
        except:
            return None

    @staticmethod
    def clean_vmess_config(config: str) -> str:
        """پاکسازی کانفیگ VMess با حذف بخش‌های اضافی بعد از رشته Base64 اصلی."""
        if "vmess://" in config:
            base64_part = config[8:]
            # فقط کاراکترهای مجاز Base64 (شامل - و _ برای URL-safe) را نگه دارید
            base64_clean = re.split(r'[^A-Za-z0-9+/=_-]', base64_part)[0]
            return f"vmess://{base64_clean}"
        return config

    @staticmethod
    def normalize_hysteria2_protocol(config: str) -> str:
        """نرمال‌سازی 'hy2://' به 'hysteria2://'."""
        if config.startswith('hy2://'):
            return config.replace('hy2://', 'hysteria2://', 1)
        return config
    
    @staticmethod
    def clean_ssr_config(config: str) -> str:
        """پاکسازی کانفیگ SSR، اطمینان از شروع با ssr:// و Base64 معتبر."""
        if config.startswith("ssr://"):
            parts = config[6:].split("/?") # جدا کردن قبل از پارامترها
            base64_part = parts[0]
            # اطمینان از معتبر بودن بخش Base64
            if ConfigValidator.is_base64(base64_part):
                return config
        return config # اگر فرمت SSR معتبر نبود، اصلی را برگردانید

    @staticmethod
    def is_vmess_config(config: str) -> bool:
        """بررسی اعتبار یک کانفیگ VMess."""
        try:
            if not config.startswith('vmess://'):
                return False
            base64_part = config[8:]
            decoded = ConfigValidator.decode_base64_url(base64_part)
            if decoded:
                json.loads(decoded) # تلاش برای بارگذاری به عنوان JSON
                return True
            return False
        except Exception:
            return False
    
    @staticmethod
    def is_ssr_config(config: str) -> bool:
        """بررسی اعتبار یک کانفیگ SSR با تلاش برای دیکد کردن بخش Base64 آن."""
        try:
            if not config.startswith('ssr://'):
                return False
            base64_part = config[6:].split('/?')[0] # گرفتن بخش قبل از پارامترهای اختیاری
            decoded_bytes = ConfigValidator.decode_base64_url(base64_part)
            if not decoded_bytes:
                return False
            decoded_str = decoded_bytes.decode('utf-8', errors='ignore') 
            parts = decoded_str.split(':')
            return len(parts) >= 6 # سرور، پورت، پروتکل، متد، obfs، پسورد
        except Exception:
            return False
            
    @staticmethod
    def is_hysteria_config(config: str) -> bool:
        """بررسی اعتبار یک کانفیگ Hysteria 1."""
        try:
            if not config.startswith('hysteria://'):
                return False
            parsed = urlparse(config)
            return bool(parsed.netloc and parsed.port) # باید سرور و پورت داشته باشد
        except Exception:
            return False

    @staticmethod
    def is_tuic_config(config: str) -> bool:
        """بررسی اعتبار یک کانفیگ TUIC."""
        try:
            if config.startswith('tuic://'):
                parsed = urlparse(config)
                return bool(parsed.netloc and ':' in parsed.netloc and parsed.username) # باید هاست، پورت و UUID داشته باشد
            return False
        except:
            return False
            
    @staticmethod
    def is_mieru_config(config: str) -> bool:
        """بررسی اعتبار یک کانفیگ Mieru."""
        try:
            if not config.startswith('mieru://'):
                return False
            parsed = urlparse(config)
            return bool(parsed.netloc and parsed.username and parsed.port) # نیاز به uuid@server:port دارد
        except:
            return False

    @staticmethod
    def is_snell_config(config: str) -> bool:
        """بررسی اعتبار یک کانفیگ Snell."""
        try:
            if not config.startswith('snell://'):
                return False
            parsed = urlparse(config)
            return bool(parsed.netloc and parsed.port and 'psk=' in parsed.query) # نیاز به server:port و psk دارد
        except:
            return False

    @staticmethod
    def is_anytls_config(config: str) -> bool:
        """بررسی اعتبار یک کانفیگ Anytls."""
        try:
            if not config.startswith('anytls://'):
                return False
            parsed = urlparse(config)
            return bool(parsed.netloc and parsed.port) # نیاز به server:port دارد
        except Exception:
            return False

    @staticmethod
    def is_ssh_config(config: str) -> bool:
        """بررسی اعتبار یک کانفیگ SSH."""
        try:
            if not config.startswith('ssh://'):
                return False
            parsed = urlparse(config)
            return bool(parsed.netloc and parsed.username and parsed.password and parsed.port) # نیاز به user:pass@server:port دارد
        except Exception:
            return False

    @staticmethod
    def is_juicity_config(config: str) -> bool:
        """بررسی اعتبار یک کانفیگ Juicity."""
        try:
            if not config.startswith('juicity://'):
                return False
            parsed = urlparse(config)
            return bool(parsed.netloc and parsed.username and parsed.port and 'password=' in parsed.query) # نیاز به uuid@server:port و password دارد
        except Exception:
            return False
            
    @staticmethod
    def is_warp_config(config: str) -> bool:
        """بررسی اعتبار یک کانفیگ WARP."""
        try:
            if not config.startswith('warp://'):
                return False
            return True 
        except Exception:
            return False

    @staticmethod
    def convert_ssconf_to_https(url: str) -> str:
        """تبدیل 'ssconf://' URL به 'https://'."""
        if url.startswith('ssconf://'):
            return url.replace('ssconf://', 'https://', 1)
        return url

    @staticmethod
    def is_base64_config(config: str) -> Tuple[bool, str]:
        """بررسی می‌کند که آیا یک کانفیگ با Base64 کدگذاری شده است و نوع پروتکل آن را برمی‌گرداند."""
        protocols = ['vmess://', 'vless://', 'ss://', 'tuic://', 'ssr://']
        for protocol in protocols:
            if config.startswith(protocol):
                base64_part = config[len(protocol):]
                decoded_url = unquote(base64_part)
                if (ConfigValidator.is_base64(decoded_url) or 
                    ConfigValidator.is_base64(base64_part)):
                    return True, protocol[:-3]
        return False, ''

    @staticmethod
    def check_base64_content(text: str) -> Optional[str]:
        """بررسی می‌کند که آیا یک متن Base64 است و اگر دیکد شد، شامل پروتکل‌های پراکسی است یا خیر."""
        try:
            decoded_text = ConfigValidator.decode_base64_text(text)
            if decoded_text:
                protocols = ['vmess://', 'vless://', 'ss://', 'trojan://', 'hysteria2://', 'hy2://', 'wireguard://', 'tuic://', 'ssconf://',
                             'ssr://', 'mieru://', 'snell://', 'anytls://', 'ssh://', 'juicity://',
                             'hysteria://', 'warp://']
                for protocol in protocols:
                    if protocol in decoded_text:
                        return decoded_text
            return None
        except:
            return None

    @staticmethod
    def split_configs(text: str) -> List[str]:
        """جداسازی چندین کانفیگ پراکسی از یک متن بزرگ."""
        configs = []
        lines = text.split('\n')
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            if ConfigValidator.is_base64(line):
                decoded_content = ConfigValidator.check_base64_content(line)
                if decoded_content:
                    line = decoded_content
                    
            protocols = ['vmess://', 'vless://', 'ss://', 'trojan://', 'hysteria2://', 'hy2://', 'wireguard://', 'tuic://', 'ssconf://',
                         'ssr://', 'mieru://', 'snell://', 'anytls://', 'ssh://', 'juicity://',
                         'hysteria://', 'warp://']
            current_pos = 0
            text_length = len(line)
            
            while current_pos < text_length:
                next_config_start = text_length
                matching_protocol = None
                
                for protocol in protocols:
                    protocol_pos = line.find(protocol, current_pos)
                    if protocol_pos != -1 and protocol_pos < next_config_start:
                        next_config_start = protocol_pos
                        matching_protocol = protocol
                
                if matching_protocol:
                    if current_pos < next_config_start and configs:
                        current_config = line[current_pos:next_config_start].strip()
                        if ConfigValidator.is_valid_config(current_config):
                            configs.append(current_config)
                    
                    current_pos = next_config_start
                    next_protocol_pos = text_length
                    
                    for protocol in protocols:
                        pos = line.find(protocol, next_config_start + len(matching_protocol))
                        if pos != -1 and pos < next_protocol_pos:
                            next_protocol_pos = pos
                    
                    current_config = line[next_config_start:next_protocol_pos].strip()
                    if matching_protocol == "vmess://":
                        current_config = ConfigValidator.clean_vmess_config(current_config)
                    elif matching_protocol == "hy2://":
                        current_config = ConfigValidator.normalize_hysteria2_protocol(current_config)
                    elif matching_protocol == "ssr://":
                        current_config = ConfigValidator.clean_ssr_config(current_config)
                        
                    if ConfigValidator.is_valid_config(current_config):
                        configs.append(current_config)
                    
                    current_pos = next_protocol_pos
                else:
                    break
                    
        return configs

    @staticmethod
    def clean_config(config: str) -> str:
        """پاکسازی یک رشته کانفیگ از کاراکترهای نامرئی، ایموجی‌ها و فضاهای اضافی."""
        config = re.sub(r'[\U0001F300-\U0001F9FF]', '', config)
        config = re.sub(r'[\x00-\x08\x0B-\x1F\x7F-\x9F]', '', config)
        config = re.sub(r'[^\S\r\n]+', ' ', config)
        config = config.strip()
        return config

    @staticmethod
    def is_valid_config(config: str) -> bool:
        """بررسی اعتبار اولیه یک کانفیگ (شروع با پروتکل شناخته شده)."""
        if not config:
            return False
            
        protocols = ['vmess://', 'vless://', 'ss://', 'trojan://', 'hysteria2://', 'hy2://', 'wireguard://', 'tuic://', 'ssconf://',
                     'ssr://', 'mieru://', 'snell://', 'anytls://', 'ssh://', 'juicity://',
                     'hysteria://', 'warp://']
        return any(config.startswith(p) for p in protocols)

    @classmethod
    def validate_protocol_config(cls, config: str, protocol: str) -> bool:
        """اعتبارسنجی پروتکل خاص یک کانفیگ."""
        try:
            if protocol == 'vmess://':
                return cls.is_vmess_config(config)
            elif protocol == 'tuic://':
                return cls.is_tuic_config(config)
            elif protocol == 'ssr://':
                return cls.is_ssr_config(config)
            elif protocol == 'mieru://':
                return cls.is_mieru_config(config)
            elif protocol == 'snell://':
                return cls.is_snell_config(config)
            elif protocol == 'anytls://':
                return cls.is_anytls_config(config)
            elif protocol == 'ssh://':
                return cls.is_ssh_config(config)
            elif protocol == 'juicity://':
                return cls.is_juicity_config(config)
            elif protocol == 'hysteria://':
                return cls.is_hysteria_config(config)
            elif protocol == 'warp://':
                return cls.is_warp_config(config)
            elif protocol in ['vless://', 'trojan://', 'hysteria2://', 'wireguard://']:
                parsed = urlparse(config)
                return bool(parsed.netloc and parsed.port)
            elif protocol == 'ss://':
                parts = config.replace('ss://', '').split('@')
                if len(parts) == 2:
                    try:
                        base64.b64decode(parts[0]).decode('utf-8')
                        return True
                    except:
                        pass
                return False
            elif protocol == 'ssconf://':
                return True
            return False
        except Exception:
            return False

    @staticmethod
    def extract_telegram_channels_from_config(config_string: str) -> List[str]:
        """کانال‌های تلگرام را از داخل مشخصات یک کانفیگ (مانند تگ‌ها یا پارامترهای خاص) استخراج می‌کند."""
        found_channels = set()
        
        telegram_url_pattern = r"(?:https?://)?t\.me/(?:s/)?([a-zA-Z0-9_]+)"

        parsed_url = urlparse(config_string)
        
        if parsed_url.fragment:
            matches = re.findall(telegram_url_pattern, unquote(parsed_url.fragment))
            for match in matches:
                found_channels.add(f"https://t.me/s/{match}")

        if parsed_url.query:
            query_params = parse_qs(parsed_url.query)
            for key, values in query_params.items():
                for value in values:
                    matches = re.findall(telegram_url_pattern, unquote(value))
                    for match in matches:
                        found_channels.add(f"https://t.me/s/{match}")

        if config_string.startswith('vmess://'):
            try:
                base64_part = config_string[len('vmess://'):]
                decoded_vmess = ConfigValidator.decode_base64_url(base64_part)
                if decoded_vmess:
                    vmess_data = json.loads(decoded_vmess.decode('utf-8'))
                    potential_fields = [vmess_data.get('ps'), vmess_data.get('id')]
                    for field_value in potential_fields:
                        if isinstance(field_value, str):
                            matches = re.findall(telegram_url_pattern, field_value)
                            for match in matches:
                                found_channels.add(f"https://t.me/s/{match}")
            except Exception:
                pass
        elif config_string.startswith('ssr://'):
            try:
                base64_part = config_string[len('ssr://'):].split('/?')[0]
                decoded_ssr = ConfigValidator.decode_base64_url(base64_part)
                if decoded_ssr:
                    full_ssr_params = decoded_ssr.decode('utf-8', errors='ignore')
                    
                    ssr_fragment_match = re.search(r'#(.+)', config_string)
                    if ssr_fragment_match:
                        remark_base64 = ssr_fragment_match.group(1)
                        decoded_remark = ConfigValidator.decode_base64_url(remark_base64)
                        if decoded_remark:
                            matches = re.findall(telegram_url_pattern, decoded_remark.decode('utf-8', errors='ignore'))
                            for match in matches:
                                found_channels.add(f"https://t.me/s/{match}")
                    
                    matches = re.findall(telegram_url_pattern, full_ssr_params)
                    for match in matches:
                        found_channels.add(f"https://t.me/s/{match}")

            except Exception:
                pass
        elif config_string.startswith('hysteria://'):
            try:
                parsed_hy = urlparse(config_string)
                if parsed_hy.fragment:
                    matches = re.findall(telegram_url_pattern, unquote(parsed_hy.fragment))
                    for match in matches:
                        found_channels.add(f"https://t.me/s/{match}")
                query_params = parse_qs(parsed_hy.query)
                if 'password' in query_params:
                    matches = re.findall(telegram_url_pattern, query_params['password'][0])
                    for match in matches:
                        found_channels.add(f"https://t.me/s/{match}")
            except Exception:
                pass

        # 4. جستجوی عمومی در کل رشته کانفیگ به عنوان آخرین چاره
        matches = re.findall(telegram_url_pattern, config_string)
        for match in matches:
            found_channels.add(f"https://t.me/s/{match}")

        return list(found_channels)
    
    @staticmethod
    def get_server_address(config_string: str, protocol_prefix: str) -> Optional[str]:
        """
        استخراج آدرس سرور (هاست‌نام یا IP) از یک رشته کانفیگ با توجه به پروتکل آن.
        """
        try:
            if protocol_prefix == 'hy2://':
                protocol_prefix = 'hysteria2://'
            elif protocol_prefix == 'hy1://':
                protocol_prefix = 'hysteria://'

            parsed_url = urlparse(config_string)
            
            if protocol_prefix == 'vmess://':
                decoded_data = base64.b64decode(config_string[len(protocol_prefix):]).decode('utf-8')
                vmess_json = json.loads(decoded_data)
                return vmess_json.get('add')
            elif protocol_prefix in ['vless://', 'trojan://', 'hysteria2://', 'tuic://', 'mieru://', 'snell://', 'anytls://', 'juicity://', 'hysteria://', 'ssh://']:
                return parsed_url.hostname
            elif protocol_prefix == 'ss://':
                parts = config_string.replace('ss://', '').split('@')
                if len(parts) == 2:
                    server_part = parts[1].split('#')[0]
                    return server_part.split(':')[0]
            elif protocol_prefix == 'ssr://':
                base64_part = config_string[len(protocol_prefix):].split('/?')[0]
                decoded_params = ConfigValidator.decode_base64_url(base64_part)
                if decoded_params:
                    return decoded_params.decode('utf-8', errors='ignore').split(':')[0]
                return None
            elif protocol_prefix == 'wireguard://':
                endpoint_match = re.search(r'@([^/?#]+)', config_string)
                if endpoint_match:
                    endpoint = endpoint_match.group(1)
                    return endpoint.split(':')[0]
            elif protocol_prefix == 'warp://':
                if parsed_url.hostname:
                    return parsed_url.hostname
                return "162.159.192.1" 
            
            return None
        except Exception:
            return None
            
    @staticmethod
    def get_canonical_parameters(config_string: str, protocol_prefix: str) -> Optional[Dict[str, Any]]:
        """
        استخراج پارامترهای "کانونی" (اصلی و عملکردی) یک کانفیگ برای شناسایی تکراری‌ها.
        این تابع بخش‌های غیرعملکردی مانند remark/tag را نادیده می‌گیرد.
        """
        parsed_url = urlparse(config_string)
        
        if protocol_prefix == 'hy2://':
            protocol_prefix = 'hysteria2://'
        elif protocol_prefix == 'hy1://':
            protocol_prefix = 'hysteria://'

        canonical_params: Dict[str, Any] = {"protocol": protocol_prefix}

        try:
            if protocol_prefix == 'vmess://':
                decoded_data = base64.b64decode(config_string[len(protocol_prefix):]).decode('utf-8')
                vmess_json = json.loads(decoded_data)
                canonical_params.update({
                    'server': vmess_json.get('add'),
                    'port': vmess_json.get('port'),
                    'uuid': vmess_json.get('id'),
                    'security': vmess_json.get('scy', 'auto'),
                    'net': vmess_json.get('net', 'tcp'),
                    'type': vmess_json.get('type', 'none'),
                    'host': vmess_json.get('host', ''),
                    'path': vmess_json.get('path', ''),
                    'tls': vmess_json.get('tls', '')
                })
            elif protocol_prefix == 'vless://':
                canonical_params.update({
                    'server': parsed_url.hostname,
                    'port': parsed_url.port,
                    'uuid': parsed_url.username,
                    'flow': parse_qs(parsed_url.query).get('flow', [''])[0],
                    'security': parse_qs(parsed_url.query).get('security', [''])[0],
                    'sni': parse_qs(parsed_url.query).get('sni', [parsed_url.hostname])[0],
                    'type': parse_qs(parsed_url.query).get('type', ['tcp'])[0],
                    'host': parse_qs(parsed_url.query).get('host', [''])[0],
                    'path': parse_qs(parsed_url.query).get('path', [''])[0]
                })
            elif protocol_prefix == 'trojan://':
                canonical_params.update({
                    'server': parsed_url.hostname,
                    'port': parsed_url.port,
                    'password': parsed_url.username,
                    'sni': parse_qs(parsed_url.query).get('sni', [parsed_url.hostname])[0],
                    'alpn': parse_qs(parsed_url.query).get('alpn', [''])[0],
                    'type': parse_qs(parsed_url.query).get('type', ['tcp'])[0],
                    'path': parse_qs(parsed_url.query).get('path', [''])[0]
                })
            elif protocol_prefix in ['hysteria2://', 'hysteria://']:
                query_params = parse_qs(parsed_url.query)
                canonical_params.update({
                    'server': parsed_url.hostname,
                    'port': parsed_url.port,
                    'password': parsed_url.username or query_params.get('auth', [''])[0] or query_params.get('password', [''])[0],
                    'sni': query_params.get('sni', [parsed_url.hostname])[0],
                    'alpn': query_params.get('alpn', [''])[0],
                    'obfs': query_params.get('obfs', [''])[0] 
                })
            elif protocol_prefix == 'ss://':
                parts = config_string.replace('ss://', '').split('@')
                if len(parts) == 2:
                    method_pass_encoded = parts[0]
                    decoded_method_pass = base64.b64decode(method_pass_encoded.replace('-', '+').replace('_', '/')).decode('utf-8')
                    method, password = decoded_method_pass.split(':')
                    server_port_part = parts[1].split('#')[0]
                    server, port = server_port_part.split(':')
                    canonical_params.update({
                        'server': server,
                        'port': int(port),
                        'method': method,
                        'password': password
                    })
            elif protocol_prefix == 'ssr://':
                base64_part = config_string[len(protocol_prefix):].split('/?')[0]
                decoded_params_str = ConfigValidator.decode_base64_url(base64_part).decode('utf-8', errors='ignore')
                ssr_parts = decoded_params_str.split(':')
                if len(ssr_parts) >= 6:
                    password_encoded = ssr_parts[5]
                    password = base64.b64decode(password_encoded.replace('-', '+').replace('_', '/')).decode('utf-8', errors='ignore')
                    canonical_params.update({
                        'server': ssr_parts[0],
                        'port': int(ssr_parts[1]),
                        'protocol_type': ssr_parts[2],
                        'method': ssr_parts[3],
                        'obfs': ssr_parts[4],
                        'password': password
                    })
            elif protocol_prefix == 'tuic://':
                query_params = parse_qs(parsed_url.query)
                canonical_params.update({
                    'server': parsed_url.hostname,
                    'port': parsed_url.port,
                    'uuid': parsed_url.username,
                    'password': query_params.get('password', [''])[0],
                    'alpn': query_params.get('alpn', [''])[0],
                    'congestion_control': query_params.get('congestion_control', [''])[0],
                    'udp_relay_mode': query_params.get('udp_relay_mode', [''])[0],
                    'disable_sni': query_params.get('disable_sni', [''])[0],
                    'tls': 'tls' in parsed_url.scheme
                })
            elif protocol_prefix == 'mieru://':
                query_params = parse_qs(parsed_url.query)
                canonical_params.update({
                    'uuid': parsed_url.username,
                    'server': parsed_url.hostname,
                    'port': parsed_url.port,
                    'tls': query_params.get('tls', [''])[0],
                    'udp_over_tcp': query_params.get('udp_over_tcp', [''])[0],
                    'peer_fingerprint': query_params.get('peer_fingerprint', [''])[0],
                    'server_name': query_params.get('server_name', [''])[0],
                })
            elif protocol_prefix == 'snell://':
                query_params = parse_qs(parsed_url.query)
                canonical_params.update({
                    'server': parsed_url.hostname,
                    'port': parsed_url.port,
                    'psk': query_params.get('psk', [''])[0],
                    'version': query_params.get('version', [''])[0],
                    'obfs': query_params.get('obfs', [''])[0],
                    'obfs_uri': query_params.get('uri', [''])[0],
                    'tfo': query_params.get('tfo', [''])[0]
                })
            elif protocol_prefix == 'anytls://':
                query_params = parse_qs(parsed_url.query)
                canonical_params.update({
                    'protocol_type': parsed_url.username,
                    'server': parsed_url.hostname,
                    'port': parsed_url.port,
                    'sni': query_params.get('sni', [''])[0],
                    'path': query_params.get('path', [''])[0],
                    'host': query_params.get('host', [''])[0],
                    'tls': query_params.get('tls', [''])[0]
                })
            elif protocol_prefix == 'ssh://':
                canonical_params.update({
                    'user': parsed_url.username,
                    'password': parsed_url.password,
                    'server': parsed_url.hostname,
                    'port': parsed_url.port
                })
            elif protocol_prefix == 'juicity://':
                query_params = parse_qs(parsed_url.query)
                canonical_params.update({
                    'uuid': parsed_url.username,
                    'server': parsed_url.hostname,
                    'port': parsed_url.port,
                    'password': query_params.get('password', [''])[0],
                    'security': query_params.get('security', [''])[0],
                    'fingerprint': query_params.get('fingerprint', [''])[0],
                    'congestion_control': query_params.get('congestion_control', [''])[0],
                    'alpn': query_params.get('alpn', [''])[0]
                })
            elif protocol_prefix == 'wireguard://':
                endpoint_match = re.search(r'@([^/?#]+)', config_string)
                canonical_params.update({
                    'public_key': parsed_url.username,
                    'endpoint': endpoint_match.group(1) if endpoint_match else None
                })
            elif protocol_prefix == 'warp://':
                canonical_params.update({
                    'server': parsed_url.hostname if parsed_url.hostname else '162.159.192.1', 
                    'uuid': parsed_url.username 
                })
            else: # پروتکل ناشناخته
                return None

            canonical_params = {k: v for k, v in canonical_params.items() if v is not None and v != ''}
            return canonical_params
        except Exception:
            return None

    @staticmethod
    def get_canonical_id(config_string: str, protocol_prefix: str) -> Optional[str]:
        """
        تولید یک رشته شناسه کانونی (Canonical ID) برای یک کانفیگ.
        این شناسه برای شناسایی دقیق کانفیگ‌های تکراری استفاده می‌شود.
        """
        canonical_params = ConfigValidator.get_canonical_parameters(config_string, protocol_prefix)
        if not canonical_params:
            return None
        
        sorted_canonical_params = sorted(canonical_params.items())
        return json.dumps(sorted_canonical_params, sort_keys=True)

