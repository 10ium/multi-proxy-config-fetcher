import re
import base64
import json
import logging 
from typing import Optional, Tuple, List, Dict, Any
from urllib.parse import unquote, urlparse, parse_qs

logger = logging.getLogger(__name__)

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
            # Base64 URL-safe (RFC 4648) ممکن است شامل - و _ باشد. padding (=) اختیاری است.
            # برای اطمینان بیشتر، کاراکترهای URL-safe را به استاندارد تبدیل می‌کنیم
            s_cleaned = s.replace('-', '+').replace('_', '/') 
            # سپس padding را اضافه می‌کنیم
            padding = len(s_cleaned) % 4
            if padding != 0:
                s_cleaned += '=' * (4 - padding)
            base64.b64decode(s_cleaned)
            return True
        except:
            return False

    @staticmethod
    def decode_base64_url(s: str) -> Optional[bytes]:
        """رمزگشایی یک رشته Base64 URL-safe."""
        try:
            # تبدیل کاراکترهای URL-safe به استاندارد Base64
            s = s.replace('-', '+').replace('_', '/') 
            # اضافه کردن padding در صورت نیاز
            padding = 4 - (len(s) % 4)
            if padding != 4: # اگر padding لازم نبود (یعنی %4 == 0)، هیچ = اضافه نمی‌شود
                s += '=' * padding
            return base64.b64decode(s)
        except:
            return None

    @staticmethod
    def decode_base64_text(text: str) -> Optional[str]:
        """
        رمزگشایی یک رشته متنی که ممکن است Base64 باشد.
        این متد برای دیکد کردن کل یک سابسکریپشن Base64 طراحی شده است.
        """
        try:
            # بررسی اولیه Base64 بودن
            if ConfigValidator.is_base64(text):
                decoded_bytes = ConfigValidator.decode_base64_url(text)
                if decoded_bytes:
                    return decoded_bytes.decode('utf-8', errors='ignore') # با نادیده گرفتن خطاها دیکد کن
            return None
        except Exception as e:
            logger.debug(f"خطا در decode_base64_text: {e}")
            return None

    @staticmethod
    def clean_vmess_config(config: str) -> str:
        """پاکسازی کانفیگ VMess با حذف بخش‌های اضافی بعد از رشته Base64 اصلی."""
        if "vmess://" in config:
            base64_part = config[8:]
            # یافتن پایان بخش Base64 با جستجوی اولین کاراکتر غیرمجاز Base64
            # (مثلاً فاصله، خط جدید، یا سایر کاراکترها)
            base64_clean_match = re.match(r'([A-Za-z0-9+/=_-]*)', base64_part)
            if base64_clean_match:
                base64_clean = base64_clean_match.group(1)
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
            # بخش Base64 تا قبل از /? یا #
            base64_part_match = re.match(r'ssr://([A-Za-z0-9+/=_-]+)', config)
            if base64_part_match:
                base64_part = base64_part_match.group(1)
                if ConfigValidator.is_base64(base64_part):
                    return f"ssr://{base64_part}"
        return config

    @staticmethod
    def is_vmess_config(config: str) -> bool:
        """بررسی اعتبار یک کانفیگ VMess."""
        try:
            if not config.startswith('vmess://'):
                return False
            base64_part = config[8:]
            decoded = ConfigValidator.decode_base64_url(base64_part)
            if decoded:
                json.loads(decoded) # تلاش برای بارگذاری JSON
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
            base64_part = config[6:].split('/?')[0].split('#')[0] # بخش Base64 تا قبل از /? یا #
            decoded_bytes = ConfigValidator.decode_base64_url(base64_part)
            if not decoded_bytes:
                return False
            decoded_str = decoded_bytes.decode('utf-8', errors='ignore') 
            parts = decoded_str.split(':')
            # SSR معمولاً دارای حداقل 6 بخش است: server:port:protocol:method:obfs:password
            return len(parts) >= 6 
        except Exception:
            return False

    @staticmethod
    def is_hysteria_config(config: str) -> bool:
        """بررسی اعتبار یک کانفیگ Hysteria 1."""
        try:
            if not config.startswith('hysteria://'):
                return False
            parsed = urlparse(config)
            # باید شامل netloc (host:port) باشد
            return bool(parsed.netloc and parsed.port) 
        except Exception:
            return False

    @staticmethod
    def is_tuic_config(config: str) -> bool:
        """بررسی اعتبار یک کانفیگ TUIC."""
        try:
            if not config.startswith('tuic://'):
                return False
            parsed = urlparse(config)
            # باید شامل netloc (host:port) و username (UUID) باشد
            return bool(parsed.netloc and parsed.port and parsed.username) 
        except Exception:
            return False

    @staticmethod
    def is_mieru_config(config: str) -> bool:
        """بررسی اعتبار یک کانفیگ Mieru."""
        try:
            if not config.startswith('mieru://'):
                return False
            parsed = urlparse(config)
            # باید شامل netloc (host:port) و username (UUID) باشد
            return bool(parsed.netloc and parsed.username and parsed.port) 
        except Exception:
            return False

    @staticmethod
    def is_snell_config(config: str) -> bool:
        """بررسی اعتبار یک کانفیگ Snell."""
        try:
            if not config.startswith('snell://'):
                return False
            parsed = urlparse(config)
            # باید شامل netloc (host:port) و پارامتر psk باشد
            return bool(parsed.netloc and parsed.port and 'psk=' in parsed.query) 
        except Exception:
            return False

    @staticmethod
    def is_anytls_config(config: str) -> bool:
        """بررسی اعتبار یک کانفیگ Anytls."""
        try:
            if not config.startswith('anytls://'):
                return False
            parsed = urlparse(config)
            # باید شامل netloc (host:port) باشد
            return bool(parsed.netloc and parsed.port) 
        except Exception:
            return False

    @staticmethod
    def is_ssh_config(config: str) -> bool:
        """بررسی اعتبار یک کانفیگ SSH."""
        try:
            if not config.startswith('ssh://'):
                return False
            parsed = urlparse(config)
            # باید شامل netloc (host:port), username و password باشد
            return bool(parsed.netloc and parsed.username and parsed.password and parsed.port) 
        except Exception:
            return False

    @staticmethod
    def is_juicity_config(config: str) -> bool:
        """بررسی اعتبار یک کانفیگ Juicity."""
        try:
            if not config.startswith('juicity://'):
                return False
            parsed = urlparse(config)
            # باید شامل netloc (host:port), username (UUID) و پارامتر password باشد
            return bool(parsed.netloc and parsed.username and parsed.port and 'password=' in parsed.query) 
        except Exception:
            return False

    @staticmethod
    def is_warp_config(config: str) -> bool:
        """بررسی اعتبار یک کانفیگ WARP."""
        try:
            if not config.startswith('warp://'):
                return False
            # برای WARP، فقط شروع با پروتکل کافی است (ساختار داخلی پیچیده است)
            return True 
        except Exception:
            return False
            
    @staticmethod
    def is_wireguard_config(config: str) -> bool:
        """بررسی اعتبار یک کانفیگ WireGuard."""
        try:
            if not config.startswith('wireguard://'):
                return False
            parsed = urlparse(config)
            # WireGuard معمولاً شامل یک کلید عمومی در username و یک endpoint است
            return bool(parsed.username and parsed.query and 'endpoint=' in parsed.query)
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
                # unquote برای مدیریت encode شده‌های URL در Base64
                decoded_url = unquote(base64_part) 
                if (ConfigValidator.is_base64(decoded_url) or 
                    ConfigValidator.is_base64(base64_part)):
                    return True, protocol[:-3] # برگرداندن نام پروتکل بدون ://
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
                        return decoded_text # اگر حاوی پروتکل بود، متن دیکد شده را برگردان
            return None
        except Exception:
            return None

    @staticmethod
    def split_configs(text: str) -> List[str]:
        """جداسازی چندین کانفیگ پراکسی از یک متن بزرگ."""
        configs = []
        lines = text.split('\n')

        protocols = ['vmess://', 'vless://', 'ss://', 'trojan://', 'hysteria2://', 'hy2://', 'wireguard://', 'tuic://', 'ssconf://',
                     'ssr://', 'mieru://', 'snell://', 'anytls://', 'ssh://', 'juicity://',
                     'hysteria://', 'warp://']
        # regex برای پیدا کردن شروع هر کانفیگ (پروتکل و سپس کاراکترهای مجاز URL-safe)
        # این regex سعی می‌کند یک خط کامل کانفیگ را تا کاراکترهای خاصی (فضای خالی، خط جدید، یا شروع پروتکل بعدی) بگیرد
        # ^(?:vmess|vless|ss|trojan|hysteria2|hy2|wireguard|tuic|ssconf|ssr|mieru|snell|anytls|ssh|juicity|hysteria|warp):\/\/[^\s\n]+
        # این regex بیش از حد تهاجمی است و ممکن است باعث از دست رفتن کانفیگ‌ها شود.
        # بهتر است ابتدا خطوط را بررسی کنیم و سپس روی هر خط یا بلوک، شروع پروتکل را پیدا کنیم.

        # این منطق split_configs در ConnectionValidator قبلی بود و تا حد زیادی صحیح است،
        # اما نیاز به اصلاح برای بهبود دقت جداسازی دارد.
        # این تابع به طور کلی سعی می‌کند هر پروتکلی را در هر جایی از متن پیدا کند.
        # ما آن را کمی بهینه‌تر می‌کنیم.
        
        # یک regex برای یافتن هر پروتکل در متن
        protocol_pattern = '|'.join(re.escape(p) for p in protocols)
        # یافتن همه تطابق‌ها به همراه موقعیت شروع آنها
        matches = list(re.finditer(f"({protocol_pattern})", text))

        if not matches:
            # اگر هیچ پروتکلی مستقیماً در متن یافت نشد، بررسی کن که آیا کل متن یک Base64 کد شده است
            decoded_content_if_base64 = ConfigValidator.check_base64_content(text)
            if decoded_content_if_base64:
                # اگر Base64 بود و حاوی پروتکل بود، recursively آن را split کن
                return ConfigValidator.split_configs(decoded_content_if_base64)
            return [] # اگر نه، هیچ کانفیگی پیدا نشد

        # پردازش تطابق‌ها برای استخراج کانفیگ‌های کامل
        for i, match in enumerate(matches):
            start = match.start()
            # پایان کانفیگ فعلی: شروع کانفیگ بعدی یا پایان متن
            end = matches[i+1].start() if i + 1 < len(matches) else len(text)
            
            config_candidate = text[start:end].strip()

            # اعمال پاکسازی‌های خاص پروتکل قبل از اعتبارسنجی
            if config_candidate.startswith("vmess://"):
                config_candidate = ConfigValidator.clean_vmess_config(config_candidate)
            elif config_candidate.startswith("hy2://"):
                config_candidate = ConfigValidator.normalize_hysteria2_protocol(config_candidate)
            elif config_candidate.startswith("ssr://"):
                config_candidate = ConfigValidator.clean_ssr_config(config_candidate)
            elif config_candidate.startswith("hy1://"):
                config_candidate = config_candidate.replace('hy1://', 'hysteria://', 1) 
            
            # پاکسازی عمومی کاراکترها
            clean_config = ConfigValidator.clean_config(config_candidate)
            
            # اعتبارسنجی نهایی قبل از اضافه کردن
            if ConfigValidator.is_valid_config(clean_config): # is_valid_config فقط شروع پروتکل را چک می‌کند
                # برای اطمینان بیشتر، می‌توانیم validate_protocol_config را هم اینجا فراخوانی کنیم
                # اما این باعث کندی می‌شود. بهتر است این کار در ConfigProcessor انجام شود.
                configs.append(clean_config)

        return configs


    @staticmethod
    def clean_config(config: str) -> str:
        """پاکسازی یک رشته کانفیگ از کاراکترهای نامرئی، ایموجی‌ها و فضاهای اضافی."""
        # حذف ایموجی‌ها
        config = re.sub(r'[\U0001F300-\U0001F9FF\U00002600-\U000027BF]', '', config, flags=re.UNICODE)
        # حذف کاراکترهای کنترلی (نامرئی)
        config = re.sub(r'[\x00-\x08\x0B-\x1F\x7F-\x9F]', '', config)
        # جایگزینی بیش از یک فضای خالی با یک فضای خالی تکی
        config = re.sub(r'[^\S\r\n]+', ' ', config)
        # حذف فضای خالی در ابتدا و انتهای رشته
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
            # نرمال‌سازی پروتکل‌های مستعار برای بررسی دقیق
            if protocol == 'hy2://':
                protocol = 'hysteria2://'
            elif protocol == 'hy1://':
                protocol = 'hysteria://'

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
            elif protocol == 'wireguard://':
                return cls.is_wireguard_config(config)
            elif protocol in ['vless://', 'trojan://', 'hysteria2://']:
                # برای این پروتکل‌ها، بررسی URLparse برای hostname و port کافی است.
                parsed = urlparse(config)
                return bool(parsed.netloc and parsed.port)
            elif protocol == 'ss://':
                parts = config.replace('ss://', '').split('@')
                if len(parts) == 2:
                    try:
                        # بخش method:password باید Base64 معتبر باشد
                        base64.b64decode(parts[0].replace('-', '+').replace('_', '/')).decode('utf-8')
                        # بخش server:port هم باید وجود داشته باشد
                        server_part = parts[1].split('#')[0]
                        return ':' in server_part and server_part.split(':')[0] and server_part.split(':')[1].isdigit()
                    except:
                        pass
                return False
            elif protocol == 'ssconf://':
                return True # ssconf:// یک لینک مستقیم نیست، فقط یک URL است
            return False
        except Exception as e:
            logger.debug(f"خطا در اعتبارسنجی پروتکل '{protocol}' برای کانفیگ '{config[:min(len(config), 50)]}...': {str(e)}")
            return False

    @staticmethod
    def extract_telegram_channels_from_config(config_string: str) -> List[str]:
        """کانال‌های تلگرام را از داخل مشخصات یک کانفیگ (مانند تگ‌ها یا پارامترهای خاص) استخراج می‌کند."""
        found_channels = set()

        telegram_url_pattern = r"(?:https?://)?t\.me/(?:s/)?([a-zA-Z0-9_]+)"

        try: # افزودن try-except برای مدیریت خطاهای urlparse (مثلا Invalid IPv6 URL)
            parsed_url = urlparse(config_string)

            # جستجو در fragment (#tag)
            if parsed_url.fragment:
                matches = re.findall(telegram_url_pattern, unquote(parsed_url.fragment))
                for match in matches:
                    found_channels.add(f"https://t.me/s/{match}")

            # جستجو در query parameters (?key=value)
            if parsed_url.query:
                query_params = parse_qs(parsed_url.query)
                for key, values in query_params.items():
                    for value in values:
                        matches = re.findall(telegram_url_pattern, unquote(value))
                        for match in matches:
                            found_channels.add(f"https://t.me/s/{match}")

            # جستجو در جزئیات پروتکل خاص (VMess, SSR, Hysteria)
            if config_string.startswith('vmess://'):
                try:
                    base64_part = config_string[len('vmess://'):]
                    decoded_vmess = ConfigValidator.decode_base64_url(base64_part)
                    if decoded_vmess:
                        vmess_data = json.loads(decoded_vmess.decode('utf-8'))
                        potential_fields = [vmess_data.get('ps'), vmess_data.get('id')] # 'ps' for pseudo-name, 'id' for UUID
                        for field_value in potential_fields:
                            if isinstance(field_value, str):
                                matches = re.findall(telegram_url_pattern, field_value)
                                for match in matches:
                                    found_channels.add(f"https://t.me/s/{match}")
                except Exception:
                    pass
            elif config_string.startswith('ssr://'):
                try:
                    base64_part = config_string[len('ssr://'):].split('/?')[0] # SSR Base64 part is before query or fragment
                    decoded_ssr = ConfigValidator.decode_base64_url(base64_part)
                    if decoded_ssr:
                        full_ssr_params = decoded_ssr.decode('utf-8', errors='ignore')

                        # SSR also has a fragment part that can contain remarks
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
            elif config_string.startswith('hysteria://') or config_string.startswith('hysteria2://'):
                try:
                    parsed_hy = urlparse(config_string)
                    if parsed_hy.fragment:
                        matches = re.findall(telegram_url_pattern, unquote(parsed_hy.fragment))
                        for match in matches:
                            found_channels.add(f"https://t.me/s/{match}")
                    query_params = parse_qs(parsed_hy.query)
                    if 'password' in query_params: # Hysteria can have password in query
                        matches = re.findall(telegram_url_pattern, query_params['password'][0])
                        for match in matches:
                            found_channels.add(f"https://t.me/s/{match}")
                except Exception:
                    pass

            # جستجوی عمومی در کل رشته کانفیگ به عنوان آخرین چاره
            matches = re.findall(telegram_url_pattern, config_string)
            for match in matches:
                found_channels.add(f"https://t.me/s/{match}")
        except ValueError as e:
            logger.debug(f"خطا در تجزیه URL کانفیگ برای استخراج کانال تلگرام: '{config_string[:min(len(config_string), 50)]}...' - {str(e)}")
        except Exception as e:
            logger.debug(f"خطای ناشناخته در استخراج کانال تلگرام از کانفیگ: '{config_string[:min(len(config_string), 50)]}...' - {str(e)}")


        return list(found_channels)

    @staticmethod
    def get_server_address(config_string: str, protocol_prefix: str) -> Optional[str]:
        """
        استخراج آدرس سرور (هاست‌نام یا IP) از یک رشته کانفیگ با توجه به پروتکل آن.
        """
        try:
            # نرمال‌سازی پروتکل برای استخراج آدرس سرور
            if protocol_prefix == 'hy2://':
                protocol_prefix = 'hysteria2://'
            elif protocol_prefix == 'hy1://':
                protocol_prefix = 'hysteria://'

            parsed_url = urlparse(config_string)

            if protocol_prefix == 'vmess://':
                decoded_data = base64.b64decode(config_string[len(protocol_prefix):]).decode('utf-8')
                vmess_json = json.loads(decoded_data)
                return vmess_json.get('add') # 'add' فیلد آدرس سرور در VMess
            elif protocol_prefix in ['vless://', 'trojan://', 'hysteria2://', 'tuic://', 'mieru://', 'snell://', 'anytls://', 'juicity://', 'hysteria://', 'ssh://']:
                return parsed_url.hostname
            elif protocol_prefix == 'ss://':
                parts = config_string.replace('ss://', '').split('@')
                if len(parts) == 2:
                    server_part = parts[1].split('#')[0] # بخش سرور:پورت قبل از fragment
                    return server_part.split(':')[0] # فقط سرور
            elif protocol_prefix == 'ssr://':
                base64_part = config_string[len(protocol_prefix):].split('/?')[0].split('#')[0]
                decoded_params = ConfigValidator.decode_base64_url(base64_part)
                if decoded_params:
                    # SSR parameters: server:port:protocol:method:obfs:password_base64
                    return decoded_params.decode('utf-8', errors='ignore').split(':')[0]
                return None
            elif protocol_prefix == 'wireguard://':
                # WireGuard format: wireguard://<public_key>@<endpoint>/?key=value
                endpoint_match = re.search(r'@([^/?#]+)', config_string)
                if endpoint_match:
                    endpoint = endpoint_match.group(1)
                    return endpoint.split(':')[0] # فقط هاست از endpoint
            elif protocol_prefix == 'warp://':
                # WARP معمولاً دارای یک IP پیش‌فرض یا سرور خاص است
                if parsed_url.hostname:
                    return parsed_url.hostname
                return "162.159.192.1" # Cloudflare Anycast IP
            elif protocol_prefix == 'ssconf://':
                # ssconf:// یک URL برای واکشی کانفیگ‌ها است، نه خود کانفیگ پراکسی
                # بنابراین آدرس سرور مستقیم ندارد
                return None 

            return None
        except Exception:
            logger.debug(f"خطا در استخراج آدرس سرور برای کانفیگ '{config_string[:min(len(config_string), 50)]}...' با پروتکل '{protocol_prefix}'.", exc_info=True)
            return None

    @staticmethod
    def get_canonical_parameters(config_string: str, protocol_prefix: str) -> Optional[Dict[str, Any]]:
        """
        استخراج پارامترهای "کانونی" (اصلی و عملکردی) یک کانفیگ برای شناسایی تکراری‌ها.
        این تابع بخش‌های غیرعملکردی مانند remark/tag را نادیده می‌گیرد و خروجی را دقیقاً کنترل می‌کند.
        """
        # logger.debug(f"در حال دریافت پارامترهای کانونی برای کانفیگ: '{config_string[:min(len(config_string), 50)]}...'")
        try: 
            # نرمال‌سازی پروتکل برای استخراج پارامترها
            if protocol_prefix == 'hy2://':
                protocol_prefix = 'hysteria2://'
            elif protocol_prefix == 'hy1://':
                protocol_prefix = 'hysteria://'

            parsed_url = urlparse(config_string)
        except ValueError as e:
            logger.debug(f"خطا در تجزیه URL برای دریافت پارامترهای کانونی: '{config_string[:50]}...' - {str(e)}", exc_info=True) 
            return None
        except Exception as e:
            logger.debug(f"خطای ناشناخته در دریافت پارامترهای کانونی: '{config_string[:50]}...' - {str(e)}", exc_info=True) 
            return None

        canonical_params: Dict[str, Any] = {"protocol": protocol_prefix}

        try:
            if protocol_prefix == 'vmess://':
                decoded_data = base64.b64decode(config_string[len(protocol_prefix):]).decode('utf-8')
                vmess_json = json.loads(decoded_data)
                canonical_params.update({
                    'server': vmess_json.get('add', ''),
                    'port': str(vmess_json.get('port', '')), 
                    'uuid': vmess_json.get('id', ''),
                    'security': vmess_json.get('scy', ''), # network security (tls, auto, none)
                    'net': vmess_json.get('net', ''),     # network type (tcp, ws, http, ...)
                    'type': vmess_json.get('type', ''),   # (http, s_path, quic, ...)
                    'host': vmess_json.get('host', ''),   # host header
                    'path': vmess_json.get('path', ''),
                    'tls': 'tls' if vmess_json.get('tls') == 'tls' else '' 
                })
            elif protocol_prefix == 'vless://':
                # VLESS UUID در بخش username است
                query_params_dict = parse_qs(parsed_url.query) 
                canonical_params.update({
                    'server': parsed_url.hostname or '',
                    'port': str(parsed_url.port or ''),
                    'uuid': parsed_url.username or '',
                    'flow': query_params_dict.get('flow', [''])[0],
                    'security': query_params_dict.get('security', [''])[0], # reality, tls, none
                    'sni': query_params_dict.get('sni', [parsed_url.hostname or ''])[0],
                    'type': query_params_dict.get('type', [''])[0], # network type (tcp, ws, grpc, ...)
                    'host': query_params_dict.get('host', [''])[0],
                    'path': query_params_dict.get('path', [''])[0],
                    'encryption': query_params_dict.get('encryption', [''])[0] # "none"
                })
            elif protocol_prefix == 'trojan://':
                # Trojan password در بخش username است
                query_params_dict = parse_qs(parsed_url.query)
                canonical_params.update({
                    'server': parsed_url.hostname or '',
                    'port': str(parsed_url.port or ''),
                    'password': parsed_url.username or '', # Password
                    'sni': query_params_dict.get('sni', [parsed_url.hostname or ''])[0],
                    'alpn': query_params_dict.get('alpn', [''])[0],
                    'type': query_params_dict.get('type', [''])[0], # (ws, grpc)
                    'path': query_params_dict.get('path', [''])[0],
                    'host': query_params_dict.get('host', [''])[0]
                })
            elif protocol_prefix in ['hysteria2://', 'hysteria://']:
                query_params_dict = parse_qs(parsed_url.query)
                canonical_params.update({
                    'server': parsed_url.hostname or '',
                    'port': str(parsed_url.port or ''),
                    # Hysteria pass can be in username part or 'auth' or 'password' query param
                    'password': parsed_url.username or query_params_dict.get('auth', [''])[0] or query_params_dict.get('password', [''])[0],
                    'sni': query_params_dict.get('sni', [parsed_url.hostname or ''])[0],
                    'alpn': query_params_dict.get('alpn', [''])[0],
                    'obfs': query_params_dict.get('obfs', [''])[0] # obfuscation
                })
            elif protocol_prefix == 'ss://':
                parts = config_string.replace('ss://', '').split('@')
                if len(parts) == 2:
                    method_pass_encoded = parts[0]
                    # SS method:password part is Base64 (URL-safe sometimes)
                    decoded_method_pass = base64.b64decode(method_pass_encoded.replace('-', '+').replace('_', '/')).decode('utf-8', errors='ignore')
                    method, password = (decoded_method_pass.split(':', 1) + [''])[:2] 
                    server_port_part = parts[1].split('#')[0] # Remove fragment if exists
                    server, port = (server_port_part.split(':', 1) + [''])[:2]
                    canonical_params.update({
                        'server': server or '',
                        'port': str(port or ''),
                        'method': method or '',
                        'password': password or ''
                    })
            elif protocol_prefix == 'ssr://':
                base64_part = config_string[len(protocol_prefix):].split('/?')[0].split('#')[0] # SSR Base64 part is before query or fragment
                decoded_params_str = ConfigValidator.decode_base64_url(base64_part).decode('utf-8', errors='ignore')
                # SSR parameters: server:port:protocol:method:obfs:password_base64/?params
                ssr_parts = decoded_params_str.split(':')
                if len(ssr_parts) >= 6:
                    password_encoded = ssr_parts[5]
                    # SSR password itself is Base64 encoded, usually URL-safe
                    password = base64.b64decode(password_encoded.replace('-', '+').replace('_', '/')).decode('utf-8', errors='ignore')
                    canonical_params.update({
                        'server': ssr_parts[0] or '',
                        'port': str(ssr_parts[1] or ''),
                        'protocol_type': ssr_parts[2] or '', # protocol plugin
                        'method': ssr_parts[3] or '',
                        'obfs': ssr_parts[4] or '', # obfuscation plugin
                        'password': password or ''
                    })
            elif protocol_prefix == 'tuic://':
                query_params_dict = parse_qs(parsed_url.query)
                canonical_params.update({
                    'server': parsed_url.hostname or '',
                    'port': str(parsed_url.port or ''),
                    'uuid': parsed_url.username or '', # UUID in username
                    'password': query_params_dict.get('password', [''])[0],
                    'alpn': query_params_dict.get('alpn', [''])[0],
                    'congestion_control': query_params_dict.get('congestion_control', [''])[0],
                    'udp_relay_mode': query_params_dict.get('udp_relay_mode', [''])[0],
                    'disable_sni': query_params_dict.get('disable_sni', [''])[0],
                    'tls': 'tls' if parsed_url.scheme.endswith('+tls') else '' # Example: tuic+tls://
                })
            elif protocol_prefix == 'mieru://':
                query_params_dict = parse_qs(parsed_url.query)
                canonical_params.update({
                    'uuid': parsed_url.username or '',
                    'server': parsed_url.hostname or '',
                    'port': str(parsed_url.port or ''),
                    'tls': query_params_dict.get('tls', [''])[0],
                    'udp_over_tcp': query_params_dict.get('udp_over_tcp', [''])[0],
                    'peer_fingerprint': query_params_dict.get('peer_fingerprint', [''])[0],
                    'server_name': query_params_dict.get('server_name', [''])[0],
                })
            elif protocol_prefix == 'snell://':
                query_params_dict = parse_qs(parsed_url.query)
                canonical_params.update({
                    'server': parsed_url.hostname or '',
                    'port': str(parsed_url.port or ''),
                    'psk': query_params_dict.get('psk', [''])[0], # Pre-shared Key
                    'version': query_params_dict.get('version', [''])[0],
                    'obfs': query_params_dict.get('obfs', [''])[0], # Obfuscation type
                    'obfs_uri': query_params_dict.get('uri', [''])[0], # Obfuscation URI
                    'tfo': query_params_dict.get('tfo', [''])[0] # TCP Fast Open
                })
            elif protocol_prefix == 'anytls://':
                query_params_dict = parse_qs(parsed_url.query)
                canonical_params.update({
                    'protocol_type': parsed_url.username or '', # e.g., 'ws', 'grpc'
                    'server': parsed_url.hostname or '',
                    'port': str(parsed_url.port or ''),
                    'sni': query_params_dict.get('sni', [''])[0],
                    'path': query_params_dict.get('path', [''])[0],
                    'host': query_params_dict.get('host', [''])[0],
                    'tls': query_params_dict.get('tls', [''])[0]
                })
            elif protocol_prefix == 'ssh://':
                # SSH format: ssh://user:pass@host:port
                canonical_params.update({
                    'user': parsed_url.username or '',
                    'password': parsed_url.password or '',
                    'server': parsed_url.hostname or '',
                    'port': str(parsed_url.port or '')
                })
            elif protocol_prefix == 'juicity://':
                query_params_dict = parse_qs(parsed_url.query)
                canonical_params.update({
                    'uuid': parsed_url.username or '',
                    'server': parsed_url.hostname or '',
                    'port': str(parsed_url.port or ''),
                    'password': query_params_dict.get('password', [''])[0],
                    'security': query_params_dict.get('security', [''])[0], # TLS type
                    'fingerprint': query_params_dict.get('fingerprint', [''])[0],
                    'congestion_control': query_params_dict.get('congestion_control', [''])[0],
                    'alpn': query_params_dict.get('alpn', [''])[0]
                })
            elif protocol_prefix == 'wireguard://':
                # WireGuard format: wireguard://<public_key>@<endpoint>/?key=value
                endpoint_match = re.search(r'@([^/?#]+)', config_string)
                query_params_dict = parse_qs(parsed_url.query)
                canonical_params.update({
                    'public_key': parsed_url.username or '',
                    'endpoint': (endpoint_match.group(1) if endpoint_match else ''),
                    'private_key': query_params_dict.get('privatekey', [''])[0] # Note: privatekey for uniqueness
                })
            elif protocol_prefix == 'warp://':
                # WARP has no standard canonical parameters beyond its type and potentially account ID
                # UUID can sometimes be embedded in the URL path or fragment for specific WARP clients/scripts.
                # For basic WARP, server is often Cloudflare's default anycast IP.
                canonical_params.update({
                    'server': parsed_url.hostname or '162.159.192.1', 
                    'uuid': parsed_url.username or '' # Sometimes used for account ID in scripts
                })
            elif protocol_prefix == 'ssconf://':
                # ssconf:// is a subscription URL, not a proxy config itself.
                # It doesn't have canonical proxy parameters.
                return None 
            else: 
                logger.debug(f"پروتکل ناشناخته برای پارامترهای کانونی: '{protocol_prefix}'. کانفیگ: '{config_string[:min(len(config_string), 50)]}...'.")
                return None

            # حذف مقادیر None یا رشته‌های خالی از canonical_params برای اطمینان از یکپارچگی (حالا کمتر لازم است)
            # canonical_params = {k: v for k, v in canonical_params.items() if v is not None and v != ''}
            # logger.debug(f"پارامترهای کانونی استخراج شده: {canonical_params}") # این لاگ برای حجم زیاد، زیاد است
            return canonical_params
        except Exception as e: 
            logger.debug(f"خطا در استخراج پارامترهای کانونی برای '{config_string[:min(len(config_string), 50)]}...': {str(e)}", exc_info=True)
            return None

    @staticmethod
    def get_canonical_id(config_string: str, protocol_prefix: str) -> Optional[str]:
        """
        تولید یک رشته شناسه کانونی (Canonical ID) برای یک کانفیگ.
        این شناسه برای شناسایی دقیق کانفیگ‌های تکراری استفاده می‌شود.
        """
        canonical_params = ConfigValidator.get_canonical_parameters(config_string, protocol_prefix)
        if not canonical_params:
            logger.debug(f"تولید شناسه کانونی برای پروتکل '{protocol_prefix}' ناموفق بود: پارامترها خالی هستند یا استخراج نشدند.")
            return None

        # Sort the items to ensure consistent ID regardless of dictionary key order
        sorted_canonical_params = sorted(canonical_params.items())
        # Use json.dumps to convert the sorted list of tuples into a string ID
        canonical_id = json.dumps(sorted_canonical_params, sort_keys=True) # sort_keys=True ensures consistent order for JSON string
        # logger.debug(f"شناسه کانونی تولید شده برای '{protocol_prefix}': {canonical_id[:min(len(canonical_id), 100)]}...") # این لاگ برای حجم زیاد، زیاد است
        return canonical_id

