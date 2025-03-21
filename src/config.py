from typing import Dict, List, Optional
from datetime import datetime
import re
from urllib.parse import urlparse
from dataclasses import dataclass
import logging
from math import inf

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class ChannelMetrics:
    total_configs: int = 0
    valid_configs: int = 0
    unique_configs: int = 0
    avg_response_time: float = 0
    last_success_time: Optional[datetime] = None
    fail_count: int = 0
    success_count: int = 0
    overall_score: float = 0.0
    protocol_counts: Dict[str, int] = None

    def __post_init__(self):
        if self.protocol_counts is None:
            self.protocol_counts = {}

class ChannelConfig:
    def __init__(self, url: str, enabled: bool = True):
        self.url = self._validate_url(url)
        self.enabled = enabled
        self.metrics = ChannelMetrics()
        self.is_telegram = bool(re.match(r'^https://t\.me/s/', self.url))
        self.error_count = 0
        self.last_check_time = None
        
    def _validate_url(self, url: str) -> str:
        if not url or not isinstance(url, str):
            raise ValueError("Invalid URL")
        url = url.strip()
        if not url.startswith(('http://', 'https://', 'ssconf://')):
            raise ValueError("Invalid URL protocol")
        return url
        
    def calculate_overall_score(self):
        try:
            total_attempts = max(1, self.metrics.success_count + self.metrics.fail_count)
            reliability_score = (self.metrics.success_count / total_attempts) * 35
            
            total_configs = max(1, self.metrics.total_configs)
            quality_score = (self.metrics.valid_configs / total_configs) * 25
            
            valid_configs = max(1, self.metrics.valid_configs)
            uniqueness_score = (self.metrics.unique_configs / valid_configs) * 25
            
            response_score = 15
            if self.metrics.avg_response_time > 0:
                response_score = max(0, min(15, 15 * (1 - (self.metrics.avg_response_time / 10))))
            
            self.metrics.overall_score = round(reliability_score + quality_score + uniqueness_score + response_score, 2)
        except Exception as e:
            logger.error(f"Error calculating score for {self.url}: {str(e)}")
            self.metrics.overall_score = 0.0

class ProxyConfig:
    def __init__(self):
        # User Configuration Mode
        # Option 1: Set use_maximum_power = True for maximum possible configs (Highest Priority)
        # Option 2: Set specific_config_count > 0 for desired number of configs (Default: 50)
        # Note: If use_maximum_power is True, specific_config_count will be ignored
        self.use_maximum_power = False
        self.specific_config_count = 200

        initial_urls = [
            ChannelConfig("https://raw.githubusercontent.com/4n0nymou3/wg-config-fetcher/refs/heads/main/configs/wireguard_configs.txt"),
            ChannelConfig("https://raw.githubusercontent.com/4n0nymou3/ss-config-updater/refs/heads/main/configs.txt"),
            ChannelConfig("https://raw.githubusercontent.com/valid7996/Gozargah/refs/heads/main/Gozargah_Sub"),
            ChannelConfig("https://raw.githubusercontent.com/soroushmirzaei/telegram-configs-collector/main/channels/protocols/hysteria"),
            ChannelConfig("https://raw.githubusercontent.com/mahsanet/MahsaFreeConfig/main/mci/sub_1.txt"),
            ChannelConfig("https://raw.githubusercontent.com/mahsanet/MahsaFreeConfig/main/mci/sub_2.txt"),
            ChannelConfig("https://raw.githubusercontent.com/mahsanet/MahsaFreeConfig/main/mci/sub_3.txt"),
            ChannelConfig("https://raw.githubusercontent.com/mahsanet/MahsaFreeConfig/main/mci/sub_4.txt"),
            ChannelConfig("https://raw.githubusercontent.com/mahsanet/MahsaFreeConfig/main/mtn/sub_1.txt"),
            ChannelConfig("https://raw.githubusercontent.com/mahsanet/MahsaFreeConfig/main/mtn/sub_2.txt"),
            ChannelConfig("https://raw.githubusercontent.com/mahsanet/MahsaFreeConfig/main/mtn/sub_3.txt"),
            ChannelConfig("https://raw.githubusercontent.com/mahsanet/MahsaFreeConfig/main/mtn/sub_4.txt"),
            ChannelConfig("https://t.me/s/FreeV2rays"),
            ChannelConfig("https://t.me/s/v2ray_free_conf"),
            ChannelConfig("https://t.me/s/PrivateVPNs"),
            ChannelConfig("https://t.me/s/IP_CF_Config"),
            ChannelConfig("https://t.me/s/shadowproxy66"),
            ChannelConfig("https://t.me/s/OutlineReleasedKey"),
            ChannelConfig("https://t.me/s/prrofile_purple"),
            ChannelConfig("https://t.me/s/proxy_shadosocks"),
            ChannelConfig("https://t.me/s/meli_proxyy"),
            ChannelConfig("https://t.me/s/DirectVPN"),
            ChannelConfig("https://t.me/s/VmessProtocol"),
            ChannelConfig("https://t.me/s/ViProxys"),
            ChannelConfig("https://t.me/s/heyatserver"),
            ChannelConfig("https://t.me/s/vpnfail_vless"),
            ChannelConfig("https://t.me/s/DailyV2RY"),
            ChannelConfig("https://t.me/s/ShadowsocksM"),
            ChannelConfig("https://t.me/s/V2ray_Alpha"),
            ChannelConfig("https://t.me/s/VlessConfig"),
            ChannelConfig("https://t.me/s/v2rayngvpn"),
            ChannelConfig("https://t.me/s/An0nymousTeam"),
            ChannelConfig("https://t.me/s/oneclickvpnkeys"),
            ChannelConfig("https://t.me/s/CaV2ray"),
            ChannelConfig("https://t.me/s/ELiV2RAY"),
            ChannelConfig("https://t.me/s/MehradLearn"),
            ChannelConfig("https://t.me/s/Outline_Vpn"),
            ChannelConfig("https://t.me/s/SafeNet_Server"),
            ChannelConfig("https://t.me/s/VPN_KING_V2RAY"),
            ChannelConfig("https://t.me/s/ipV2Ray"),
            ChannelConfig("https://t.me/s/lrnbymaa"),
            ChannelConfig("https://t.me/s/proxy_kafee"),
            ChannelConfig("https://t.me/s/proxy_mtm"),
            ChannelConfig("https://t.me/s/v2rayNG_VPNN"),
            ChannelConfig("https://t.me/s/tv2rayrr"),
            ChannelConfig("https://t.me/s/V2rey_Hiddify"),
            ChannelConfig("https://t.me/s/server_nekobox"),
            ChannelConfig("https://t.me/s/V2rayNGn"),
            ChannelConfig("https://t.me/s/v2raying"),
            ChannelConfig("https://t.me/s/s0013_Official"),
            ChannelConfig("https://t.me/s/V2RAYROZ"),
            ChannelConfig("https://t.me/s/V2rayNG_Outlinee"),
            ChannelConfig("https://t.me/s/sinavm"),
            ChannelConfig("https://t.me/s/customv2ray"),
            ChannelConfig("https://t.me/s/fastkanfig"),
            ChannelConfig("https://t.me/s/vpnstorefast"),
            ChannelConfig("https://t.me/s/Hope_Net"),
            ChannelConfig("https://t.me/s/oxnet_ir"),
            ChannelConfig("https://t.me/s/CryptoGuardVPN"),
            ChannelConfig("https://t.me/s/freeiranianv2rey"),
            ChannelConfig("https://t.me/s/ernoxin_shop"),
            ChannelConfig("https://t.me/s/DarkVPNpro"),
            ChannelConfig("https://t.me/s/Free_Vpn_for_All_of_Us"),
            ChannelConfig("https://t.me/s/NoForcedHeaven"),
            ChannelConfig("https://t.me/s/v2raybaz"),
            ChannelConfig("https://t.me/s/freedomofinfor"),
            ChannelConfig("https://t.me/s/Vpnstable"),
            ChannelConfig("https://t.me/s/orange_vpns"),
            ChannelConfig("https://t.me/s/amirtronic"),
            ChannelConfig("https://t.me/s/kb_v2ray_store"),
            ChannelConfig("https://t.me/s/FreeVPNHomesConfigs"),
            ChannelConfig("https://t.me/s/Ahmedhamoomi_Servers"),
            ChannelConfig("https://t.me/s/V2ray_nima02"),
            ChannelConfig("https://t.me/s/ISVvpn"),
            ChannelConfig("https://t.me/s/proSSH"),
            ChannelConfig("https://t.me/s/v2ray_hub1"),
            ChannelConfig("https://t.me/s/GrizzlyVPN"),
            ChannelConfig("https://t.me/s/BESTFORBEST66"),
            ChannelConfig("https://t.me/s/curetech"),
            ChannelConfig("https://t.me/s/v2Source"),
            ChannelConfig("https://t.me/s/GetConfigIR"),
            ChannelConfig("https://t.me/s/Parsashonam"),
            ChannelConfig("https://t.me/s/ArV2ray"),
            ChannelConfig("https://t.me/s/configfa"),
            ChannelConfig("https://t.me/s/Change_IP1"),
            ChannelConfig("https://t.me/s/VPNCUSTOMIZE"),
            ChannelConfig("https://t.me/s/Suevpnx"),
            ChannelConfig("https://t.me/s/Nfastvpnn"),
            ChannelConfig("https://t.me/s/napsternetv"),
            ChannelConfig("https://t.me/s/Alpha_V2ray_Iran"),
            ChannelConfig("https://t.me/s/vpn_proxy666"),
            ChannelConfig("https://t.me/s/xsfilternet"),
            ChannelConfig("https://t.me/s/Hiidify_V2ray"),
            ChannelConfig("https://t.me/s/ConfigWireguard"),
            ChannelConfig("https://t.me/s/WireVpnGuard"),
            ChannelConfig("https://t.me/s/LonUp_M"),
            ChannelConfig("https://t.me/s/ConfigV2rayNG"),
            ChannelConfig("https://t.me/s/V2ray_Tunnel_Plus"),
            ChannelConfig("https://t.me/s/CookVip"),
        ]

        self.SOURCE_URLS = self._remove_duplicate_urls(initial_urls)
        self.SUPPORTED_PROTOCOLS = self._initialize_protocols()
        self._initialize_settings()
        self._set_smart_limits()

    def _initialize_protocols(self) -> Dict:
        return {
            "wireguard://": {"priority": 2, "aliases": [], "enabled": True},
            "hysteria2://": {"priority": 2, "aliases": ["hy2://"], "enabled": True},
            "vless://": {"priority": 2, "aliases": [], "enabled": True},
            "vmess://": {"priority": 1, "aliases": [], "enabled": True},
            "ss://": {"priority": 2, "aliases": [], "enabled": True},
            "trojan://": {"priority": 2, "aliases": [], "enabled": True},
            "tuic://": {"priority": 1, "aliases": [], "enabled": True}
        }

    def _initialize_settings(self):
        self.MAX_CONFIG_AGE_DAYS = min(30, max(1, 7))
        self.CHANNEL_RETRY_LIMIT = min(10, max(1, 5))
        self.CHANNEL_ERROR_THRESHOLD = min(0.9, max(0.1, 0.7))
        self.OUTPUT_FILE = 'configs/proxy_configs.txt'
        self.STATS_FILE = 'configs/channel_stats.json'
        self.MAX_RETRIES = min(10, max(1, 5))
        self.RETRY_DELAY = min(60, max(5, 15))
        self.REQUEST_TIMEOUT = min(120, max(10, 60))
        
        self.HEADERS = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }

    def _set_smart_limits(self):
        if self.use_maximum_power:
            self._set_maximum_power_mode()
        else:
            self._set_specific_count_mode()

    def _set_maximum_power_mode(self):
        max_configs = 10000
        
        for protocol in self.SUPPORTED_PROTOCOLS:
            self.SUPPORTED_PROTOCOLS[protocol].update({
                "min_configs": 1,
                "max_configs": max_configs,
                "flexible_max": True
            })
        
        self.MIN_CONFIGS_PER_CHANNEL = 1
        self.MAX_CONFIGS_PER_CHANNEL = max_configs
        self.MAX_RETRIES = min(10, max(1, 10))
        self.CHANNEL_RETRY_LIMIT = min(10, max(1, 10))
        self.REQUEST_TIMEOUT = min(120, max(30, 90))

    def _set_specific_count_mode(self):
        if self.specific_config_count <= 0:
            self.specific_config_count = 50
        
        protocols_count = len(self.SUPPORTED_PROTOCOLS)
        base_per_protocol = max(1, self.specific_config_count // protocols_count)
        
        for protocol in self.SUPPORTED_PROTOCOLS:
            self.SUPPORTED_PROTOCOLS[protocol].update({
                "min_configs": 1,
                "max_configs": min(base_per_protocol * 2, 1000),
                "flexible_max": True
            })
        
        self.MIN_CONFIGS_PER_CHANNEL = 1
        self.MAX_CONFIGS_PER_CHANNEL = min(max(5, self.specific_config_count // 2), 1000)

    def _normalize_url(self, url: str) -> str:
        try:
            if not url:
                raise ValueError("Empty URL")
                
            url = url.strip()
            if url.startswith('ssconf://'):
                url = url.replace('ssconf://', 'https://', 1)
                
            parsed = urlparse(url)
            if not parsed.scheme or not parsed.netloc:
                raise ValueError("Invalid URL format")
                
            path = parsed.path.rstrip('/')
            
            if parsed.netloc.startswith('t.me/s/'):
                channel_name = parsed.path.strip('/').lower()
                return f"telegram:{channel_name}"
                
            return f"{parsed.scheme}://{parsed.netloc}{path}"
        except Exception as e:
            logger.error(f"URL normalization error: {str(e)}")
            raise

    def _remove_duplicate_urls(self, channel_configs: List[ChannelConfig]) -> List[ChannelConfig]:
        try:
            seen_urls = {}
            unique_configs = []
            
            for config in channel_configs:
                if not isinstance(config, ChannelConfig):
                    logger.warning(f"Invalid config skipped: {config}")
                    continue
                    
                try:
                    normalized_url = self._normalize_url(config.url)
                    if normalized_url not in seen_urls:
                        seen_urls[normalized_url] = True
                        unique_configs.append(config)
                except Exception:
                    continue
            
            if not unique_configs:
                self.save_empty_config_file()
                logger.error("No valid sources found. Empty config file created.")
                return []
                
            return unique_configs
        except Exception as e:
            logger.error(f"Error removing duplicate URLs: {str(e)}")
            self.save_empty_config_file()
            return []

    def is_protocol_enabled(self, protocol: str) -> bool:
        try:
            if not protocol:
                return False
                
            protocol = protocol.lower().strip()
            
            if protocol in self.SUPPORTED_PROTOCOLS:
                return self.SUPPORTED_PROTOCOLS[protocol].get("enabled", False)
                
            for main_protocol, info in self.SUPPORTED_PROTOCOLS.items():
                if protocol in info.get("aliases", []):
                    return info.get("enabled", False)
                    
            return False
        except Exception:
            return False

    def get_enabled_channels(self) -> List[ChannelConfig]:
        channels = [channel for channel in self.SOURCE_URLS if channel.enabled]
        if not channels:
            self.save_empty_config_file()
            logger.error("No enabled channels found. Empty config file created.")
        return channels

    def update_channel_stats(self, channel: ChannelConfig, success: bool, response_time: float = 0):
        if success:
            channel.metrics.success_count += 1
            channel.metrics.last_success_time = datetime.now()
        else:
            channel.metrics.fail_count += 1
        
        if response_time > 0:
            if channel.metrics.avg_response_time == 0:
                channel.metrics.avg_response_time = response_time
            else:
                channel.metrics.avg_response_time = (channel.metrics.avg_response_time * 0.7) + (response_time * 0.3)
        
        channel.calculate_overall_score()
        
        if channel.metrics.overall_score < 25:
            channel.enabled = False
        
        if not any(c.enabled for c in self.SOURCE_URLS):
            self.save_empty_config_file()
            logger.error("All channels are disabled. Empty config file created.")

    def adjust_protocol_limits(self, channel: ChannelConfig):
        if self.use_maximum_power:
            return
            
        for protocol in channel.metrics.protocol_counts:
            if protocol in self.SUPPORTED_PROTOCOLS:
                current_count = channel.metrics.protocol_counts[protocol]
                if current_count > 0:
                    self.SUPPORTED_PROTOCOLS[protocol]["min_configs"] = min(
                        self.SUPPORTED_PROTOCOLS[protocol]["min_configs"],
                        current_count
                    )

    def save_empty_config_file(self) -> bool:
        try:
            with open(self.OUTPUT_FILE, 'w', encoding='utf-8') as f:
                f.write("")
            return True
        except Exception:
            return False
