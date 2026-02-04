import os
import gzip
import re
import time
import logging
from typing import List, Dict, Set, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from lxml import etree
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ===================== 配置区 =====================
CONFIG_FILE = "config.txt"
OUTPUT_DIR = "output"
LOG_FILE = "epg_merge.log"
MAX_WORKERS = 3  # 并发线程数（可根据需求调整）
TIMEOUT = 30
CORE_RETRY_COUNT = 2

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# 核心频道配置（山东本地→央视→其他卫视优先级）
CHANNEL_PRIORITY = [
    ("山东本地", ["山东"]),
    ("央视", ["CCTV"]),
    ("其他卫视", ["卫视", "浙江", "湖南", "江苏", "东方", "北京", "安徽", "广东", "河南", "深圳"])
]

# 酷9专用ID映射表（数字ID→名称ID，含完整4K频道）
COOL9_ID_MAPPING = {
    # 山东本地频道
    "89": "山东卫视", "221": "山东教育", "381": "山东新闻", 
    "382": "山东农科", "383": "山东齐鲁", "384": "山东文旅",
    # 央视常规频道
    "1": "CCTV1", "2": "CCTV2", "3": "CCTV3", "4": "CCTV4", 
    "5": "CCTV5", "6": "CCTV6", "7": "CCTV7", "8": "CCTV8",
    "9": "CCTV9", "10": "CCTV10",
    # 4K超高清频道（完整补充）
    "101": "CCTV4K", "102": "浙江卫视4K", "103": "湖南卫视4K",
    "104": "东方卫视4K", "105": "北京卫视4K", "106": "广东卫视4K",
    "107": "深圳卫视4K", "108": "山东卫视4K"
}

# ===================== 频道过滤配置 =====================
# 国内频道关键词（完整覆盖，包含港澳台）
DOMESTIC_KEYWORDS = [
    # 中央频道
    "CCTV", "央视", "中国", "中央",
    
    # 省级卫视
    "卫视", "山东", "浙江", "湖南", "江苏", "东方", "北京", "安徽", "广东", 
    "河南", "深圳", "四川", "重庆", "天津", "湖北", "江西", "河北", "山西", 
    "陕西", "甘肃", "青海", "宁夏", "新疆", "内蒙古", "辽宁", "吉林", "黑龙江",
    "上海", "福建", "广西", "海南", "贵州", "云南", "西藏", 
    
    # 港澳台地区
    "香港", "澳门", "台湾", "翡翠", "凤凰", "亚视", "明珠", "本港", "国际",
    "中天", "东森", "TVBS", "三立", "民视", "华视", "台视", "公视",
    
    # 地方频道常见关键词
    "新闻", "综合", "都市", "生活", "影视", "体育", "少儿", "教育", "公共",
    "经济", "法制", "农业", "文旅", "交通", "购物", "戏剧", "音乐",
    
    # 数字频道
    "高清", "4K", "HD", "标清", "SD"
]

# 国外频道关键词（明确屏蔽）
FOREIGN_KEYWORDS = [
    # 欧美国家
    "BBC", "CNN", "FOX", "HBO", "Discovery", "National Geographic", "ESPN",
    "MTV", "VH1", "Disney", "Cartoon Network", "Nickelodeon",
    "ABC", "NBC", "CBS", "PBS", "Sky", "ITV", "France", "Deutsche",
    "RTL", "TF1", "RAI", "NHK", "KBS", "MBC", "SBS", "Arirang",
    
    # 其他外语关键词
    "English", "Sports", "Movie", "News", "Entertainment", "Music",
    "Kids", "Family", "Documentary", "Lifestyle", "Travel", "Food",
    "Fashion", "Business", "Finance", "Religion", "God", "Church",
    
    # 国家/地区名称
    "USA", "UK", "Britain", "British", "American", "Canada", "Canadian",
    "Australia", "Australian", "New Zealand", "Germany", "German",
    "France", "French", "Italy", "Italian", "Spain", "Spanish",
    "Japan", "Japanese", "Korea", "Korean", "Russia", "Russian",
    "India", "Indian", "Thailand", "Thai", "Vietnam", "Vietnamese",
    
    # 卫星电视标识
    "Satellite", "Sat", "DTH", "DBS", "Asia", "Pacific", "World",
    "International", "Global", "Euro", "European"
]

# 特殊处理频道（强制保留的特定频道，即使包含国外关键词）
WHITELIST_CHANNELS = [
    "CCTV-4", "CCTV-9", "CCTV News",  # 央视国际频道
    "凤凰卫视", "凤凰中文", "凤凰资讯", "凤凰香港",  # 凤凰卫视系列
    "翡翠台", "明珠台", "本港台", "国际台",  # 香港本地频道
]

# ==================================================

class EPGGenerator:
    def __init__(self):
        self.session = self._create_session()
        self.channel_ids: Set[str] = set()
        self.priority_channels = {cat[0]: [] for cat in CHANNEL_PRIORITY}
        self.other_channels: List = []
        self.all_programs: List = []
        self.filtered_foreign_count = 0
        
    def _create_session(self) -> requests.Session:
        """创建带重试机制的会话"""
        session = requests.Session()
        retry_strategy = Retry(
            total=CORE_RETRY_COUNT + 2,
            backoff_factor=1.5,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/xml, */*",
            "Accept-Encoding": "gzip, deflate"
        })
        return session

    def read_epg_sources(self) -> List[str]:
        """读取配置文件中的EPG源"""
        if not os.path.exists(CONFIG_FILE):
            logging.error(f"配置文件不存在: {CONFIG_FILE}")
            raise FileNotFoundError(f"找不到配置文件: {CONFIG_FILE}")
            
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                sources = []
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if line and not line.startswith("#"):
                        if line.startswith(("http://", "https://")):
                            sources.append(line)
                        else:
                            logging.warning(f"第{line_num}行格式错误，已跳过: {line}")
                
                if len(sources) < 3:
                    logging.warning(f"仅找到{len(sources)}个有效EPG源，建议至少配置3个")
                
                return sources[:8]  # 限制最大源数量，避免过度抓取
                
        except Exception as e:
            logging.error(f"读取配置文件失败: {str(e)}")
            raise

    def clean_xml_content(self, content: str) -> str:
        """清理XML内容中的无效字符，避免解析报错"""
        # 移除控制字符和非XML标准字符
        content_clean = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', content)
        # 修复常见的XML转义问题
        content_clean = content_clean.replace('& ', '&amp; ')
        return content_clean

    def fetch_single_source(self, source: str) -> Tuple[bool, str, any]:
        """并发获取单个EPG源数据"""
        try:
            start_time = time.time()
            logging.info(f"开始抓取: {source}")
            
            response = self.session.get(source, timeout=TIMEOUT)
            response.raise_for_status()
            
            # 处理gzip压缩
            if source.endswith('.gz'):
                content = gzip.decompress(response.content).decode('utf-8')
            else:
                content = response.text
                
            # 清理XML内容，避免解析失败
            content_clean = self.clean_xml_content(content)
            xml_tree = etree.fromstring(content_clean.encode('utf-8'))
            
            cost_time = time.time() - start_time
            logging.info(f"成功抓取: {source} | 耗时: {cost_time:.2f}s")
            return True, source, xml_tree
            
        except Exception as e:
            logging.error(f"抓取失败 {source}: {str(e)}")
            return False, source, None

    def should_keep_channel(self, channel_name: str) -> Tuple[bool, str]:
        """判断是否应该保留该频道，返回(是否保留, 原因)"""
        channel_name_lower = channel_name.lower()
        
        # 1. 首先检查白名单（强制保留）
        for white_channel in WHITELIST_CHANNELS:
            if white_channel in channel_name:
                return True, f"白名单频道: {white_channel}"
        
        # 2. 检查是否包含国内关键词
        has_domestic_keyword = any(kw in channel_name for kw in DOMESTIC_KEYWORDS)
        
        # 3. 检查是否包含国外关键词
        has_foreign_keyword = any(
            fk.lower() in channel_name_lower for fk in FOREIGN_KEYWORDS
        )
        
        # 4. 判断逻辑：
        #    - 如果包含国内关键词，保留
        #    - 如果不含国内关键词但含国外关键词，过滤
        #    - 如果都不含，可能是地方频道，根据其他规则判断
        
        if has_domestic_keyword:
            if has_foreign_keyword:
                # 同时包含国内外关键词，以国内为主（可能是"BBC中文"之类的）
                domestic_kws = [kw for kw in DOMESTIC_KEYWORDS if kw in channel_name]
                return True, f"包含国内关键词: {domestic_kws}"
            else:
                # 只含国内关键词，肯定保留
                domestic_kws = [kw for kw in DOMESTIC_KEYWORDS if kw in channel_name]
                return True, f"包含国内关键词: {domestic_kws}"
        else:
            if has_foreign_keyword:
                # 只含国外关键词，过滤
                foreign_kws = [fk for fk in FOREIGN_KEYWORDS if fk.lower() in channel_name_lower]
                return False, f"包含国外关键词: {foreign_kws}"
            else:
                # 都不包含，可能是数字频道或地方台，根据额外规则判断
                # 规则：中文频道保留，纯英文/数字频道过滤
                chinese_chars = sum(1 for c in channel_name if '\u4e00' <= c <= '\u9fff')
                if chinese_chars >= 2:  # 至少2个中文字符
                    return True, "中文频道"
                else:
                    return False, "非中文频道"

    def process_channels(self, xml_tree, source: str) -> Tuple[int, int]:
        """处理频道数据，含分类、过滤、统计"""
        channels = xml_tree.xpath("//channel")
        shandong_count = 0
        filtered_count = 0
        
        for channel in channels:
            cid = channel.get("id", "").strip()
            if not cid:
                continue
                
            # 应用酷9ID映射（数字ID→名称ID）
            if cid in COOL9_ID_MAPPING:
                cid = COOL9_ID_MAPPING[cid]
                
            if cid in self.channel_ids:
                continue  # 跳过重复频道
                
            # 获取频道名称
            display_names = channel.xpath(".//display-name/text()")
            channel_name = display_names[0].strip() if display_names else ""
            
            if not channel_name:
                continue  # 跳过无名频道
                
            # 判断是否应该保留该频道
            keep_channel, reason = self.should_keep_channel(channel_name)
            
            if not keep_channel:
                filtered_count += 1
                self.filtered_foreign_count += 1
                logging.debug(f"过滤国外频道: {channel_name} | 原因: {reason}")
                continue
                
            # 更新频道ID（统一格式）
            channel.set("id", cid)
            self.channel_ids.add(cid)
            
            # 按优先级分类
            channel_added = False
            for cat_name, keywords in CHANNEL_PRIORITY:
                if any(kw in channel_name for kw in keywords):
                    self.priority_channels[cat_name].append(channel)
                    channel_added = True
                    if "山东" in channel_name:
                        shandong_count += 1  # 统计山东本地频道
                    break
                    
            if not channel_added:
                self.other_channels.append(channel)
                
        return shandong_count, filtered_count

    def process_programs(self, xml_tree):
        """处理节目单数据，映射酷9频道ID，同时过滤国外频道节目"""
        programs = xml_tree.xpath("//programme")
        for program in programs:
            channel_id = program.get("channel", "")
            
            # 首先检查频道ID是否在保留列表中
            if channel_id in self.channel_ids or channel_id in COOL9_ID_MAPPING.values():
                # 节目单频道ID映射（与频道ID保持一致）
                if channel_id in COOL9_ID_MAPPING:
                    program.set("channel", COOL9_ID_MAPPING[channel_id])
                self.all_programs.append(program)

    def fetch_all_sources(self, sources: List[str]) -> bool:
        """并发获取所有EPG源数据并处理"""
        successful_sources = 0
        
        with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(sources))) as executor:
            future_to_source = {
                executor.submit(self.fetch_single_source, source): source 
                for source in sources
            }
            
            for future in as_completed(future_to_source):
                source = future_to_source[future]
                try:
                    success, _, xml_tree = future.result()
                    if success and xml_tree is not None:
                        shandong_count, filtered_count = self.process_channels(xml_tree, source)
                        self.process_programs(xml_tree)
                        successful_sources += 1
                        logging.info(f"处理完成: {source} | 山东频道: {shandong_count}个 | 过滤国外: {filtered_count}个")
                        
                except Exception as e:
                    logging.error(f"处理源数据失败 {source}: {str(e)}")
        
        return successful_sources > 0

    def generate_final_xml(self) -> str:
        """生成最终的EPG XML文件（按优先级排序）"""
        # 创建XML根节点
        xml_declare = f'''<?xml version="1.0" encoding="UTF-8"?>
<tv generator-info-name="optimized-epg-generator" 
    generator-info-url="https://github.com/fxq12345/epg" 
    last-update="{time.strftime("%Y%m%d%H%M%S")}">'''
        
        root = etree.fromstring(f"{xml_declare}</tv>".encode("utf-8"))
        
        # 按优先级添加频道（山东本地→央视→其他卫视→其他频道）
        insert_position = 0
        for category, _ in CHANNEL_PRIORITY:
            for channel in self.priority_channels[category]:
                root.insert(insert_position, channel)
                insert_position += 1
                
        # 添加其他国内频道
        for channel in self.other_channels:
            root.insert(insert_position, channel)
            insert_position += 1
            
        # 添加所有节目单
        for program in self.all_programs:
            root.append(program)
            
        return etree.tostring(root, encoding="utf-8", pretty_print=True).decode("utf-8")

    def save_epg_files(self, xml_content: str):
        """保存EPG文件（XML+GZIP），清理旧文件"""
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # 清理旧文件，避免占用空间
        for f in os.listdir(OUTPUT_DIR):
            if f.endswith(('.xml', '.gz', '.log')):
                try:
                    os.remove(os.path.join(OUTPUT_DIR, f))
                except Exception as e:
                    logging.warning(f"删除旧文件失败 {f}: {str(e)}")
        
        # 保存XML文件
        xml_path = os.path.join(OUTPUT_DIR, "epg.xml")
        with open(xml_path, "w", encoding="utf-8") as f:
            f.write(xml_content)
        xml_size = os.path.getsize(xml_path)
        
        # 保存GZIP压缩文件（节省空间，机顶盒支持自动解压）
        gz_path = os.path.join(OUTPUT_DIR, "epg.gz")
        with gzip.open(gz_path, "wb") as f:
            f.write(xml_content.encode("utf-8"))
        gz_size = os.path.getsize(gz_path)
        
        logging.info(f"EPG文件生成完成: XML={xml_size}字节, GZIP={gz_size}字节")

    def print_statistics(self):
        """打印详细统计报告，方便核对"""
        total_channels = len(self.channel_ids)
        total_programs = len(self.all_programs)
        
        logging.info("\n" + "="*60)
        logging.info("📊 EPG生成统计报告")
        logging.info("="*60)
        
        for category, _ in CHANNEL_PRIORITY:
            count = len(self.priority_channels[category])
            logging.info(f"  {category}: {count}个频道")
            
        other_count = len(self.other_channels)
        logging.info(f"  其他国内频道: {other_count}个")
        logging.info(f"  总频道数: {total_channels}个")
        logging.info(f"  总节目数: {total_programs}个")
        logging.info(f"  过滤国外频道: {self.filtered_foreign_count}个")
        logging.info("="*60)

    def run(self):
        """主运行方法，统一调度所有流程"""
        start_time = time.time()
        logging.info("=== EPG生成开始 ===")
        
        try:
            # 读取配置文件中的EPG源
            sources = self.read_epg_sources()
            logging.info(f"读取到{len(sources)}个EPG源")
            
            # 并发获取并处理所有源数据
            if not self.fetch_all_sources(sources):
                logging.error("所有EPG源获取失败，程序退出")
                return False
                
            # 生成最终的XML内容
            xml_content = self.generate_final_xml()
            
            # 保存文件（XML+GZIP）
            self.save_epg_files(xml_content)
            
            # 输出统计报告
            self.print_statistics()
            
            total_time = time.time() - start_time
            logging.info(f"=== EPG生成完成! 总耗时: {total_time:.2f}秒 ===")
            return True
            
        except Exception as e:
            logging.error(f"EPG生成失败: {str(e)}")
            return False

def main():
    """主函数，程序入口"""
    generator = EPGGenerator()
    success = generator.run()
    exit(0 if success else 1)

if __name__ == "__main__":
    main()
