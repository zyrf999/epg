import os
import gzip
import re
import time
import logging
from datetime import datetime, timedelta
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

# 仅保留必要的手动映射（不确定的可以全部删除，留空{}）
COOL9_ID_MAPPING = {
    "89": "山东卫视", "221": "山东教育", "381": "山东新闻", 
    "382": "山东农科", "383": "山东齐鲁", "384": "山东文旅",
    "1": "CCTV1", "2": "CCTV2", "3": "CCTV3", "4": "CCTV4", 
    "5": "CCTV5", "6": "CCTV6", "7": "CCTV7", "8": "CCTV8",
    "9": "CCTV9", "10": "CCTV10"
}

# 国外频道关键词黑名单（命中则过滤）
FOREIGN_KEYWORDS = [
    "BBC", "CNN", "NBC", "FOX", "HBO", "Netflix", "Disney",
    "欧美", "美国", "英国", "法国", "德国", "日本", "韩国",
    "泰国", "越南", "印尼", "马来西亚", "新加坡", "澳洲",
    "欧洲", "美洲", "非洲", "俄罗斯", "印度", "巴西"
]

# 国内特殊频道关键词（兜底，防止误过滤）
DOMESTIC_SPECIAL = ["popc", "爱", "淘", "new", "NEW", "POPC", "超级电影", "IPTV", "new系列", "NewTV"]
# ==================================================

class EPGGenerator:
    def __init__(self):
        self.session = self._create_session()
        self.channel_ids: Set[str] = set()  # 去重频道ID
        self.all_channels: List = []        # 所有保留的频道
        self.all_programs: List = []        # 所有保留的节目单
        self.name_to_final_id = dict()      # 频道名称→最终数字ID 映射
        self.program_channel_map = dict()   # 临时存储节目单channel映射

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
                
                if len(sources) < 1:
                    logging.error(f"未找到有效EPG源，程序退出")
                    raise ValueError("无有效EPG源")
                
                return sources[:8]
        except Exception as e:
            logging.error(f"读取配置文件失败: {str(e)}")
            raise

    def clean_xml_content(self, content: str) -> str:
        """清理XML内容中的无效字符"""
        content_clean = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', content)
        content_clean = content_clean.replace('& ', '&amp; ')
        return content_clean

    def fetch_single_source(self, source: str) -> Tuple[bool, str, any]:
        """并发获取单个EPG源数据"""
        try:
            start_time = time.time()
            logging.info(f"开始抓取: {source}")
            
            response = self.session.get(source, timeout=TIMEOUT)
            response.raise_for_status()
            
            if source.endswith('.gz'):
                content = gzip.decompress(response.content).decode('utf-8')
            else:
                content = response.text
                
            content_clean = self.clean_xml_content(content)
            xml_tree = etree.fromstring(content_clean.encode('utf-8'))
            
            cost_time = time.time() - start_time
            logging.info(f"成功抓取: {source} | 耗时: {cost_time:.2f}s")
            return True, source, xml_tree
            
        except Exception as e:
            logging.error(f"抓取失败 {source}: {str(e)}")
            return False, source, None

    def normalize_channel_name(self, name: str) -> str:
        """标准化频道名称（统一识别NEWTV系列）"""
        name = re.sub(r'[^\u4e00-\u9fff0-9a-zA-Z]', '', name)
        name = name.replace("new", "NEW").replace("newtv", "NEWTV")
        name = re.sub(r'^IHOT|^IPTV', '', name)
        return name.strip()

    def pre_fetch_program_channels(self, sources: List[str]):
        """预抓取所有节目单的channel，建立名称→数字ID映射"""
        logging.info("开始预抓取节目单频道映射...")
        for source in sources:
            try:
                response = self.session.get(source, timeout=TIMEOUT)
                response.raise_for_status()
                
                if source.endswith('.gz'):
                    content = gzip.decompress(response.content).decode('utf-8')
                else:
                    content = response.text
                    
                content_clean = self.clean_xml_content(content)
                xml_tree = etree.fromstring(content_clean.encode('utf-8'))
                
                # 提取所有节目单的channel（数字ID）和对应频道名称
                programs = xml_tree.xpath("//programme")
                channels = xml_tree.xpath("//channel")
                
                # 建立频道ID→名称映射
                channel_id_to_name = {}
                for ch in channels:
                    cid = ch.get("id", "").strip()
                    display_names = ch.xpath(".//display-name/text()")
                    ch_name = display_names[0].strip() if display_names else cid
                    channel_id_to_name[cid] = ch_name
                
                # 建立名称→数字ID映射
                for program in programs:
                    prog_cid = program.get("channel", "").strip()
                    if prog_cid.isdigit() and prog_cid in channel_id_to_name:
                        ch_name = channel_id_to_name[prog_cid]
                        normalized_name = self.normalize_channel_name(ch_name)
                        if normalized_name and normalized_name not in self.program_channel_map:
                            self.program_channel_map[normalized_name] = prog_cid
                            
            except Exception as e:
                logging.warning(f"预抓取{source}失败: {str(e)}")
        
        logging.info(f"预抓取完成，建立{len(self.program_channel_map)}个名称→数字ID映射")

    def process_channels(self, xml_tree, source: str) -> int:
        """处理频道：自动给NEWTV系列分配数字ID"""
        channels = xml_tree.xpath("//channel")
        add_count = 0
        
        for channel in channels:
            original_cid = channel.get("id", "").strip()
            if not original_cid:
                continue
            
            # 获取频道名称并标准化
            display_names = channel.xpath(".//display-name/text()")
            channel_name = display_names[0].strip() if display_names else original_cid
            normalized_name = self.normalize_channel_name(channel_name)
            if not normalized_name:
                continue
            
            # 过滤国外频道
            if any(kw in channel_name for kw in FOREIGN_KEYWORDS):
                continue
            if any(kw in channel_name for kw in DOMESTIC_SPECIAL):
                pass
            
            # 核心修改点1：对所有频道都尝试从预抓取映射中获取数字ID
            final_cid = original_cid
            
            # 首先尝试从预抓取映射中查找
            if normalized_name in self.program_channel_map:
                final_cid = self.program_channel_map[normalized_name]
                logging.debug(f"从预抓取映射中找到匹配: '{normalized_name}' -> {final_cid}")
            
            # 如果没找到，并且是NEWTV系列，再尝试其他方法
            elif "NEWTV" in normalized_name or "NEW" in normalized_name:
                # 若预抓取失败，尝试从当前源节目单提取
                programs = xml_tree.xpath('//programme[contains(@channel, "{}")]'.format(normalized_name[:4]))
                if programs:
                    final_cid = programs[0].get("channel", "").strip()
            
            # 处理手动映射和去重
            if normalized_name in self.name_to_final_id:
                final_cid = self.name_to_final_id[normalized_name]
            else:
                if original_cid in COOL9_ID_MAPPING:
                    final_cid = COOL9_ID_MAPPING[original_cid]
                elif channel_name in COOL9_ID_MAPPING:
                    final_cid = COOL9_ID_MAPPING[channel_name]
                
                # 核心修改点2：确保最终ID是数字
                # 如果不是数字，尝试从预抓取映射中查找
                if not final_cid.isdigit() and normalized_name in self.program_channel_map:
                    final_cid = self.program_channel_map[normalized_name]
            
            if final_cid in self.channel_ids or not final_cid:
                continue
            
            # 更新频道ID并保存映射
            channel.set("id", final_cid)
            self.channel_ids.add(final_cid)
            self.name_to_final_id[normalized_name] = final_cid
            self.all_channels.append(channel)
            add_count += 1
                
        logging.info(f"从{source}处理到{add_count}个新频道")
        return add_count

    def get_channel_name_by_id(self, channel_id: str) -> str:
        """根据频道ID获取频道名称"""
        for channel in self.all_channels:
            if channel.get("id", "") == channel_id:
                display_names = channel.xpath(".//display-name/text()")
                if display_names:
                    return display_names[0].strip()
        return ""

    def adjust_program_time(self, program, days=0, hours=0):
        """调整节目时间"""
        for attr in ["start", "stop"]:
            time_str = program.get(attr, "")
            if time_str and ' ' in time_str:
                time_part, tz = time_str.split(' ')
                if len(time_part) >= 14:
                    try:
                        dt = datetime.strptime(time_part[:14], "%Y%m%d%H%M%S")
                        
                        # 记录原始时间（用于日志）
                        original = dt.strftime("%Y-%m-%d %H:%M")
                        
                        dt = dt + timedelta(days=days, hours=hours)
                        new_time = dt.strftime("%Y%m%d%H%M%S") + " " + tz
                        program.set(attr, new_time)
                        
                        # 记录调整详情
                        adjusted = dt.strftime("%Y-%m-%d %H:%M")
                        logging.debug(f"时间调整: {original} -> {adjusted} ({days:+d}天 {hours:+d}小时)")
                        
                    except Exception as e:
                        logging.warning(f"时间调整失败 {time_str}: {e}")

    def process_programs(self, xml_tree):
        """处理节目单：修正时间问题"""
        programs = xml_tree.xpath("//programme")
        
        ai_count = 0
        other_count = 0
        
        for program in programs:
            prog_cid = program.get("channel", "").strip()
            
            if prog_cid.isdigit() and prog_cid in self.channel_ids:
                # 获取频道名称
                channel_name = self.get_channel_name_by_id(prog_cid)
                
                if channel_name:
                    # 判断是否为爱系列
                    is_ai_series = "爱" in channel_name or "iHOT" in channel_name.upper()
                    
                    if is_ai_series:
                        # 爱系列：加24小时（因为慢了一天）← 已修复！
                        self.adjust_program_time(program, hours=+24)
                        ai_count += 1
                        logging.info(f"爱系列 {channel_name} 时间调整 +24小时")
                    else:
                        # 其他频道：加8小时（UTC -> 北京时间）
                        self.adjust_program_time(program, hours=+8)
                        other_count += 1
                
                self.all_programs.append(program)
        
        # 添加统计信息
        if ai_count > 0 or other_count > 0:
            logging.info(f"时间调整统计: 爱系列 {ai_count}个, 其他频道 {other_count}个")

    def fetch_all_sources(self, sources: List[str]) -> bool:
        """并发获取所有EPG源并处理"""
        # 第一步：预抓取节目单channel映射
        self.pre_fetch_program_channels(sources)
        
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
                        self.process_channels(xml_tree, source)
                        self.process_programs(xml_tree)
                        successful_sources += 1
                        
                except Exception as e:
                    logging.error(f"处理源数据失败 {source}: {str(e)}")
        
        if successful_sources == 0:
            logging.error("所有EPG源处理失败")
            return False
        return True

    def generate_final_xml(self) -> str:
        """生成最终EPG XML文件"""
        xml_declare = f'''<?xml version="1.0" encoding="UTF-8"?>
<tv generator-info-name="domestic-epg-generator" 
    generator-info-url="https://github.com/fxq12345/epg" 
    last-update="{time.strftime("%Y%m%d%H%M%S")}">'''
        
        root = etree.fromstring(f"{xml_declare}</tv>".encode("utf-8"))
        
        for channel in self.all_channels:
            root.append(channel)
            
        for program in self.all_programs:
            root.append(program)
            
        return etree.tostring(root, encoding="utf-8", pretty_print=True).decode("utf-8")

    def save_epg_files(self, xml_content: str):
        """保存EPG文件"""
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # 清理旧文件
        for f in os.listdir(OUTPUT_DIR):
            if f.endswith(('.xml', '.gz', '.log')):
                try:
                    os.remove(os.path.join(OUTPUT_DIR, f))
                except Exception as e:
                    logging.warning(f"删除旧文件失败 {f}: {str(e)}")
        
        # 保存XML和GZIP
        xml_path = os.path.join(OUTPUT_DIR, "epg.xml")
        with open(xml_path, "w", encoding="utf-8") as f:
            f.write(xml_content)
        xml_size = os.path.getsize(xml_path)
        
        gz_path = os.path.join(OUTPUT_DIR, "epg.gz")
        with gzip.open(gz_path, "wb") as f:
            f.write(xml_content.encode("utf-8"))
        gz_size = os.path.getsize(gz_path)
        
        logging.info(f"EPG文件生成完成: XML={xml_size}字节, GZIP={gz_size}字节")

    def print_statistics(self):
        """打印统计报告"""
        total_channels = len(self.channel_ids)
        total_programs = len(self.all_programs)
        
        logging.info("\n" + "="*50)
        logging.info("📊 EPG生成统计报告")
        logging.info("="*50)
        logging.info(f"  最终保留频道数: {total_channels}个")
        logging.info(f"  最终保留节目单数: {total_programs}个")
        logging.info(f"  自动关联名称→数字ID数: {len(self.name_to_final_id)}个")
        logging.info("="*50)

    def run(self):
        """主运行方法"""
        start_time = time.time()
        logging.info("=== EPG生成开始 ===")
        
        # 添加当前时间信息
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logging.info(f"当前系统时间: {current_time}")
        
        try:
            sources = self.read_epg_sources()
            logging.info(f"读取到{len(sources)}个有效EPG源")
            
            if not self.fetch_all_sources(sources):
                return False
                
            xml_content = self.generate_final_xml()
            self.save_epg_files(xml_content)
            self.print_statistics()
            
            total_time = time.time() - start_time
            logging.info(f"=== EPG生成完成! 总耗时: {total_time:.2f}秒 ===")
            return True
            
        except Exception as e:
            logging.error(f"EPG生成失败: {str(e)}")
            return False

def main():
    """主函数入口"""
    generator = EPGGenerator()
    success = generator.run()
    exit(0 if success else 1)

if __name__ == "__main__":
    main()
