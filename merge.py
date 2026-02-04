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
        self.name_to_final_id = dict()      # 频道名称→最终ID 映射
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
        """标准化频道名称（与epg_data.json的logo配置完全匹配）"""
        # 核心：数据源频道名 → epg_data.json中的epgid
        name_mapping = {
            "1905极限反转(国内)": "1905极限反转",
            "4K修复频道": "4K修复",
            "河南移动电视": "河南移动电视",
            "亚洲卫视": "亚洲卫视",
            # 可继续补充其他需要匹配的频道
        }
        # 优先匹配logo配置的名称
        if name in name_mapping:
            name = name_mapping[name]
        # 基础标准化（去特殊字符、统一格式）
        name = re.sub(r'[^\u4e00-\u9fff0-9a-zA-Z]', '', name)
        name = name.replace("new", "NEW").replace("newtv", "NEWTV")
        name = re.sub(r'^IHOT|^IPTV', '', name)
        return name.strip()

    def pre_fetch_program_channels(self, sources: List[str]):
        """预抓取所有节目单的channel，建立名称→ID映射"""
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
                
                # 建立频道ID→名称映射
                channel_id_to_name = {}
                for ch in xml_tree.xpath("//channel"):
                    cid = ch.get("id", "").strip()
                    display_names = ch.xpath(".//display-name/text()")
                    ch_name = display_names[0].strip() if display_names else cid
                    channel_id_to_name[cid] = ch_name
                
                # 建立名称→数字ID映射
                for program in xml_tree.xpath("//programme"):
                    prog_cid = program.get("channel", "").strip()
                    if prog_cid.isdigit() and prog_cid in channel_id_to_name:
                        ch_name = channel_id_to_name[prog_cid]
                        normalized_name = self.normalize_channel_name(ch_name)
                        if normalized_name not in self.program_channel_map:
                            self.program_channel_map[normalized_name] = prog_cid
                            
            except Exception as e:
                logging.warning(f"预抓取{source}失败: {str(e)}")
        
        logging.info(f"预抓取完成，建立{len(self.program_channel_map)}个名称→ID映射")

    def process_channels(self, xml_tree, source: str) -> int:
        """处理频道：统一名称以匹配logo"""
        channels = xml_tree.xpath("//channel")
        add_count = 0
        
        for channel in channels:
            original_cid = channel.get("id", "").strip()
            if not original_cid:
                continue
            
            # 获取并标准化频道名称（匹配logo）
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
            
            # 分配最终ID（确保节目单匹配）
            final_cid = original_cid
            if normalized_name in self.program_channel_map:
                final_cid = self.program_channel_map[normalized_name]
            
            # 去重并保存
            if normalized_name in self.name_to_final_id:
                final_cid = self.name_to_final_id[normalized_name]
            else:
                if not final_cid.isdigit() and normalized_name in self.program_channel_map:
                    final_cid = self.program_channel_map[normalized_name]
            
            if final_cid in self.channel_ids or not final_cid:
                continue
            
            # 更新频道信息（名称+ID）
            channel.set("id", final_cid)
            # 统一频道名称为logo配置的名称
            for dn in channel.xpath(".//display-name"):
                dn.text = normalized_name
            self.channel_ids.add(final_cid)
            self.name_to_final_id[normalized_name] = final_cid
            self.all_channels.append(channel)
            add_count += 1
                
        logging.info(f"从{source}处理到{add_count}个新频道")
        return add_count

    def process_programs(self, xml_tree):
        """处理节目单：匹配频道ID"""
        import datetime
        programs = xml_tree.xpath("//programme")
        for program in programs:
            prog_cid = program.get("channel", "").strip()
            if not (prog_cid.isdigit() and prog_cid in self.channel_ids):
                continue

            # 时区转换为东八区
            start_str = program.get("start", "")
            stop_str = program.get("stop", "")
            if start_str and stop_str:
                try:
                    start_utc = datetime.datetime.strptime(start_str[:14], "%Y%m%d%H%M%S")
                    stop_utc = datetime.datetime.strptime(stop_str[:14], "%Y%m%d%H%M%S")
                    start_cst = start_utc + datetime.timedelta(hours=8)
                    stop_cst = stop_utc + datetime.timedelta(hours=8)
                    program.set("start", start_cst.strftime("%Y%m%d%H%M%S") + " +0800")
                    program.set("stop", stop_cst.strftime("%Y%m%d%H%M%S") + " +0800")
                except Exception as e:
                    logging.warning(f"节目时间转换失败: {str(e)}")
                    continue

            self.all_programs.append(program)

    def fetch_all_sources(self, sources: List[str]) -> bool:
        """并发获取所有EPG源并处理"""
        self.pre_fetch_program_channels(sources)
        successful_sources = 0
        with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(sources))) as executor:
            future_to_source = {executor.submit(self.fetch_single_source, source): source for source in sources}
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
        return successful_sources > 0

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
        for f in os.listdir(OUTPUT_DIR):
            if f.endswith(('.xml', '.gz', '.log')):
                try:
                    os.remove(os.path.join(OUTPUT_DIR, f))
                except Exception as e:
                    logging.warning(f"删除旧文件失败 {f}: {str(e)}")
        xml_path = os.path.join(OUTPUT_DIR, "epg.xml")
        with open(xml_path, "w", encoding="utf-8") as f:
            f.write(xml_content)
        gz_path = os.path.join(OUTPUT_DIR, "epg.gz")
        with gzip.open(gz_path, "wb") as f:
            f.write(xml_content.encode("utf-8"))
        logging.info(f"EPG文件生成完成: XML={os.path.getsize(xml_path)}字节, GZIP={os.path.getsize(gz_path)}字节")

    def print_statistics(self):
        """打印统计报告"""
        logging.info("\n" + "="*50)
        logging.info("📊 EPG生成统计报告")
        logging.info("="*50)
        logging.info(f"  最终保留频道数: {len(self.channel_ids)}个")
        logging.info(f"  最终保留节目单数: {len(self.all_programs)}个")
        logging.info(f"  已匹配logo的频道数: {len(self.name_to_final_id)}个")
        logging.info("="*50)

    def run(self):
        """主运行方法"""
        start_time = time.time()
        logging.info("=== EPG生成开始 ===")
        try:
            sources = self.read_epg_sources()
            logging.info(f"读取到{len(sources)}个有效EPG源")
            if not self.fetch_all_sources(sources):
                return False
            xml_content = self.generate_final_xml()
            self.save_epg_files(xml_content)
            self.print_statistics()
            logging.info(f"=== EPG生成完成! 总耗时: {time.time()-start_time:.2f}秒 ===")
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
