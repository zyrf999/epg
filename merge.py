import os
import gzip
import re
import time
import logging
from typing import List, Dict, Set, Tuple
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from xml.dom import minidom

import requests
from lxml import etree
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from opencc import OpenCC

# ===================== 配置区 =====================
CONFIG_FILE = "config.txt"
OUTPUT_DIR = "output"
LOG_FILE = "epg_merge.log"
MAX_WORKERS = 5
TIMEOUT = 30
CORE_RETRY_COUNT = 2
TZ_UTC_PLUS_8 = timezone(timedelta(hours=8))

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# 频道名→ID映射（全部用字符串）
COOL9_ID_MAPPING = {
    "山东卫视": "89", "山东教育": "221", "CCTV1": "1", "CCTV2": "2"
}

# 过滤关键词
FOREIGN_KEYWORDS = ["BBC", "CNN", "欧美", "美国", "日本", "韩国"]
DOMESTIC_SPECIAL = ["爱", "NEW", "IPTV"]
# ==================================================

class EPGGenerator:
    def __init__(self):
        self.session = self._create_session()
        self.channel_ids: Set[str] = set()  # 频道ID统一用字符串
        self.all_channels: Dict[str, List] = defaultdict(list)
        self.all_programs: Dict[str, List] = defaultdict(list)
        self.name_to_id: Dict[str, str] = {}  # 名称→字符串ID
        self.cc = OpenCC("t2s")

    def _create_session(self) -> requests.Session:
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
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        })
        return session

    def read_epg_sources(self) -> List[str]:
        if not os.path.exists(CONFIG_FILE):
            raise FileNotFoundError(f"找不到配置文件: {CONFIG_FILE}")
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip() and not line.startswith("#")]

    def clean_xml_content(self, content: str) -> str:
        content_clean = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', content)
        content_clean = content_clean.replace('& ', '&amp; ')
        return content_clean

    def fetch_single_source(self, source: str) -> Tuple[bool, str, any]:
        try:
            start_time = time.time()
            logging.info(f"抓取: {source}")
            response = self.session.get(source, timeout=TIMEOUT)
            response.raise_for_status()
            
            if source.endswith('.gz'):
                content = gzip.decompress(response.content).decode('utf-8', errors='ignore')
            else:
                content = response.text
                
            content_clean = self.clean_xml_content(content)
            xml_tree = etree.fromstring(content_clean.encode('utf-8'))
            logging.info(f"成功抓取{source} | 耗时: {time.time()-start_time:.2f}s")
            return True, source, xml_tree
        except Exception as e:
            logging.error(f"抓取{source}失败: {str(e)}")
            return False, source, None

    def normalize_channel_name(self, name: str) -> str:
        name = self.cc.convert(name.strip())
        name = re.sub(r'[^\u4e00-\u9fff0-9a-zA-Z]', '', name)
        name = name.replace("new", "NEW").replace("newtv", "NEWTV")
        return name

    def process_single_epg(self, xml_tree):
        # 处理频道（ID强制为字符串）
        for channel in xml_tree.xpath("//channel"):
            cid = str(channel.get("id", "").strip())  # 强制字符串
            display_names = [self.normalize_channel_name(name.text) for name in channel.xpath(".//display-name") if name.text]
            if not display_names:
                continue
            
            main_name = display_names[0]
            if main_name in self.name_to_id:
                final_id = self.name_to_id[main_name]
            else:
                final_id = COOL9_ID_MAPPING.get(main_name, cid)
                self.name_to_id[main_name] = final_id
                for name in display_names:
                    self.name_to_id[name] = final_id
            
            for name in display_names:
                if name not in [n[0] for n in self.all_channels[final_id]]:
                    self.all_channels[final_id].append((name, "zh"))
            self.channel_ids.add(final_id)

        # 处理节目单（ID强制为字符串）
        for program in xml_tree.xpath("//programme"):
            prog_cid = str(program.get("channel", "").strip())  # 强制字符串
            try:
                prog_name_elem = xml_tree.xpath(f"//channel[@id='{prog_cid}']//display-name")
                prog_name = self.normalize_channel_name(prog_name_elem[0].text) if prog_name_elem else ""
            except:
                prog_name = ""
            
            final_id = self.name_to_id.get(prog_name, "")
            if not final_id:
                continue
            
            # 时区转换
            start_str = re.sub(r'\s+', '', program.get("start"))
            stop_str = re.sub(r'\s+', '', program.get("stop"))
            try:
                start = datetime.strptime(start_str, "%Y%m%d%H%M%S%z").astimezone(TZ_UTC_PLUS_8)
                stop = datetime.strptime(stop_str, "%Y%m%d%H%M%S%z").astimezone(TZ_UTC_PLUS_8)
            except:
                continue
            
            prog_elem = etree.Element(
                "programme",
                attrib={"channel": final_id, 
                        "start": start.strftime("%Y%m%d%H%M%S %z"), 
                        "stop": stop.strftime("%Y%m%d%H%M%S %z")}
            )
            # 处理标题
            title = program.find("title")
            if title and title.text:
                title_elem = etree.SubElement(prog_elem, "title")
                title_elem.text = self.cc.convert(title.text.strip())
            # 处理描述
            desc = program.find("desc")
            if desc and desc.text:
                desc_elem = etree.SubElement(prog_elem, "desc")
                desc_elem.text = self.cc.convert(desc.text.strip())
            
            if len(prog_elem) > 0:
                self.all_programs[final_id].append(prog_elem)

    def fetch_all_sources(self, sources: List[str]) -> bool:
        successful = 0
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(self.fetch_single_source, s): s for s in sources}
            for future in as_completed(futures):
                success, _, xml_tree = future.result()
                if success and xml_tree:
                    try:
                        self.process_single_epg(xml_tree)
                        successful += 1
                    except Exception as e:
                        logging.error(f"处理源失败: {str(e)}")
        return successful > 0

    def generate_final_xml(self) -> str:
        # 强制创建output目录
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        # 即使无数据，也生成空文件避免Workflow报错
        if not self.channel_ids:
            with open(os.path.join(OUTPUT_DIR, "epg.xml"), "w", encoding="utf-8") as f:
                f.write("<!-- 空EPG -->")
            return "<!-- 空EPG -->"
        
        root = etree.Element(
            "tv",
            attrib={"generator-info-name": "epg-merged", 
                    "last-update": datetime.now(TZ_UTC_PLUS_8).strftime("%Y%m%d%H%M%S")}
        )
        # 添加频道
        for cid in self.channel_ids:
            channel_elem = etree.SubElement(root, "channel", attrib={"id": cid})
            for name, lang in self.all_channels[cid]:
                dn_elem = etree.SubElement(channel_elem, "display-name", attrib={"lang": lang})
                dn_elem.text = name
        # 添加节目单
        for cid in self.all_programs:
            root.extend(self.all_programs[cid])
        
        rough_str = etree.tostring(root, encoding="utf-8")
        reparsed = minidom.parseString(rough_str)
        return reparsed.toprettyxml(indent='\t', newl='\n', encoding="utf-8").decode("utf-8")

    def save_epg_files(self, xml_content: str):
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        xml_path = os.path.join(OUTPUT_DIR, "epg.xml")
        with open(xml_path, "w", encoding="utf-8") as f:
            f.write(xml_content)
        gz_path = os.path.join(OUTPUT_DIR, "epg.gz")
        with gzip.open(gz_path, "wb") as f:
            f.write(xml_content.encode("utf-8"))
        logging.info(f"生成完成: XML={os.path.getsize(xml_path)}字节")

    def run(self):
        start_time = time.time()
        logging.info("=== EPG合并开始 ===")
        try:
            sources = self.read_epg_sources()
            logging.info(f"读取到{len(sources)}个源")
            self.fetch_all_sources(sources)
            xml_content = self.generate_final_xml()
            self.save_epg_files(xml_content)
            logging.info(f"=== 完成! 耗时: {time.time()-start_time:.2f}秒 ===")
            return True
        except Exception as e:
            logging.error(f"运行失败: {str(e)}", exc_info=True)
            # 即使失败，也生成空文件
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            with open(os.path.join(OUTPUT_DIR, "epg.xml"), "w", encoding="utf-8") as f:
                f.write("<!-- 运行失败 -->")
            return False

if __name__ == "__main__":
    EPGGenerator().run()
