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
MAX_WORKERS = 3
TIMEOUT = 30
CORE_RETRY_COUNT = 2
DAYS_KEEP = 3
GENERATE_GZ = True

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

COOL9_ID_MAPPING = {
    "1": "CCTV1", "2": "CCTV2", "3": "CCTV3", "4": "CCTV4",
    "5": "CCTV5", "6": "CCTV6", "7": "CCTV7", "8": "CCTV8",
    "9": "CCTV9", "10": "CCTV10", "11": "CCTV11", "12": "CCTV12",
    "13": "CCTV13", "14": "CCTV14", "15": "CCTV15", "16": "CCTV16",
    "21": "北京卫视", "22": "河南卫视", "23": "河北卫视",
    "24": "湖北卫视", "25": "海南卫视", "26": "贵州卫视", "27": "厦门卫视",
    "28": "CCTV5+", "29": "峨眉电影", "30": "峨眉电影4K", "31": "北京IPTV4K超清",
    "32": "淘电影", "33": "淘娱乐", "34": "淘剧场", "35": "淘baby", "36": "淘精彩",
    "37": "萌宠TV", "38": "优漫卡通"
}

FOREIGN_KEYWORDS = [
    "BBC", "CNN", "NBC", "FOX", "HBO", "Netflix", "Disney",
    "欧美", "美国", "英国", "法国", "德国", "日本", "韩国",
    "泰国", "越南", "印尼", "马来西亚", "新加坡", "澳洲",
    "欧洲", "美洲", "非洲", "俄罗斯", "印度", "巴西"
]

DOMESTIC_SPECIAL = ["popc", "淘", "new", "NEW", "POPC", "超级电影", "IPTV", "new系列", "NewTV"]
# ==================================================

class EPGGenerator:
    def __init__(self):
        self.session = self._create_session()
        self.channel_ids: Set[str] = set()
        self.all_channels: List = []
        self.all_programs: List = []
        self.name_to_final_id: Dict[str, str] = dict()
        self.program_channel_map: Dict[str, str] = dict()
        self.prog_key_set: Set[Tuple[str, str, str]] = set()

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
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/xml, */*",
            "Accept-Encoding": "gzip, deflate"
        })
        return session

    def clean_ch_name(self, name: str) -> str:
        name = name.strip()
        name = re.sub(r'\s+|-', '', name)
        name = re.sub(r'(高清|HD|hd|4K|超清|综合|频道)', '', name)
        return name

    def read_epg_sources(self) -> List[Tuple[str, bool]]:
        """返回 (url, 是否+8小时)"""
        if not os.path.exists(CONFIG_FILE):
            logging.error(f"配置文件不存在: {CONFIG_FILE}")
            raise FileNotFoundError(f"找不到配置文件: {CONFIG_FILE}")
        sources = []
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                for line_num, raw_line in enumerate(f, 1):
                    raw_line = raw_line.strip()
                    if not raw_line or raw_line.startswith("#"):
                        continue
                    parts = raw_line.split("#")
                    url = parts[0].strip()
                    need_add8 = False
                    if len(parts) > 1:
                        tag = parts[1].strip()
                        if tag == "tz8":
                            need_add8 = True
                    if url.startswith(("http://", "https://")):
                        sources.append((url, need_add8))
                    else:
                        logging.warning(f"第{line_num}行无效链接跳过：{url}")
        except Exception as e:
            logging.error(f"读取配置失败：{str(e)}")
            raise
        logging.info(f"读取到 {len(sources)} 个EPG源")
        return sources

    def is_foreign_channel(self, name: str) -> bool:
        name_low = name.lower()
        for kw in DOMESTIC_SPECIAL:
            if kw.lower() in name_low:
                return False
        for kw in FOREIGN_KEYWORDS:
            if kw.lower() in name_low:
                return True
        return False

    def fix_xml_time(self, timestr: str, add8: bool) -> str:
        if not add8:
            return timestr
        if not timestr or len(timestr) < 14:
            return timestr
        try:
            dt = datetime.strptime(timestr[:14], "%Y%m%d%H%M%S")
            dt = dt + timedelta(hours=8)
            return dt.strftime("%Y%m%d%H%M%S") + " +0800"
        except Exception:
            return timestr

    def fetch_and_parse_xml(self, url: str):
        try:
            resp = self.session.get(url, timeout=TIMEOUT)
            resp.raise_for_status()
            raw_data = resp.content
            if raw_data.startswith(b'\x1f\x8b'):
                raw_data = gzip.decompress(raw_data)
            parser = etree.XMLParser(recover=True, encoding='utf-8')
            root = etree.fromstring(raw_data, parser=parser)
            return root
        except Exception as e:
            logging.error(f"拉取/解析源失败【{url}】：{str(e)}")
            return None

    def parse_one_source(self, url: str, add8: bool):
        root = self.fetch_and_parse_xml(url)
        if root is None:
            return
        for ch in root.xpath('//channel'):
            cid = ch.get('id')
            name_nodes = ch.xpath('./display-name/text()')
            if not name_nodes:
                continue
            ch_name = name_nodes[0].strip()
            if self.is_foreign_channel(ch_name):
                logging.info(f"过滤境外频道：{ch_name}")
                continue
            clean_name = self.clean_ch_name(ch_name)
            final_id = None
            for mid, mname in COOL9_ID_MAPPING.items():
                clean_map_name = self.clean_ch_name(mname)
                if clean_map_name in clean_name or clean_name in clean_map_name:
                    final_id = mid
                    break
            if final_id is None:
                final_id = str(len(self.name_to_final_id) + 1000)
            self.name_to_final_id[ch_name] = final_id
            if final_id not in self.channel_ids:
                self.channel_ids.add(final_id)
                new_ch = etree.Element("channel", attrib={"id": final_id})
                dn = etree.SubElement(new_ch, "display-name")
                dn.text = ch_name
                self.all_channels.append(new_ch)
            self.program_channel_map[cid] = final_id

        for prog in root.xpath('//programme'):
            orig_cid = prog.get('channel')
            if orig_cid not in self.program_channel_map:
                continue
            new_cid = self.program_channel_map[orig_cid]
            start_raw = prog.get("start", "")
            stop_raw = prog.get("stop", "")
            start_fixed = self.fix_xml_time(start_raw, add8)
            stop_fixed = self.fix_xml_time(stop_raw, add8)
            prog.set("channel", new_cid)
            prog.set("start", start_fixed)
            prog.set("stop", stop_fixed)

            title_nodes = prog.xpath('./title/text()')
            title = title_nodes[0].strip() if title_nodes else ""
            key = (new_cid, start_fixed, title)
            if key not in self.prog_key_set:
                self.prog_key_set.add(key)
                self.all_programs.append(prog)

    def run_fetch_all(self, source_list: List[Tuple[str, bool]]):
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            task_map = {
                executor.submit(self.parse_one_source, url, add8): (url, add8)
                for url, add8 in source_list
            }
            for task in as_completed(task_map):
                url, add8 = task_map[task]
                try:
                    task.result()
                except Exception as e:
                    logging.error(f"任务异常：{url} | {e}")

    def build_final_xml(self, save_path: str):
        tv_root = etree.Element("tv")
        tv_root.set("generator-info-name", "EPG Merge Script")
        for ch in self.all_channels:
            tv_root.append(ch)
        for p in self.all_programs:
            tv_root.append(p)
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        tree = etree.ElementTree(tv_root)
        tree.write(save_path, encoding="utf-8", xml_declaration=True, pretty_print=True)
        logging.info(f"✅ EPG文件已保存至：{save_path}")
        if GENERATE_GZ:
            gz_path = save_path + ".gz"
            with open(save_path, "rb") as f_in:
                with gzip.open(gz_path, "wb") as f_out:
                    f_out.writelines(f_in)
            logging.info(f"✅ 压缩包已保存：{gz_path}")

    def start(self):
        logging.info(f"当前系统时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        sources = self.read_epg_sources()
        if not sources:
            logging.warning("没有任何EPG源，直接退出")
            return
        self.run_fetch_all(sources)
        out_file = os.path.join(OUTPUT_DIR, "epg.xml")
        self.build_final_xml(out_file)

if __name__ == "__main__":
    generator = EPGGenerator()
    generator.start()
