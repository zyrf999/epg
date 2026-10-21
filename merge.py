import os
import gzip
import re
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
GENERATE_GZ = True

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# 频道数字ID映射表（和你m3u里tvg-id对应，不能删！）
# 每条 = (你的数字ID, 该频道在EPG源里可能出现的各种名字)
MAP_LIST = [
    ("1",  ["CCTV1", "CCTV-1", "CCTV 1", "CCTV1综合", "中央一台", "中央1台", "CCTV-1综合"]),
    ("2",  ["CCTV2", "CCTV-2", "CCTV 2", "财经", "中央二台", "中央2台"]),
    ("3",  ["CCTV3", "CCTV-3", "CCTV 3", "综艺", "中央三台", "中央3台"]),
    ("4",  ["CCTV4", "CCTV-4", "CCTV 4", "国际", "中文国际", "中央四台", "中央4台"]),
    ("5",  ["CCTV5", "CCTV-5", "CCTV 5", "体育", "中央五台", "中央5台"]),
    ("6",  ["CCTV6", "CCTV-6", "CCTV 6", "电影", "中央六台", "中央6台"]),
    ("7",  ["CCTV7", "CCTV-7", "CCTV 7", "国防军事", "军事", "中央七台", "中央7台"]),
    ("8",  ["CCTV8", "CCTV-8", "CCTV 8", "电视剧", "中央八台", "中央8台"]),
    ("9",  ["CCTV9", "CCTV-9", "CCTV 9", "纪录", "中央九台", "中央9台"]),
    ("10", ["CCTV10", "CCTV-10", "CCTV 10", "科教", "中央十台", "中央10台"]),
    ("11", ["CCTV11", "CCTV-11", "CCTV 11", "戏曲", "中央十一台", "中央11台"]),
    ("12", ["CCTV12", "CCTV-12", "CCTV 12", "社会与法", "法制", "中央十二台", "中央12台"]),
    ("13", ["CCTV13", "CCTV-13", "CCTV 13", "新闻", "新闻频道", "中央十三台", "中央13台"]),
    ("14", ["CCTV14", "CCTV-14", "CCTV 14", "少儿", "儿童", "中央十四台", "中央14台"]),
    ("15", ["CCTV15", "CCTV-15", "CCTV 15", "音乐", "中央十五台", "中央15台"]),
    ("16", ["CCTV16", "CCTV-16", "CCTV 16", "奥林匹克", "奥运"]),
    ("21", ["北京卫视", "北京卫视HD", "北京卫视高清", "BTV北京", "BTV-北京"]),
    ("22", ["河南卫视", "河南卫视HD", "河南卫视高清"]),
    ("23", ["河北卫视", "河北卫视HD", "河北卫视高清"]),
    ("24", ["湖北卫视", "湖北卫视HD", "湖北卫视高清"]),
    ("25", ["海南卫视", "海南卫视HD", "海南卫视高清", "旅游卫视"]),
    ("26", ["贵州卫视", "贵州卫视HD", "贵州卫视高清"]),
    ("27", ["厦门卫视", "厦门卫视HD", "厦门卫视高清"]),
    ("28", ["CCTV5+", "CCTV5Plus", "CCTV5+体育", "体育赛事"]),
    ("29", ["峨眉电影"]),
    ("30", ["峨眉电影4K"]),
    ("31", ["北京IPTV4K超清", "北京IPTV4K"]),
    ("32", ["淘电影"]),
    ("33", ["淘娱乐"]),
    ("34", ["淘剧场"]),
    ("35", ["淘baby", "淘Baby", "淘宝贝"]),
    ("36", ["淘精彩"]),
    ("37", ["萌宠TV", "萌宠tv"]),
    ("38", ["优漫卡通", "优漫"]),
]

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
        # 源原生channel id -> 你的数字ID
        self.src_id_to_my_id: Dict[str, str] = {}
        # 你的数字ID -> channel节点（去重）
        self.my_id_channel: Dict[str, etree.Element] = {}
        self.program_list: List[etree.Element] = []
        self.prog_uniq: Set[Tuple[str, str, str]] = set()
        # 预构建：清洗后的别名 -> 数字ID
        self.alias_exact: Dict[str, str] = {}
        for my_id, aliases in MAP_LIST:
            for a in aliases:
                self.alias_exact[self._clean(a)] = my_id

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=CORE_RETRY_COUNT + 2,
            backoff_factor=1.5,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/xml, */*",
            "Accept-Encoding": "gzip, deflate"
        })
        return session

    def _clean(self, name: str) -> str:
        """清洗频道名：去空格、横杠、高清/HD/4K/超清/综合/频道等后缀"""
        s = name.strip()
        s = re.sub(r'[\s\-—_]+', '', s)
        s = re.sub(r'(高清|HD|hd|4K|超清|全高清|FHD|fhd|综合|频道|标清|PLUS|plus|Plus)', '', s)
        return s

    def match_my_id(self, origin_name: str) -> str:
        """匹配数字ID。策略：
        1) 清洗后精确命中别名表
        2) 遍历别名，长别名优先匹配（防止CCTV1吃掉CCTV11/13）
        3) 都不命中则返回None，该频道直接跳过（不输出）
        """
        raw = self._clean(origin_name)
        if raw in self.alias_exact:
            return self.alias_exact[raw]
        # 长别名优先：把所有别名按长度降序排
        for my_id, aliases in MAP_LIST:
            for a in sorted(aliases, key=len, reverse=True):
                ca = self._clean(a)
                if ca and (ca in raw or raw in ca):
                    return my_id
        return None

    def read_source_list(self) -> List[Tuple[str, bool]]:
        sources = []
        if not os.path.exists(CONFIG_FILE):
            logging.error(f"找不到配置文件 {CONFIG_FILE}")
            raise FileNotFoundError(CONFIG_FILE)
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            for line_no, raw in enumerate(f, 1):
                raw = raw.strip()
                if not raw or raw.startswith("#"):
                    continue
                parts = raw.split("#")
                url = parts[0].strip()
                need_add8 = False
                if len(parts) > 1 and parts[1].strip() == "tz8":
                    need_add8 = True
                if url.startswith(("http://", "https://")):
                    sources.append((url, need_add8))
                else:
                    logging.warning(f"第{line_no}行无效链接跳过：{url}")
        logging.info(f"读取到 {len(sources)} 个EPG源")
        return sources

    def is_foreign(self, name: str) -> bool:
        name_low = name.lower()
        for kw in DOMESTIC_SPECIAL:
            if kw.lower() in name_low:
                return False
        for kw in FOREIGN_KEYWORDS:
            if kw.lower() in name_low:
                return True
        return False

    def time_shift(self, timestr: str, add8: bool) -> str:
        """仅标记 tz8 的源做UTC+8；其余原样返回"""
        if not add8:
            return timestr
        if not timestr or len(timestr) < 14:
            return timestr
        try:
            dt = datetime.strptime(timestr[:14], "%Y%m%d%H%M%S")
            dt += timedelta(hours=8)
            return dt.strftime("%Y%m%d%H%M%S") + " +0800"
        except Exception:
            return timestr

    def fetch_xml(self, url: str):
        try:
            resp = self.session.get(url, timeout=TIMEOUT)
            resp.raise_for_status()
            data = resp.content
            if data.startswith(b'\x1f\x8b'):
                data = gzip.decompress(data)
            parser = etree.XMLParser(recover=True, encoding="utf-8")
            root = etree.fromstring(data, parser=parser)
            return root
        except Exception as e:
            logging.error(f"拉取解析失败【{url}】：{e}")
            return None

    def parse_one(self, url: str, add8: bool):
        root = self.fetch_xml(url)
        if root is None:
            return
        # 1) 处理channel：源id -> 你的数字ID
        for ch in root.xpath("//channel"):
            src_id = ch.get("id")
            name_nodes = ch.xpath("./display-name/text()")
            if not name_nodes:
                continue
            ch_name = name_nodes[0].strip()
            if self.is_foreign(ch_name):
                continue
            my_id = self.match_my_id(ch_name)
            if my_id is None:
                continue  # 不在映射表里的频道，直接不输出
            self.src_id_to_my_id[src_id] = my_id
            if my_id not in self.my_id_channel:
                new_ch = etree.Element("channel", attrib={"id": my_id})
                dn = etree.SubElement(new_ch, "display-name")
                dn.text = ch_name
                self.my_id_channel[my_id] = new_ch
                logging.info(f"频道映射成功：{ch_name} -> {my_id}")
        # 2) 处理programme：把channel改成你的数字ID，按需+8小时
        for prog in root.xpath("//programme"):
            src_id = prog.get("channel")
            if src_id not in self.src_id_to_my_id:
                continue
            my_id = self.src_id_to_my_id[src_id]
            s_raw = prog.get("start", "")
            e_raw = prog.get("stop", "")
            s_new = self.time_shift(s_raw, add8)
            e_new = self.time_shift(e_raw, add8)
            prog.set("channel", my_id)
            prog.set("start", s_new)
            prog.set("stop", e_new)
            title_nodes = prog.xpath("./title/text()")
            title = title_nodes[0].strip() if title_nodes else ""
            key = (my_id, s_new, title)
            if key not in self.prog_uniq:
                self.prog_uniq.add(key)
                self.program_list.append(prog)

    def run_all(self, src_list: List[Tuple[str, bool]]):
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
            task_map = {exe.submit(self.parse_one, u, t): (u, t) for u, t in src_list}
            for task in as_completed(task_map):
                u, t = task_map[task]
                try:
                    task.result()
                except Exception as e:
                    logging.error(f"任务异常：{u} | {e}")

    def build_xml(self, out_path: str):
        tv = etree.Element("tv")
        tv.set("generator-info-name", "EPG Merge")
        for ch in self.my_id_channel.values():
            tv.append(ch)
        for p in self.program_list:
            tv.append(p)
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        tree = etree.ElementTree(tv)
        tree.write(out_path, encoding="utf-8", xml_declaration=True, pretty_print=True)
        logging.info(f"✅ EPG已保存：{out_path}（频道数={len(self.my_id_channel)}，节目数={len(self.program_list)}）")
        if GENERATE_GZ:
            gz_path = out_path + ".gz"
            with open(out_path, "rb") as f_in:
                with gzip.open(gz_path, "wb") as f_out:
                    f_out.writelines(f_in)
            logging.info(f"✅ 压缩包已保存：{gz_path}")

    def start(self):
        logging.info(f"启动，系统时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        srcs = self.read_source_list()
        if not srcs:
            logging.warning("没有可用EPG源")
            return
        self.run_all(srcs)
        out_file = os.path.join(OUTPUT_DIR, "epg.xml")
        self.build_xml(out_file)

if __name__ == "__main__":
    generator = EPGGenerator()
    generator.start()
