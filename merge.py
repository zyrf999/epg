import xml.etree.ElementTree as ET
from collections import defaultdict
import aiohttp
import asyncio
from tqdm.asyncio import tqdm_asyncio
from datetime import datetime, timezone, timedelta
import gzip
import shutil
from xml.dom import minidom
import re
from opencc import OpenCC
import os
from tqdm import tqdm

TZ_UTC_PLUS_8 = timezone(timedelta(hours=8))

# ========== 酷九播放器需要的数字ID映射（和你朋友的一致） ==========
COOL9_ID_MAPPING = {
    "山东卫视": "89", "山东教育": "221", "山东新闻": "381", 
    "山东农科": "382", "山东齐鲁": "383", "山东文旅": "384",
    "CCTV1": "1", "CCTV2": "2", "CCTV3": "3", "CCTV4": "4", 
    "CCTV5": "5", "CCTV6": "6", "CCTV7": "7", "CCTV8": "8",
    "CCTV9": "9", "CCTV10": "10"
}
# 国外频道过滤（保留国内频道）
FOREIGN_CHANNELS = {'CNN', 'BBC', 'FOX', 'HBO', 'ESPN', 'NHK', 'KBS', 'MBC', 'SBS'}
# ==============================================================

def is_foreign_channel(channel_name):
    return channel_name.strip() in FOREIGN_CHANNELS

def transform2_zh_hans(string):
    cc = OpenCC("t2s")
    return cc.convert(string)

async def fetch_epg(url):
    connector = aiohttp.TCPConnector(limit=16, ssl=False)
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"}
    try:
        async with aiohttp.ClientSession(connector=connector, trust_env=True, headers=headers) as session:
            async with session.get(url) as response:
                if url.endswith('.gz'):
                    return gzip.decompress(await response.read()).decode('utf-8', errors='ignore')
                return await response.text(encoding='utf-8')
    except Exception as e:
        print(f"{url} 错误: {e}")
        return None

def parse_epg(epg_content):
    try:
        root = ET.fromstring(epg_content, ET.XMLParser(encoding='UTF-8'))
    except Exception as e:
        print(f"解析XML错误: {e}")
        return {}, defaultdict(list)

    channels = {}
    programmes = defaultdict(list)

    for channel in root.findall('channel'):
        channel_id = transform2_zh_hans(channel.get('id'))
        display_names = []
        for name in channel.findall('display-name'):
            name_text = transform2_zh_hans(name.text) if name.text else ""
            display_names.append([name_text, name.get('lang', 'zh')])
        if not channel_id.isdigit() and channel_id not in [d[0] for d in display_names]:
            display_names.append([channel_id, 'zh'])
        channels[channel_id] = display_names

    for programme in root.findall('programme'):
        channel_id = transform2_zh_hans(programme.get('channel'))
        prog_elem = ET.Element('programme')
        prog_elem.attrib.update(programme.attrib)
        
        # 时间格式处理（符合EPG标准）
        start_str = re.sub(r'\s+', '', programme.get('start'))
        stop_str = re.sub(r'\s+', '', programme.get('stop'))
        try:
            start = datetime.strptime(start_str, "%Y%m%d%H%M%S%z").astimezone(TZ_UTC_PLUS_8)
            stop = datetime.strptime(stop_str, "%Y%m%d%H%M%S%z").astimezone(TZ_UTC_PLUS_8)
            prog_elem.set("start", start.strftime("%Y%m%d%H%M%S%z").replace(' ', ''))
            prog_elem.set("stop", stop.strftime("%Y%m%d%H%M%S%z").replace(' ', ''))
        except Exception as e:
            print(f"时间解析错误: {e}")
            continue
        
        # 处理节目标题/描述
        for title in programme.findall('title'):
            title_text = transform2_zh_hans(title.text.strip()) if title.text else "精彩节目"
            title_elem = ET.SubElement(prog_elem, 'title')
            title_elem.text = title_text
            if title.get('lang'):
                title_elem.set('lang', title.get('lang'))
        for desc in programme.findall('desc'):
            if desc.text:
                desc_elem = ET.SubElement(prog_elem, 'desc')
                desc_elem.text = transform2_zh_hans(desc.text.strip())
                if desc.get('lang'):
                    desc_elem.set('lang', desc.get('lang'))
        programmes[channel_id].append(prog_elem)

    return channels, programmes

def write_to_xml(channels, programmes, filename):
    if not os.path.exists('output'):
        os.makedirs('output')
    root = ET.Element('tv', attrib={'date': datetime.now(TZ_UTC_PLUS_8).strftime("%Y%m%d%H%M%S%z").replace(' ', '')})
    for channel_id, display_names in channels.items():
        # 核心：将频道ID转为酷九需要的数字格式
        final_cid = COOL9_ID_MAPPING.get(channel_id, channel_id)
        channel_elem = ET.SubElement(root, 'channel', attrib={"id": final_cid})
        for name, lang in display_names:
            ET.SubElement(channel_elem, 'display-name', attrib={"lang": lang}).text = name
    # 处理节目单：同步更新节目单的channel为数字ID
    for channel_id, progs in programmes.items():
        final_cid = COOL9_ID_MAPPING.get(channel_id, channel_id)
        for prog in progs:
            prog.set('channel', final_cid)
            root.append(prog)

    # 美化并写入
    rough_xml = ET.tostring(root, 'utf-8')
    pretty_xml = minidom.parseString(rough_xml).toprettyxml(indent='\t', newl='\n')
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(pretty_xml)
    # 压缩
    with open(filename, 'rb') as f_in, gzip.open(filename.replace('.xml', '.gz'), 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

def get_urls():
    urls = []
    with open('config.txt', 'r', encoding='utf-8') as f:
        urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]
    return urls

async def main():
    urls = get_urls()
    tasks = [fetch_epg(url) for url in urls]
    print("抓取EPG数据...")
    epg_contents = await tqdm_asyncio.gather(*tasks, desc="抓取进度")
    
    all_channels = defaultdict(list)  # 键：频道名称ID，值：display_names
    all_programmes = defaultdict(list)  # 键：频道名称ID，值：节目列表

    for idx, content in enumerate(epg_contents, 1):
        if not content:
            continue
        print(f"处理数据源 {idx}/{len(urls)}")
        channels, programmes = parse_epg(content)
        for channel_id, display_names in channels.items():
            # 过滤国外频道
            if any(is_foreign_channel(name[0]) for name in display_names):
                continue
            # 合并频道（去重）
            if channel_id not in all_channels:
                all_channels[channel_id] = display_names
            # 合并节目单（保留最新）
            if len(programmes[channel_id]) > len(all_programmes[channel_id]):
                all_programmes[channel_id] = programmes[channel_id]

    # 生成并保存EPG（自动转换数字ID）
    print("生成EPG文件...")
    write_to_xml(all_channels, all_programmes, 'output/epg.xml')
    # 统计输出
    total_channels = len(all_channels)
    total_programs = sum(len(p) for p in all_programmes.values())
    print(f"生成完成：{total_channels}个频道，{total_programs}个节目（已适配酷九数字ID）")

if __name__ == '__main__':
    asyncio.run(main())
