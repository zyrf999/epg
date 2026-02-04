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

# 优化过滤规则：只过滤纯国外频道，避免误杀国内频道（参考你朋友的精准匹配思路）
FOREIGN_CHANNELS = {
    'CNN', 'BBC', 'FOX', 'HBO', 'ESPN', 'NHK', 'KBS', 'MBC', 'SBS',
    '日本テレビ', 'テレビ朝日', 'TBSテレビ', 'フジテレビ', 'JTBC', 'TV조선'
}

def is_foreign_channel(channel_name):
    """精准过滤：只匹配完全一致的纯国外频道名"""
    if not channel_name:
        return False
    return channel_name.strip() in FOREIGN_CHANNELS

def transform2_zh_hans(string):
    cc = OpenCC("t2s")
    new_str = cc.convert(string)
    return new_str

async def fetch_epg(url):
    connector = aiohttp.TCPConnector(limit=16, ssl=False)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
    }
    try:
        async with aiohttp.ClientSession(connector=connector, trust_env=True, headers=headers) as session:
            async with session.get(url) as response:
                if url.endswith('.gz'):
                    compressed_data = await response.read()
                    return gzip.decompress(compressed_data).decode('utf-8', errors='ignore')
                else:
                    return await response.text(encoding='utf-8')
    except aiohttp.ClientError as e:
        print(f"{url} HTTP请求错误: {e}")
    except asyncio.TimeoutError:
        print(f"{url} 请求超时")
    except Exception as e:
        print(f"{url} 其他错误: {e}")
    return None

def parse_epg(epg_content):
    try:
        parser = ET.XMLParser(encoding='UTF-8')
        root = ET.fromstring(epg_content, parser=parser)
    except ET.ParseError as e:
        print(f"Error parsing XML: {e}")
        print(f"Problematic content: {epg_content[:500]}")
        return {}, defaultdict(list)

    channels = {}
    programmes = defaultdict(list)

    for channel in root.findall('channel'):
        channel_id = transform2_zh_hans(channel.get('id'))
        channel_display_names = []
        for name in channel.findall('display-name'):
            name_text = transform2_zh_hans(name.text) if name.text else ""
            channel_display_names.append([name_text, name.get('lang', 'zh')])
        if not channel_id.isdigit() and channel_id not in [d[0] for d in channel_display_names]:
            channel_display_names.append([channel_id, 'zh'])
        channels[channel_id] = channel_display_names

    for programme in root.findall('programme'):
        channel_id = transform2_zh_hans(programme.get('channel'))
        # 修复：创建独立的programme节点，不依赖原root（避免节点污染）
        prog_elem = ET.Element('programme')
        prog_elem.attrib.update(programme.attrib)
        
        channel_start = datetime.strptime(
            re.sub(r'\s+', '', programme.get('start')), "%Y%m%d%H%M%S%z")
        channel_stop = datetime.strptime(
            re.sub(r'\s+', '', programme.get('stop')), "%Y%m%d%H%M%S%z")
        channel_start = channel_start.astimezone(TZ_UTC_PLUS_8)
        channel_stop = channel_stop.astimezone(TZ_UTC_PLUS_8)
        # 修复：时间格式符合EPG标准（无空格）
        prog_elem.set("start", channel_start.strftime("%Y%m%d%H%M%S%z").replace(' ', ''))
        prog_elem.set("stop", channel_stop.strftime("%Y%m%d%H%M%S%z").replace(' ', ''))
        
        for title in programme.findall('title'):
            if title.text is None:
                channel_title = "精彩节目"
            else:
                channel_title = title.text.strip()
            langattr = title.get('lang')
            if langattr == 'zh' or langattr is None:
                channel_title = transform2_zh_hans(channel_title)
            channel_elem_t = ET.SubElement(prog_elem, 'title')
            channel_elem_t.text = channel_title
            if langattr is not None:
                channel_elem_t.set('lang', langattr)
        for desc in programme.findall('desc'):
            if desc.text is None:
                continue
            langattr = desc.get('lang')
            channel_desc = desc.text.strip()
            if langattr == 'zh' or langattr is None:
                channel_desc = transform2_zh_hans(channel_desc)
            channel_elem_d = ET.SubElement(prog_elem, 'desc')
            channel_elem_d.text = channel_desc.strip()
            if langattr is not None:
                channel_elem_d.set('lang', langattr)
        programmes[channel_id].append(prog_elem)

    return channels, programmes

def write_to_xml(channels_id, channels_names, programmes, filename):
    if not os.path.exists('output'):
        os.makedirs('output')
    current_time = datetime.now(TZ_UTC_PLUS_8).strftime("%Y%m%d%H%M%S%z").replace(' ', '')
    root = ET.Element('tv', attrib={'date': current_time})
    for channel_id in channels_id:
        channel_elem = ET.SubElement(root, 'channel', attrib={"id": channel_id})
        for display_name_node in channels_names[channel_id]:
            display_name = display_name_node[0]
            langattr = display_name_node[1]
            display_name_elem = ET.SubElement(channel_elem, 'display-name', attrib={"lang": langattr})
            display_name_elem.text = display_name
        # 写入节目（此时programmes[channel_id]已正确映射）
        for prog in programmes[channel_id]:
            prog.set('channel', channel_id)
            root.append(prog)

    rough_string = ET.tostring(root, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(reparsed.toprettyxml(indent='\t', newl='\n'))

def compress_to_gz(input_filename, output_filename):
    with open(input_filename, 'rb') as f_in:
        with gzip.open(output_filename, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

def get_urls():
    urls = []
    with open('config.txt', 'r', encoding='utf-8') as file:
        for line in file:
            line = line.strip()
            if line and not line.startswith('#'):
                urls.append(line)
    return urls

async def main():
    urls = get_urls()
    tasks = [fetch_epg(url) for url in urls]
    print("Fetching EPG data...")
    epg_contents = await tqdm_asyncio.gather(*tasks, desc="Fetching URLs")
    all_channels_map = {}
    all_channel_id = set()
    all_channel_names = defaultdict(list)
    all_programmes = defaultdict(list)  # 键：channel_id，值：节目列表
    print("Finished.")
    i = 0
    for epg_content in epg_contents:
        i += 1
        print(f"Processing EPG source...{i}/{len(epg_contents)}")
        if epg_content is None:
            continue
        print("Parsing EPG data...")
        channels, programmes = parse_epg(epg_content)
        print("Finished.")
        with tqdm(total=len(channels), desc="Merging EPG", unit="file") as pbar:
            for channel_id, display_names in channels.items():
                # 精准过滤纯国外频道（保留国内所有频道，包括含英文关键词的）
                is_foreign = any(is_foreign_channel(name[0]) for name in display_names)
                if is_foreign:
                    pbar.update(1)
                    continue
                # 过滤无节目频道
                if len(programmes[channel_id]) == 0:
                    pbar.update(1)
                    continue
                # 频道合并逻辑（上游原版，无错误）
                is_in_map = channel_id in all_channels_map
                map_id = channel_id
                for display_name_node in display_names:
                    display_name = display_name_node[0]
                    if is_in_map:
                        break
                    is_in_map = is_in_map or (display_name in all_channels_map)
                    map_id = display_name
                map_id = all_channels_map.get(map_id, channel_id)
                if not is_in_map:
                    all_channel_id.add(channel_id)
                    all_channel_names[channel_id] = display_names
                    # 修复：节目映射到 channel_id（而非 display_name）
                    all_programmes[channel_id] = programmes[channel_id]
                    all_channels_map[channel_id] = channel_id
                    for display_name_node in display_names:
                        display_name = display_name_node[0]
                        all_channels_map[display_name] = channel_id
                elif len(all_programmes[map_id]) < len(programmes[channel_id]):
                    all_programmes[map_id] = programmes[channel_id]
                    for display_name_node in display_names:
                        display_name = display_name_node[0]
                        if display_name not in all_channels_map:
                            all_channel_names[map_id].append(display_name_node)
                            all_channels_map[display_name] = map_id
                pbar.update(1)
    print("Writing to XML...")
    write_to_xml(all_channel_id, all_channel_names, all_programmes, 'output/epg.xml')
    compress_to_gz('output/epg.xml', 'output/epg.gz')
    # 新增统计输出，方便验证
    total_channels = len(all_channel_id)
    total_programs = sum(len(p) for p in all_programmes.values())
    print(f"生成完成：{total_channels}个频道，{total_programs}个节目")

if __name__ == '__main__':
    asyncio.run(main())
