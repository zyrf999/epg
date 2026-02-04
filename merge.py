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

# 全局初始化
cc = OpenCC("t2s")
TZ_UTC_PLUS_8 = timezone(timedelta(hours=8))
EPG_TIME_FORMAT = "%Y%m%d%H%M%S%z"

# 国外频道关键字
FOREIGN_KEYWORDS = [
    'CNN', 'BBC', 'FOX', 'HBO', 'ESPN', 'NHK', 'KBS', 'MBC', 'SBS'
]


def is_foreign_channel(channel_name):
    if not channel_name:
        return False
    return any(keyword.upper() in channel_name.upper() for keyword in FOREIGN_KEYWORDS)


def transform2_zh_hans(string):
    return cc.convert(string)


async def fetch_epg(url):
    timeout = aiohttp.ClientTimeout(total=30)
    connector = aiohttp.TCPConnector(limit=16, ssl=False)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
    }
    try:
        async with aiohttp.ClientSession(connector=connector, headers=headers, timeout=timeout) as session:
            async with session.get(url) as response:
                if url.endswith('.gz'):
                    return gzip.decompress(await response.read()).decode('utf-8', errors='ignore')
                return await response.text(encoding='utf-8')
    except Exception as e:
        print(f"⚠️ {url} 抓取失败: {str(e)}")
        return None


def parse_epg(epg_content):
    try:
        root = ET.fromstring(epg_content, ET.XMLParser(encoding='UTF-8'))
    except Exception as e:
        print(f"⚠️ XML解析失败: {str(e)}")
        return {}, defaultdict(list)

    channels = {}
    programmes = defaultdict(list)

    for channel in root.findall('channel'):
        channel_id = transform2_zh_hans(channel.get('id') or '')
        display_names = []
        for name in channel.findall('display-name'):
            name_text = transform2_zh_hans(name.text.strip()) if name.text else ""
            display_names.append([name_text, name.get('lang', 'zh')])
        if channel_id and channel_id not in [d[0] for d in display_names]:
            display_names.append([channel_id, 'zh'])
        if channel_id:
            channels[channel_id] = display_names

    for programme in root.findall('programme'):
        channel_id = transform2_zh_hans(programme.get('channel') or '')
        if not channel_id:
            continue
        try:
            start_str = re.sub(r'\s+', '', programme.get('start') or '')
            stop_str = re.sub(r'\s+', '', programme.get('stop') or '')
            start = datetime.strptime(start_str, EPG_TIME_FORMAT).astimezone(TZ_UTC_PLUS_8)
            stop = datetime.strptime(stop_str, EPG_TIME_FORMAT).astimezone(TZ_UTC_PLUS_8)
        except Exception as e:
            print(f"⚠️ 时间解析失败: {str(e)}")
            continue

        prog_elem = ET.Element(
            'programme',
            attrib={
                "channel": channel_id,
                "start": start.strftime(EPG_TIME_FORMAT).replace(' ', ''),
                "stop": stop.strftime(EPG_TIME_FORMAT).replace(' ', '')
            }
        )
        # 处理title
        for title in programme.findall('title'):
            title_text = transform2_zh_hans(title.text.strip()) if title.text else "精彩节目"
            title_elem = ET.SubElement(prog_elem, 'title')
            title_elem.text = title_text
            if title.get('lang'):
                title_elem.set('lang', title.get('lang'))
        # 处理desc
        for desc in programme.findall('desc'):
            if desc.text:
                desc_elem = ET.SubElement(prog_elem, 'desc')
                desc_elem.text = transform2_zh_hans(desc.text.strip())
                if desc.get('lang'):
                    desc_elem.set('lang', desc.get('lang'))
        programmes[channel_id].append(prog_elem)

    return channels, programmes


def write_to_xml(channels, programmes):
    if not os.path.exists('output'):
        os.makedirs('output')
    root = ET.Element('tv', attrib={
        'date': datetime.now(TZ_UTC_PLUS_8).strftime(EPG_TIME_FORMAT).replace(' ', '')
    })
    # 添加频道
    for channel_id, display_names in channels.items():
        channel_elem = ET.SubElement(root, 'channel', attrib={"id": channel_id})
        seen_names = set()
        for name, lang in display_names:
            if name not in seen_names:
                seen_names.add(name)
                ET.SubElement(channel_elem, 'display-name', attrib={"lang": lang}).text = name
    # 添加节目
    for channel_id, progs in programmes.items():
        for prog in progs:
            prog.set('channel', channel_id)
            root.append(prog)
    # 美化并写入
    rough_xml = ET.tostring(root, 'utf-8')
    pretty_xml = minidom.parseString(rough_xml).toprettyxml(indent='\t', newl='\n')
    with open('output/epg.xml', 'w', encoding='utf-8') as f:
        f.write(pretty_xml)
    # 压缩
    with open('output/epg.xml', 'rb') as f_in, gzip.open('output/epg.gz', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)


def get_urls():
    if not os.path.exists('config.txt'):
        print("⚠️ config.txt不存在")
        return []
    with open('config.txt', 'r', encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip() and not line.startswith('#')]


async def main():
    urls = get_urls()
    if not urls:
        print("❌ 无有效数据源URL")
        return

    print("=== 开始抓取EPG数据 ===")
    tasks = [fetch_epg(url) for url in urls]
    epg_contents = await tqdm_asyncio.gather(*tasks, desc="抓取进度")
    print("=== 抓取完成 ===")

    all_channels = {}
    all_programmes = defaultdict(list)
    channel_map = {}  # 频道ID/名称 -> 统一ID

    for idx, content in enumerate(epg_contents, 1):
        if not content:
            print(f"⚠️ 数据源 {idx} 无内容")
            continue
        print(f"=== 处理数据源 {idx}/{len(urls)} ===")
        channels, programmes = parse_epg(content)
        for channel_id, display_names in channels.items():
            # 过滤国外频道
            if any(is_foreign_channel(name) for name, _ in display_names):
                continue
            # 过滤无节目频道
            if not programmes.get(channel_id):
                continue
            # 合并频道
            unified_id = None
            # 检查是否已存在该频道
            for name in [channel_id] + [n for n, _ in display_names]:
                if name in channel_map:
                    unified_id = channel_map[name]
                    break
            if unified_id:
                # 保留节目多的版本
                if len(programmes[channel_id]) > len(all_programmes[unified_id]):
                    all_programmes[unified_id] = programmes[channel_id]
                    # 合并频道名称
                    for name, lang in display_names:
                        if name not in [n for n, _ in all_channels[unified_id]]:
                            all_channels[unified_id].append([name, lang])
                            channel_map[name] = unified_id
            else:
                # 新增频道
                unified_id = channel_id
                all_channels[unified_id] = display_names
                all_programmes[unified_id] = programmes[channel_id]
                channel_map[unified_id] = unified_id
                for name, lang in display_names:
                    channel_map[name] = unified_id

    # 统计并输出
    channel_count = len(all_channels)
    programme_count = sum(len(p) for p in all_programmes.values())
    print(f"\n=== 合并结果 ===")
    print(f"频道数: {channel_count}")
    print(f"节目数: {programme_count}")

    if channel_count == 0 or programme_count == 0:
        print("❌ 未生成有效EPG数据")
        return

    # 写入文件
    print("\n=== 写入EPG文件 ===")
    write_to_xml(all_channels, all_programmes)
    print("=== 操作完成 ===")


if __name__ == '__main__':
    asyncio.run(main())
