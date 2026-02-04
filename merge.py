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

# 全局初始化OpenCC（修复重复初始化）
cc = OpenCC("t2s")
TZ_UTC_PLUS_8 = timezone(timedelta(hours=8))
# EPG标准时间格式（修复时区格式问题）
EPG_TIME_FORMAT = "%Y%m%d%H%M%S%z"

# 纯国外频道关键字列表
FOREIGN_KEYWORDS = [
    'CNN', 'BBC', 'FOX', 'HBO', 'ESPN', 'NHK', 'KBS', 'MBC', 'SBS'  # 省略其余关键字
]


def is_foreign_channel(channel_name):
    if not channel_name:
        return False
    channel_name_upper = channel_name.upper()
    for keyword in FOREIGN_KEYWORDS:
        if keyword.upper() in channel_name_upper:
            print(f"屏蔽国外频道: {channel_name} (匹配关键字: {keyword})")
            return True
    return False


def transform2_zh_hans(string):
    return cc.convert(string)  # 复用全局实例


async def fetch_epg(url):
    # 添加超时配置（30秒）
    timeout = aiohttp.ClientTimeout(total=30)
    connector = aiohttp.TCPConnector(limit=16, ssl=False)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
    }
    try:
        async with aiohttp.ClientSession(connector=connector, trust_env=True, headers=headers, timeout=timeout) as session:
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
        print(f"解析XML错误: {e}")
        print(f"问题内容: {epg_content[:500]}")
        return {}, defaultdict(list)

    channels = {}
    programmes = defaultdict(list)

    for channel in root.findall('channel'):
        channel_id = transform2_zh_hans(channel.get('id'))
        channel_display_names = []
        for name in channel.findall('display-name'):
            name_text = transform2_zh_hans(name.text.strip()) if name.text else ""
            channel_display_names.append([name_text, name.get('lang', 'zh')])
        if not channel_id.isdigit() and channel_id not in [d[0] for d in channel_display_names]:
            channel_display_names.append([channel_id, 'zh'])
        channels[channel_id] = channel_display_names

    for programme in root.findall('programme'):
        channel_id = transform2_zh_hans(programme.get('channel'))
        # 修复时间格式解析
        try:
            start_str = re.sub(r'\s+', '', programme.get('start'))
            stop_str = re.sub(r'\s+', '', programme.get('stop'))
            channel_start = datetime.strptime(start_str, EPG_TIME_FORMAT)
            channel_stop = datetime.strptime(stop_str, EPG_TIME_FORMAT)
        except ValueError as e:
            print(f"时间解析错误: {e} (start={start_str}, stop={stop_str})")
            continue

        # 转换时区并格式化（符合EPG标准）
        channel_start_cn = channel_start.astimezone(TZ_UTC_PLUS_8)
        channel_stop_cn = channel_stop.astimezone(TZ_UTC_PLUS_8)
        start_formatted = channel_start_cn.strftime(EPG_TIME_FORMAT).replace(' ', '')
        stop_formatted = channel_stop_cn.strftime(EPG_TIME_FORMAT).replace(' ', '')

        # 新建独立的programme节点（避免依赖原root）
        prog_elem = ET.Element(
            'programme',
            attrib={"channel": channel_id, "start": start_formatted, "stop": stop_formatted}
        )
        # 处理title
        for title in programme.findall('title'):
            title_text = title.text.strip() if title.text else "精彩节目"
            lang_attr = title.get('lang')
            if lang_attr in ['zh', None]:
                title_text = transform2_zh_hans(title_text)
            title_elem = ET.SubElement(prog_elem, 'title')
            title_elem.text = title_text
            if lang_attr:
                title_elem.set('lang', lang_attr)
        # 处理desc
        for desc in programme.findall('desc'):
            if not desc.text:
                continue
            desc_text = desc.text.strip()
            lang_attr = desc.get('lang')
            if lang_attr in ['zh', None]:
                desc_text = transform2_zh_hans(desc_text)
            desc_elem = ET.SubElement(prog_elem, 'desc')
            desc_elem.text = desc_text
            if lang_attr:
                desc_elem.set('lang', lang_attr)
        programmes[channel_id].append(prog_elem)

    return channels, programmes


def write_to_xml(channels_id, channels_names, programmes, filename):
    if not os.path.exists('output'):
        os.makedirs('output')
    current_time = datetime.now(TZ_UTC_PLUS_8).strftime("%Y%m%d%H%M%S%z").replace(' ', '')
    root = ET.Element('tv', attrib={'date': current_time})
    for channel_id in channels_id:
        channel_elem = ET.SubElement(root, 'channel', attrib={"id": channel_id})
        # 去重display-name
        seen_names = set()
        for display_name_node in channels_names[channel_id]:
            display_name, lang_attr = display_name_node
            if display_name in seen_names:
                continue
            seen_names.add(display_name)
            display_name_elem = ET.SubElement(channel_elem, 'display-name', attrib={"lang": lang_attr})
            display_name_elem.text = display_name
        # 添加programme
        for prog in programmes[channel_id]:
            prog.set('channel', channel_id)
            root.append(prog)

    # 美化XML
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
    if not os.path.exists('config.txt'):
        print("警告: config.txt不存在")
        return urls
    with open('config.txt', 'r', encoding='utf-8') as file:
        for line in file:
            line = line.strip()
            if line and not line.startswith('#'):
                urls.append(line)
    return urls


async def main():
    urls = get_urls()
    if not urls:
        print("无有效URL，退出")
        return
    tasks = [fetch_epg(url) for url in urls]
    print("正在抓取EPG数据...")
    epg_contents = await tqdm_asyncio.gather(*tasks, desc="抓取进度")
    print("抓取完成")

    all_channels_map = {}  # 频道ID/名称 -> 统一ID
    all_channel_ids = set()
    all_channel_names = defaultdict(list)
    all_programmes = defaultdict(list)

    for idx, epg_content in enumerate(epg_contents, 1):
        print(f"处理EPG源 {idx}/{len(epg_contents)}")
        if not epg_content:
            continue
        channels, programmes = parse_epg(epg_content)
        with tqdm(total=len(channels), desc="合并进度", unit="频道") as pbar:
            for channel_id, display_names in channels.items():
                # 过滤国外频道
                is_foreign = any(is_foreign_channel(name[0]) for name in display_names)
                if is_foreign:
                    pbar.update(1)
                    continue
                # 过滤无节目频道
                if not programmes.get(channel_id):
                    pbar.update(1)
                    continue
                # 频道合并（优先保留节目多的）
                unified_id = None
                # 检查是否已存在该频道
                for name in [channel_id] + [d[0] for d in display_names]:
                    if name in all_channels_map:
                        unified_id = all_channels_map[name]
                        break
                if unified_id:
                    # 若已有该频道，保留节目多的版本
                    if len(programmes[channel_id]) > len(all_programmes[unified_id]):
                        all_programmes[unified_id] = programmes[channel_id]
                        # 合并display-name
                        for name_node in display_names:
                            name = name_node[0]
                            if name not in [d[0] for d in all_channel_names[unified_id]]:
                                all_channel_names[unified_id].append(name_node)
                                all_channels_map[name] = unified_id
                else:
                    # 新频道，添加到集合
                    unified_id = channel_id
                    all_channel_ids.add(unified_id)
                    all_channel_names[unified_id] = display_names
                    all_programmes[unified_id] = programmes[channel_id]
                    # 记录映射
                    all_channels_map[unified_id] = unified_id
                    for name_node in display_names:
                        all_channels_map[name_node[0]] = unified_id
                pbar.update(1)

    # 输出统计信息
    print(f"\n最终合并结果：{len(all_channel_ids)}个频道，共{sum(len(p) for p in all_programmes.values())}个节目")
    # 写入文件
    print("写入XML文件...")
    write_to_xml(all_channel_ids, all_channel_names, all_programmes, 'output/epg.xml')
    compress_to_gz('output/epg.xml', 'output/epg.gz')
    print("完成")


if __name__ == '__main__':
    asyncio.run(main())
