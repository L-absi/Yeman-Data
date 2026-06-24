import json
import csv
import time
import asyncio
import aiohttp
from pathlib import Path
from typing import Dict, List, Any, Set, Optional
from tqdm.asyncio import tqdm as async_tqdm

# -------------------- الإعدادات --------------------
BASE_DIR = Path(".")
STREAMS_DIR = BASE_DIR / "streams"
CHANNELS_DIR = BASE_DIR / "channels"

CONCURRENT_LIMIT = 200        # عدد الطلبات المتزامنة
TIMEOUT = aiohttp.ClientTimeout(total=3, connect=2, sock_read=2)  # وقت قصير جداً

# -------------------- دوال مساعدة --------------------
def load_json(path: Path) -> Any:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"❌ خطأ في قراءة {path}: {e}")
        return None

def build_headers(server: Dict) -> Dict[str, str]:
    headers = {}
    if server.get("Referer"):
        headers["Referer"] = server["Referer"]
    if server.get("userAgent"):
        headers["User-Agent"] = server["userAgent"]
    return headers

async def check_stream_async(session: aiohttp.ClientSession, url: str, headers: Dict = None) -> Dict[str, Any]:
    result = {
        "url": url,
        "status": "unknown",
        "working": False,
        "error": None
    }
    if not url:
        result["status"] = "no_url"
        result["error"] = "No URL provided"
        return result

    try:
        async with session.get(url, headers=headers, timeout=TIMEOUT) as resp:
            chunk = await resp.content.read(1)
            if resp.status == 200 and chunk:
                result["working"] = True
                result["status"] = 200
            else:
                result["status"] = resp.status
                result["error"] = f"Status {resp.status} or empty"
    except asyncio.TimeoutError:
        result["status"] = "timeout"
        result["error"] = "Connection timed out"
    except aiohttp.ClientConnectorError:
        result["status"] = "connection_error"
        result["error"] = "Failed to connect"
    except Exception as e:
        result["status"] = "error"
        result["error"] = str(e)

    return result

def update_channels_working_status(report_rows: List[Dict]):
    status_map = {}
    for row in report_rows:
        cf = row.get("channel_file")
        cid = row.get("channel_id")
        if cf is None or cid is None:
            continue
        key = (cf, cid)
        if key not in status_map:
            status_map[key] = False
        if row["working"]:
            status_map[key] = True

    for channel_file in CHANNELS_DIR.glob("*.json"):
        stem = channel_file.stem
        channels = load_json(channel_file)
        if not channels or not isinstance(channels, list):
            continue
        updated = False
        for ch in channels:
            ch_id = ch.get("id")
            if not ch_id:
                continue
            new_working = 1 if status_map.get((stem, ch_id), False) else 0
            if ch.get("isworking") != new_working:
                ch["isworking"] = new_working
                updated = True
        if updated:
            with open(channel_file, 'w', encoding='utf-8') as f:
                json.dump(channels, f, indent=2, ensure_ascii=False)
            print(f"✅ تم تحديث {channel_file.name}")

def collect_all_tasks(sections_dict: Dict[str, Dict]) -> List[Dict]:
    tasks = []

    def recurse_collect(sec_key: str, path_name: str, depth: int = 0):
        if depth > 10:
            return
        section = sections_dict.get(sec_key)
        if not section:
            return
        for cat in section.get("categories", []):
            if cat.get("type") == "sub":
                sub_key = cat.get("subKey") or cat.get("key")
                if sub_key:
                    new_path = f"{path_name} > {cat.get('name', sub_key)}"
                    recurse_collect(sub_key, new_path, depth + 1)
                continue

            channels_file_rel = cat.get("channelsFile")
            if not channels_file_rel:
                continue
            channels_file_path = BASE_DIR / channels_file_rel
            if not channels_file_path.exists() or channels_file_path.is_dir():
                continue

            channel_key = channels_file_path.stem
            channels = load_json(channels_file_path)
            if not channels:
                continue

            category_name = cat.get("name", cat.get("key", "غير معروف"))
            for channel in channels:
                ch_id = channel.get("id")
                ch_name = channel.get("name", "بدون اسم")
                if not ch_id:
                    continue

                stream_file = STREAMS_DIR / channel_key / f"{ch_id}.json"
                if not stream_file.exists():
                    tasks.append({
                        "section": path_name,
                        "category": category_name,
                        "channel": ch_name,
                        "channel_file": channel_key,
                        "channel_id": ch_id,
                        "url": None,
                        "headers": None
                    })
                    continue

                servers = load_json(stream_file)
                if not servers:
                    continue

                for server in servers:
                    url = server.get("url", "")
                    headers = build_headers(server)
                    tasks.append({
                        "section": path_name,
                        "category": category_name,
                        "channel": ch_name,
                        "channel_file": channel_key,
                        "channel_id": ch_id,
                        "url": url,
                        "headers": headers
                    })

    sub_keys: Set[str] = set()
    for sec in sections_dict.values():
        for cat in sec.get("categories", []):
            if cat.get("type") == "sub":
                sk = cat.get("subKey") or cat.get("key")
                if sk:
                    sub_keys.add(sk)

    for sec_key, sec in sections_dict.items():
        if sec_key in sub_keys:
            continue
        sec_name = sec.get("name", sec_key)
        recurse_collect(sec_key, sec_name)

    return tasks

async def process_async(sections_dict: Dict[str, Dict], report_rows: List[Dict]):
    tasks = collect_all_tasks(sections_dict)
    total = len(tasks)
    print(f"📊 إجمالي الروابط: {total}")

    connector = aiohttp.TCPConnector(limit=CONCURRENT_LIMIT)
    async with aiohttp.ClientSession(connector=connector) as session:
        semaphore = asyncio.Semaphore(CONCURRENT_LIMIT)

        async def sem_task(task):
            async with semaphore:
                if task["url"] is None:
                    return task, {
                        "url": "",
                        "status": "file_missing",
                        "working": False,
                        "error": "Stream file not found"
                    }
                result = await check_stream_async(session, task["url"], task["headers"])
                return task, result

        # إنشاء جميع المهام مع شريط تقدم
        coros = [sem_task(task) for task in tasks]
        for coro in async_tqdm.as_completed(coros, desc="فحص الروابط", total=total, unit="رابط"):
            task, result = await coro
            report_rows.append({
                "section": task["section"],
                "category": task["category"],
                "channel": task["channel"],
                "channel_file": task["channel_file"],
                "channel_id": task["channel_id"],
                "url": result["url"],
                "status": result["status"],
                "working": result["working"],
                "error": result["error"]
            })

async def main_async():
    sections_list = load_json(BASE_DIR / "section_categories.json")
    if not sections_list:
        print("❌ لم يتم العثور على section_categories.json")
        return

    sections_dict: Dict[str, Dict] = {}
    for sec in sections_list:
        key = sec.get("key")
        if key:
            sections_dict[key] = sec

    report_rows: List[Dict] = []
    start_time = time.time()
    await process_async(sections_dict, report_rows)
    elapsed = time.time() - start_time

    update_channels_working_status(report_rows)

    csv_file = "streams_report.csv"
    fields = ["section", "category", "channel", "channel_file", "channel_id", "url", "status", "working", "error"]
    with open(csv_file, "w", encoding="utf-8-sig", newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(report_rows)

    print(f"\n✅ تم إنشاء التقرير: {csv_file}")
    print(f"   إجمالي الروابط: {len(report_rows)}")
    print(f"   الوقت المستغرق: {elapsed:.2f} ثانية")

if __name__ == "__main__":
    asyncio.run(main_async())