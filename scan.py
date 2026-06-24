import json
import requests
import csv
from pathlib import Path
from typing import Dict, List, Any, Set, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import time

# -------------------- الإعدادات --------------------
BASE_DIR = Path(".")
STREAMS_DIR = BASE_DIR / "streams"
CHANNELS_DIR = BASE_DIR / "channels"

MAX_WORKERS = 20
TIMEOUT = 5  # ثوانٍ

# -------------------- دوال مساعدة --------------------
def load_json(path: Path) -> Any:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"❌ خطأ في قراءة {path}: {e}")
        return None

def check_stream(session: requests.Session, url: str, headers: Dict[str, str] = None) -> Dict[str, Any]:
    """فحص رابط البث مع إعادة استخدام الجلسة."""
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
        resp = session.get(url, headers=headers or {}, stream=True, timeout=TIMEOUT, allow_redirects=True)
        # قراءة أول بايت فقط للتأكد من وجود محتوى فعلي
        chunk = resp.raw.read(1, decode_content=False)
        if resp.status_code == 200 and chunk:
            result["working"] = True
            result["status"] = resp.status_code
        else:
            result["status"] = resp.status_code
            result["error"] = f"Status {resp.status_code} or empty response"
    except requests.exceptions.Timeout:
        result["status"] = "timeout"
        result["error"] = "Connection timed out"
    except requests.exceptions.ConnectionError:
        result["status"] = "connection_error"
        result["error"] = "Failed to connect"
    except Exception as e:
        result["status"] = "error"
        result["error"] = str(e)

    return result

def build_headers(server: Dict) -> Dict[str, str]:
    headers = {}
    if server.get("Referer"):
        headers["Referer"] = server["Referer"]
    if server.get("userAgent"):
        headers["User-Agent"] = server["userAgent"]
    return headers

def update_channels_working_status(report_rows: List[Dict]):
    """
    تحديث حقل 'isworking' في جميع ملفات القنوات.
    الهيكل الجديد: ملفات البث أصبحت streams/<channel_key>/<id>.json
    """
    # تجميع حالة العمل لكل قناة (مفتاح القناة + معرف القناة)
    # سنستخدم زوج (channel_file_stem, channel_id) لتحديد ما إذا كانت القناة تعمل.
    status_map = {}  # key: (channel_file_stem, channel_id) -> bool
    for row in report_rows:
        # row تحتوي الآن على 'channel_file' و 'channel_id' (سنضيفها لاحقًا)
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

# -------------------- تجميع المهام --------------------
def collect_all_tasks(sections_dict: Dict[str, Dict]) -> List[Dict]:
    """
    تجميع جميع مهام الفحص مع دعم الهيكل الجديد:
    - ملفات القنوات: channels/<key>.json
    - ملفات البث: streams/<key>/<id>.json
    """
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

            channels_file_rel = cat.get("channelsFile")  # مثال: channels/max_tv.json
            if not channels_file_rel:
                continue
            channels_file_path = BASE_DIR / channels_file_rel
            if not channels_file_path.exists() or channels_file_path.is_dir():
                continue

            # اسم الملف بدون لاحقة = مفتاح القناة (مجلد البث)
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

                # المسار الجديد لملف البث
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

    # تجنب تكرار معالجة الأقسام الفرعية كأقسام رئيسية
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

# -------------------- الفحص متعدد الخيوط --------------------
def process_with_threads(sections_dict: Dict[str, Dict], report_rows: List[Dict]):
    tasks = collect_all_tasks(sections_dict)
    total = len(tasks)
    print(f"📊 إجمالي الروابط المطلوب فحصها: {total}")

    session = requests.Session()
    # إعداد محول لزيادة عدد الاتصالات المتزامنة
    adapter = requests.adapters.HTTPAdapter(pool_maxsize=MAX_WORKERS, pool_block=True)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_task = {}
        for task in tasks:
            if task["url"] is None:
                future = executor.submit(lambda: None)
            else:
                future = executor.submit(check_stream, session, task["url"], task["headers"])
            future_to_task[future] = task

        with tqdm(total=total, desc="فحص الروابط", unit="رابط") as pbar:
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                if task["url"] is None:
                    result = {
                        "url": "",
                        "status": "file_missing",
                        "working": False,
                        "error": "Stream file not found"
                    }
                else:
                    result = future.result()

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
                pbar.update(1)

# -------------------- الدالة الرئيسية --------------------
def main():
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
    process_with_threads(sections_dict, report_rows)
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
    main()