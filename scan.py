import json
import requests
import csv
from pathlib import Path
from typing import Dict, List, Any, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import time

# -------------------- الإعدادات --------------------
BASE_DIR = Path(".")
STREAMS_DIR = BASE_DIR / "streams"
CHANNELS_DIR = BASE_DIR / "channels"

# عدد الخيوط المتزامنة (يمكنك تعديله حسب قوة جهازك وسرعة الإنترنت)
MAX_WORKERS = 20
TIMEOUT = 5  # ثوانٍ

def load_json(path: Path) -> Any:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"❌ خطأ في قراءة {path}: {e}")
        return None

def check_stream(session: requests.Session, url: str, headers: Dict[str, str] = None) -> Dict[str, Any]:
    """فحص رابط البث (باستخدام جلسة لتقليل overhead)."""
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
        # نجلب فقط رأس الاستجابة أو نتحقق من الاتصال بدون تحميل المحتوى
        resp = session.get(url, headers=headers or {}, stream=True, timeout=TIMEOUT, allow_redirects=True)
        # نقرأ أول 1 بايت فقط للتأكد من وجود بيانات (أو نكتفي بـ status_code)
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
    بعد الفحص، نقوم بتحديث جميع ملفات channels/*.json:
    نضيف حقل "isworking": 1 إذا كانت أي من روابط القناة تعمل، 0 إذا لا.
    """
    # 1. بناء قاموس: streams_file -> is_working
    stream_status = {}
    for row in report_rows:
        sf = row["streams_file"]
        if sf not in stream_status:
            stream_status[sf] = False
        if row["working"]:
            stream_status[sf] = True

    # 2. تحميل كل ملف قنوات، تحديث كل كائن قناة، حفظ
    channels_dir = Path("channels")
    for channels_file in channels_dir.glob("*.json"):
        channels = load_json(channels_file)
        if not channels:
            continue
        changed = False
        for ch in channels:
            streams_file = ch.get("streamsFile")
            if streams_file and streams_file in stream_status:
                isw = 1 if stream_status[streams_file] else 0
                # لا نعدل إذا كان موجوداً مسبقاً بنفس القيمة
                if ch.get("isworking") != isw:
                    ch["isworking"] = isw
                    changed = True
        if changed:
            with open(channels_file, 'w', encoding='utf-8') as f:
                json.dump(channels, f, indent=2, ensure_ascii=False)
            print(f"✅ تم تحديث {channels_file.name}")
            
def collect_all_tasks(sections_dict: Dict[str, Dict]) -> List[Dict]:
    """تجميع جميع مهام الفحص (قائمة بكل رابط مع بيانات القناة)."""
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

            channels = load_json(channels_file_path)
            if not channels:
                continue

            category_name = cat.get("name", cat.get("key", "غير معروف"))
            for channel in channels:
                channel_name = channel.get("name", "بدون اسم")
                streams_file = channel.get("streamsFile")
                if not streams_file:
                    continue
                streams_path = BASE_DIR / streams_file
                if not streams_path.exists():
                    # مهمة وهمية للإبلاغ عن ملف مفقود
                    tasks.append({
                        "section": path_name,
                        "category": category_name,
                        "channel": channel_name,
                        "streams_file": streams_file,
                        "url": None,
                        "headers": None
                    })
                    continue

                servers = load_json(streams_path)
                if not servers:
                    continue

                for server in servers:
                    url = server.get("url", "")
                    headers = build_headers(server)
                    tasks.append({
                        "section": path_name,
                        "category": category_name,
                        "channel": channel_name,
                        "streams_file": streams_file,
                        "url": url,
                        "headers": headers
                    })

    # الأقسام الرئيسية
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

def process_with_threads(sections_dict: Dict[str, Dict], report_rows: List[Dict]):
    tasks = collect_all_tasks(sections_dict)
    total = len(tasks)
    print(f"📊 إجمالي الروابط المطلوب فحصها: {total}")

    # إنشاء جلسة واحدة يعاد استخدامها
    session = requests.Session()

    # استخدام ThreadPoolExecutor مع tqdm
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # نرسل جميع المهام
        future_to_task = {}
        for task in tasks:
            if task["url"] is None:
                # مهمة وهمية (ملف مفقود) نتعامل معها مباشرة
                future = executor.submit(lambda: None)  # دالة فارغة
            else:
                future = executor.submit(check_stream, session, task["url"], task["headers"])
            future_to_task[future] = task

        # شريط التقدم
        with tqdm(total=total, desc="فحص الروابط", unit="رابط") as pbar:
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                if task["url"] is None:
                    # رابط مفقود
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
                    "streams_file": task["streams_file"],
                    "url": result["url"],
                    "status": result["status"],
                    "working": result["working"],
                    "error": result["error"]
                })
                pbar.update(1)

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

    # كتابة التقرير
    csv_file = "streams_report.csv"
    fields = ["section", "category", "channel", "streams_file", "url", "status", "working", "error"]
    with open(csv_file, "w", encoding="utf-8-sig", newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(report_rows)

    print(f"\n✅ تم إنشاء التقرير: {csv_file}")
    print(f"   إجمالي الروابط: {len(report_rows)}")
    print(f"   الوقت المستغرق: {elapsed:.2f} ثانية")

if __name__ == "__main__":
    main()