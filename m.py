#!/usr/bin/env python3
"""
تعديل مسارات streamsFile في ملفات matchingchannels/ إلى الهيكل الجديد:
streams/<source_file_stem>/<id>.json
"""
import json
from pathlib import Path
from tqdm import tqdm

BASE_DIR = Path(".")
MATCHING_DIR = BASE_DIR / "matchingchannels"

def update_matching_files():
    json_files = sorted(MATCHING_DIR.glob("*.json"))
    if not json_files:
        print("❌ لا توجد ملفات في matchingchannels/")
        return

    total_updated = 0
    for file_path in tqdm(json_files, desc="📁 معالجة ملفات matchingchannels", unit="file"):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            tqdm.write(f"   ❌ فشل في قراءة {file_path.name}: {e}")
            continue

        if not isinstance(data, list):
            tqdm.write(f"   ⚠️ {file_path.name} ليس قائمة، تخطي")
            continue

        changed = False
        for item in data:
            if not isinstance(item, dict):
                continue
            src_file = item.get("source_file")
            item_id = item.get("id")
            if src_file and item_id is not None:
                # إزالة اللاحقة .json من اسم الملف
                stem = Path(src_file).stem   # مثلاً "90" بدلاً من "90.json"
                new_path = f"streams/{stem}/{item_id}.json"
                if item.get("streamsFile") != new_path:
                    item["streamsFile"] = new_path
                    changed = True

        if changed:
            try:
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                total_updated += 1
                tqdm.write(f"   ✅ تم تحديث {file_path.name}")
            except Exception as e:
                tqdm.write(f"   ❌ فشل في حفظ {file_path.name}: {e}")

    print(f"\n🎉 تم تحديث {total_updated} ملف في matchingchannels/")

if __name__ == "__main__":
    update_matching_files()