#!/usr/bin/env python3
"""
تحديث مسارات streamsFile في جميع ملفات القنوات إلى الهيكل الجديد:
- من streams/<id>.json
- إلى streams/<channel_file_stem>/<id>.json
"""
import json
from pathlib import Path
from tqdm import tqdm

BASE_DIR = Path(".")
CHANNELS_DIR = BASE_DIR / "channels"

def update_streams_files():
    channel_files = sorted(CHANNELS_DIR.glob("*.json"))
    if not channel_files:
        print("❌ لم يتم العثور على ملفات في channels/")
        return

    updated_count = 0
    total_channels = 0

    for channel_file in tqdm(channel_files, desc="📁 تحديث ملفات القنوات", unit="file"):
        stem = channel_file.stem  # اسم الملف بدون .json = مفتاح القناة

        try:
            with open(channel_file, "r", encoding="utf-8") as f:
                channels = json.load(f)
        except Exception as e:
            tqdm.write(f"   ❌ فشل قراءة {channel_file.name}: {e}")
            continue

        if not isinstance(channels, list):
            tqdm.write(f"   ⚠️ {channel_file.name} ليس قائمة، تخطي")
            continue

        changed = False
        for ch in channels:
            ch_id = ch.get("id")
            if not ch_id:
                continue

            # المسار الجديد
            new_path = f"streams/{stem}/{ch_id}.json"

            # تحديث إذا كان مختلفاً
            if ch.get("streamsFile") != new_path:
                ch["streamsFile"] = new_path
                changed = True
                total_channels += 1

        if changed:
            with open(channel_file, "w", encoding="utf-8") as f:
                json.dump(channels, f, indent=2, ensure_ascii=False)
            updated_count += 1
            tqdm.write(f"   ✅ تم تحديث {channel_file.name} ({len(channels)} قناة)")

    print(f"\n🎉 تم التحديث: {updated_count} ملف قنوات، {total_channels} قناة.")

if __name__ == "__main__":
    update_streams_files()