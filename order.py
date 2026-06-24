#!/usr/bin/env python3
"""
نقل ملفات البث من streams/ إلى مجلدات فرعية حسب القنوات:
- من streams/<channel_id>.json
- إلى streams/<channel_filename>/<channel_id>.json
"""
import json
import shutil
from pathlib import Path
from tqdm import tqdm

BASE_DIR = Path(".")
CHANNELS_DIR = BASE_DIR / "channels"
STREAMS_DIR = BASE_DIR / "streams"

def move_streams():
    channel_files = sorted(CHANNELS_DIR.glob("*.json"))
    if not channel_files:
        print("❌ لم يتم العثور على ملفات في channels/")
        return

    total_moved = 0

    for channel_file in tqdm(channel_files, desc="📁 معالجة ملفات القنوات", unit="file"):
        stem = channel_file.stem

        try:
            with open(channel_file, "r", encoding="utf-8") as f:
                channels = json.load(f)
        except Exception as e:
            tqdm.write(f"   ❌ فشل قراءة {channel_file.name}: {e}")
            continue

        if not isinstance(channels, list):
            tqdm.write(f"   ⚠️ المحتوى ليس قائمة، تخطي")
            continue

        out_dir = STREAMS_DIR / stem
        out_dir.mkdir(parents=True, exist_ok=True)

        moved_here = 0
        for ch in tqdm(channels, desc=f"   📂 {stem}", unit="ch", leave=False):
            ch_id = ch.get("id")
            streams_file = ch.get("streamsFile")

            if not ch_id or not streams_file:
                continue

            src_path = Path(streams_file)
            if not src_path.is_absolute():
                src_path = BASE_DIR / src_path

            if not src_path.exists():
                continue

            dest_path = out_dir / f"{ch_id}.json"

            try:
                shutil.move(str(src_path), str(dest_path))
                moved_here += 1
            except Exception as e:
                tqdm.write(f"   ❌ فشل نقل {src_path} → {dest_path}: {e}")

        total_moved += moved_here
        tqdm.write(f"   ✅ تم نقل {moved_here} قناة إلى {out_dir.relative_to(BASE_DIR)}")

    print(f"\n🎉 تمت العملية بنجاح. إجمالي الملفات المنقولة: {total_moved}")

if __name__ == "__main__":
    move_streams()