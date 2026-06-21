#!/usr/bin/env python3
import subprocess
import os
import sys

# ======= الإعدادات (بدون أسرار) =======
REPO_NAME = "Yeman-Data"        # اسم المستودع
BRANCH = "main"
COMMIT_MSG = "إضافة ملفات subcategories"

# جلب التوكن من متغير البيئة
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
if not GITHUB_TOKEN:
    print("❌ يجب تعيين GITHUB_TOKEN في متغيرات البيئة.")
    sys.exit(1)

# يمكنك أيضاً جلب اسم المستخدم من البيئة أو تثبيته
GITHUB_USER = "L-absi"

# =======================================

def configure_git_credentials():
    """تعديل الرابط البعيد ليشمل التوكن (لا يُخزّن في الملفات)"""
    remote_url = f"https://{GITHUB_USER}:{GITHUB_TOKEN}@github.com/{GITHUB_USER}/{REPO_NAME}.git"
    subprocess.run(["git", "remote", "set-url", "origin", remote_url], check=True)

def reset_remote_url():
    """إعادة الرابط البعيد إلى الصيغة العامة بعد الدفع"""
    clean_url = f"https://github.com/{GITHUB_USER}/{REPO_NAME}.git"
    subprocess.run(["git", "remote", "set-url", "origin", clean_url], check=False)

def run_git_commands():
    try:
        print("▶️ git add .")
        subprocess.run(["git", "add", "."], check=True)

        print("▶️ git status")
        subprocess.run(["git", "status"], check=True)

        print(f"▶️ git commit -m \"{COMMIT_MSG}\"")
        subprocess.run(["git", "commit", "-m", COMMIT_MSG], check=True)

        print(f"▶️ git push origin {BRANCH}")
        subprocess.run(["git", "push", "origin", BRANCH], check=True)

        print("✅ تم الدفع بنجاح.")
    except subprocess.CalledProcessError as e:
        print(f"❌ فشل: {e}")
    finally:
        reset_remote_url()

if __name__ == "__main__":
    configure_git_credentials()
    run_git_commands()