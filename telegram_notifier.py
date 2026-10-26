#!/usr/bin/env python3
"""
telegram_notifier.py: Sends formatted status/goal updates to the user's Telegram.
Uses Hermes environment configuration and verified user chat ID.
"""

import os
import sys
import json
import urllib.request
import urllib.parse
from pathlib import Path

def get_chat_id():
    env_file = Path.home() / ".hermes/.env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if line.startswith("TELEGRAM_CHAT_ID="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    return os.environ.get("TELEGRAM_CHAT_ID", "")

def get_bot_token():
    env_file = Path.home() / ".hermes/.env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if line.startswith("TELEGRAM_BOT_TOKEN="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    return os.environ.get("TELEGRAM_BOT_TOKEN", "")

def send_message(text: str):
    token = get_bot_token()
    chat_id = get_chat_id()
    if not token or not chat_id:
        # Silent return if Telegram credentials are not configured
        return False
    
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text
    }
    
    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            res = json.loads(resp.read().decode())
            if res.get("ok"):
                print("[+] Telegram message sent successfully.")
                return True
            else:
                print(f"[-] Telegram API returned error: {res}")
                return False
    except Exception as e:
        print(f"[-] Failed to send Telegram message: {e}")
        return False

if __name__ == "__main__":
    msg = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "Hermes Research Automation: Test Notification."
    send_message(msg)
