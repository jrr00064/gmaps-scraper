#!/usr/bin/env python3
"""
Download free proxies for testing
Warning: Free proxies are unreliable. Use paid residential for production.
"""

import requests
import re

print("🔍 Downloading free proxies...")
print("="*70)

proxies = set()

# Source 1: Proxy-list.download
print("\n[1/3] Getting from proxy-list.download...")
try:
    r = requests.get("https://www.proxy-list.download/api/v1/get?type=http&anon=elite", timeout=10)
    if r.status_code == 200:
        for line in r.text.strip().split("\r\n"):
            if re.match(r"\d+\.\d+\.\d+\.\d+:\d+", line):
                proxies.add(f"http://{line}")
        print(f"  ✓ Found {len(proxies)} proxies")
except Exception as e:
    print(f"  ✗ Failed: {e}")

# Source 2: Free-proxy-list.net (requires parsing)
print("\n[2/3] Getting from free-proxy-list.net...")
try:
    r = requests.get("https://www.free-proxy-list.net/", timeout=10)
    if r.status_code == 200:
        matches = re.findall(r'(\d+\.\d+\.\d+\.\d+):(\d+)', r.text)
        for ip, port in matches[:50]:  # Limit to 50
            proxies.add(f"http://{ip}:{port}")
        print(f"  ✓ Total: {len(proxies)} proxies")
except Exception as e:
    print(f"  ✗ Failed: {e}")

# Save
print("\n[3/3] Saving to proxies.txt...")
if proxies:
    with open("proxies.txt", "w") as f:
        for proxy in sorted(proxies):
            f.write(f"{proxy}\n")
    print(f"  ✓ Saved {len(proxies)} proxies to /tmp/gmaps-scraper/proxies.txt")
    print("\n⚠️ WARNING: Free proxies are slow and unreliable!")
    print("   For production, use paid residential proxies.")
else:
    print("  ✗ No proxies found. Add manually to proxies.txt:")
    print("      echo 'ip:port' > /tmp/gmaps-scraper/proxies.txt")
