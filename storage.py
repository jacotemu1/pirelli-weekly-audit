from __future__ import annotations

import hashlib


def make_fingerprint(site_code: str, url: str, title: str, description: str) -> str:
    payload = f"{site_code}|{url}|{title}|{description}".encode('utf-8', errors='ignore')
    return hashlib.sha1(payload).hexdigest()[:16]
