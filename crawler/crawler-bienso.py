"""
Crawler danh sách biển số đấu giá - VPA  từ (vpa.com.vn)
trang này có api luôn rồi lên không cần cai đặt selenium hay fake browser nữa, chỉ cần requests + pandas là đủ.
API: /v2/public/plates/search 

Cài đặt:  pip install requests pandas tqdm
Chạy:     python vpa_crawler.py --vehicle motor_bike --status published
          python vpa_crawler.py --vehicle car --status published
          python vpa_crawler.py --max-pages 5        # chạy thử 5 trang trước
"""
import argparse
import json
import random
import time
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

BASE_URL = "https://vpa.com.vn/v2/public/plates/search"
TAKE = 50                 # vi trang đang  có 50 nên ở đay vụt 50 thôi không sợ chết api
DELAY = (1.0, 2.0)        # nghỉ ngẫu nhiên trống fw chặn ip bot
OUT = Path("/model/data/raw")

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (student research project IT3190; contact: <email cua ban>)",
    "Accept": "application/json",
    "Referer": "https://vpa.com.vn/",
})


def get_page(page, vehicle, status, retries=4):
    params = {"page": page, "take": TAKE, "vehicle": vehicle, "status": status}
    for attempt in range(retries):
        try:
            r = session.get(BASE_URL, params=params, timeout=30)
            if r.status_code == 429:  # bị giới hạn tốc độ -> nghỉ lâu hơn
                raise requests.HTTPError("429 Too Many Requests")
            r.raise_for_status()
            return r.json()
        except Exception as e:
            wait = 10 * (2 ** attempt)
            print(f"\n[trang {page}] lỗi: {e} -> nghỉ {wait}s rồi thử lại")
            time.sleep(wait)
    raise RuntimeError(f"Bỏ cuộc ở trang {page}")


def slim(item):
    """Giữ trường cần thiết, rút gọn 'kinds' (bỏ metadata lặp lại) để file nhẹ hơn nhiều."""
    kinds = item.get("kinds") or []
    return {
        "id": item.get("id"),
        "full_number": item.get("fullNumber"),
        "local_symbol": item.get("localSymbol"),
        "serial_letter": item.get("serialLetter"),
        "serial_number": item.get("serialNumber"),
        "vehicle": item.get("vehicle"),
        "status": item.get("status"),
        "color": item.get("color"),
        "starting_price": item.get("startingPrice"),
        "province_code": item.get("provinceCode"),
        "province_name": (item.get("province") or {}).get("name"),
        # nhãn phong thủy do VPA gắn (bỏ nhãn chung chung "Phong thuỷ" khớp mọi số)
        "vpa_kinds": "|".join(k["name"] for k in kinds if k.get("group") == "phong_thuy"),
        "vpa_avoid": "|".join(k["name"] for k in kinds if k.get("group") == "bien_tranh"),
        "register_start": item.get("registerStartTime"),
        "register_end": item.get("registerEndTime"),
        "auction_start": item.get("auctionStartTime"),
        "auction_end": item.get("auctionEndTime"),
        "updated_at": item.get("updatedAt"),
        # giữ nguyên các trường lạ (vd giá trúng ở API kết quả) để không mất dữ liệu
        **{f"extra_{k}": v for k, v in item.items()
           if k not in {"key", "id", "fullNumber", "localSymbol", "serialLetter", "serialNumber",
                        "vehicle", "status", "color", "startingPrice", "provinceCode", "province",
                        "kinds", "registerStartTime", "registerEndTime", "auctionStartTime",
                        "auctionEndTime", "updatedAt"}},
    }


def crawl(vehicle, status, max_pages=None):
    OUT.mkdir(parents=True, exist_ok=True)
    name = f"plates_{vehicle}_{status}"
    data_file = OUT / f"{name}.jsonl"
    progress_file = OUT / f"{name}.progress"
    done = set(map(int, progress_file.read_text().split())) if progress_file.exists() else set()

    first = get_page(1, vehicle, status)
    meta = first.get("meta", {})
    page_count = meta.get("pageCount", 1)
    print(f"{vehicle}/{status}: {meta.get('itemCount'):,} biển, {page_count:,} trang")
    if max_pages:
        page_count = min(page_count, max_pages)

    with data_file.open("a", encoding="utf-8") as f, progress_file.open("a") as pf:
        for page in tqdm(range(1, page_count + 1), desc=name):
            if page in done:
                continue
            payload = first if page == 1 else get_page(page, vehicle, status)
            items = payload.get("data", [])
            if not items:
                print(f"\nTrang {page} rỗng, dừng.")
                break
            for it in items:
                f.write(json.dumps(slim(it), ensure_ascii=False) + "\n")
            pf.write(f"{page}\n")
            pf.flush()
            time.sleep(random.uniform(*DELAY))
    return data_file


import threading
from concurrent.futures import ThreadPoolExecutor, as_completed


class RateLimiter:
    """Giới hạn TỔNG số request/giây cho mọi luồng; tự giảm tốc khi server báo quá tải."""
    def __init__(self, rate):
        self.interval = 1.0 / rate
        self.lock = threading.Lock()
        self.next_time = time.monotonic()

    def wait(self):
        with self.lock:
            now = time.monotonic()
            sleep_for = self.next_time - now
            self.next_time = max(now, self.next_time) + self.interval
        if sleep_for > 0:
            time.sleep(sleep_for)

    def slow_down(self):
        with self.lock:
            self.interval = min(self.interval * 2, 5.0)
            print(f"\n[giảm tốc] còn {1 / self.interval:.2f} request/giây")


_local = threading.local()


def _thread_session():
    if not hasattr(_local, "s"):
        _local.s = requests.Session()
        _local.s.headers.update(session.headers)
    return _local.s


def _get_page_limited(page, vehicle, status, limiter, retries=5):
    params = {"page": page, "take": TAKE, "vehicle": vehicle, "status": status}
    for attempt in range(retries):
        limiter.wait()
        try:
            r = _thread_session().get(BASE_URL, params=params, timeout=30)
            if r.status_code in (429, 502, 503, 504):
                limiter.slow_down()
                raise requests.HTTPError(f"{r.status_code}")
            r.raise_for_status()
            return page, r.json().get("data", [])
        except Exception as e:
            time.sleep(10 * (2 ** attempt))
    raise RuntimeError(f"Bỏ cuộc ở trang {page}")


def crawl_parallel(vehicle, status, workers=10, rate=2.5, max_pages=None):
    """Giống crawl() nhưng chạy nhiều luồng. Dùng chung file .progress nên có thể đổi qua lại."""
    OUT.mkdir(parents=True, exist_ok=True)
    name = f"plates_{vehicle}_{status}"
    data_file = OUT / f"{name}.jsonl"
    progress_file = OUT / f"{name}.progress"
    done = set(map(int, progress_file.read_text().split())) if progress_file.exists() else set()

    meta = get_page(1, vehicle, status).get("meta", {})
    page_count = min(meta.get("pageCount", 1), max_pages or 10**9)
    todo = [p for p in range(1, page_count + 1) if p not in done]
    print(f"{vehicle}/{status}: {meta.get('itemCount'):,} biển | còn {len(todo):,}/{page_count:,} trang | "
          f"{workers} luồng, tối đa {rate} request/giây")

    limiter = RateLimiter(rate)
    write_lock = threading.Lock()
    with data_file.open("a", encoding="utf-8") as f, progress_file.open("a") as pf, \
            ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(_get_page_limited, p, vehicle, status, limiter) for p in todo]
        for fut in tqdm(as_completed(futures), total=len(futures), desc=name):
            try:
                page, items = fut.result()
            except Exception as e:
                print(f"\n{e} (chạy lại sau để lấy bù)")
                continue
            with write_lock:
                for it in items:
                    f.write(json.dumps(slim(it), ensure_ascii=False) + "\n")
                pf.write(f"{page}\n")
                pf.flush()
    return data_file


def to_csv(jsonl_path):
    df = pd.read_json(jsonl_path, lines=True, dtype={"province_code": str, "serial_number": str,
                                                     "local_symbol": str, "id": str})
    before = len(df)
    df = df.drop_duplicates(subset="id", keep="last")
    out = jsonl_path.with_suffix(".csv")
    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"{out}: {len(df):,} dòng (bỏ {before - len(df):,} bản trùng)")
    return df


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--vehicle", default="motor_bike", help="motor_bike hoặc car (kiểm tra lại trên web)")
    ap.add_argument("--status", default="published", help="published = đang niêm yết")
    ap.add_argument("--max-pages", type=int, default=None, help="giới hạn số trang để chạy thử")
    ap.add_argument("--workers", type=int, default=4, help="số luồng (1 = tuần tự)")
    ap.add_argument("--rate", type=float, default=2.5, help="tổng số request/giây tối đa")
    args, _ = ap.parse_known_args()  # bỏ qua tham số -f do
    # Lưu ý Colab: nếu dán code vào ô, XÓA khối này đi và gọi crawl_parallel(...) ở ô riêng.
    to_csv(crawl_parallel(args.vehicle, args.status, args.workers, args.rate, args.max_pages))