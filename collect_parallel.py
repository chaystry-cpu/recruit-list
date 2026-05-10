"""
採用HP自動検出スクリプト【並列処理版】
=======================================
通常版: 1日300社
この版: 1日3,000〜5,000社（10並列）
全国90,450社 → 約20〜30日で完了

使い方:
  python collect_parallel.py            # 通常実行（10並列・1回2,000社）
  python collect_parallel.py --workers 20  # 並列数を増やす
  python collect_parallel.py --batch 5000  # 1回の処理件数を増やす
"""

import os, re, csv, time, json, glob, sys, argparse
import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urljoin, quote
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# ===================== 設定 =====================
TARGET_KEYWORDS   = ["情報通信","ソフトウェア","システム","テクノロジー",
                     "デジタル","ネット","クラウド","データ","AI","アイティー"]
TARGET_PREFECTURE = None      # 例: "東京都" / None=全国
WORKERS           = 10        # 並列数（増やすほど速いがブロックリスクも上がる）
BATCH_SIZE        = 2000      # 1回の実行で処理する件数
DELAY_MIN         = 0.5       # リクエスト間隔 最小（秒）
DELAY_MAX         = 1.5       # リクエスト間隔 最大（秒）
PROCESSED_DB      = "processed.json"
OUTPUT_CSV        = "companies.csv"
# ================================================

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
TIMEOUT = 8  # タイムアウト（秒）

RECRUIT_URL_KW  = ["recruit","career","careers","採用","join","jobs","job",
                    "employ","hiring","saiyo","saiyou","entry","work-with-us"]
RECRUIT_TEXT_KW = ["採用情報","採用サイト","キャリア採用","募集要項","新卒採用",
                    "中途採用","求人情報","採用ページ","採用・求人","仲間募集","一緒に働く"]

FIELDNAMES = ["社名","社名カナ","法人番号","都道府県","市区町村","郵便番号",
              "企業HP","採用HP有無","採用ページURL","代表電話番号","確認日"]

# スレッドセーフなロック
csv_lock = Lock()
db_lock  = Lock()
counter  = {"done": 0, "recruit": 0, "error": 0}
cnt_lock = Lock()


# ========== CSVロード ==========
def load_companies():
    # ZIPから直接読み込み対応
    import zipfile
    zip_files = glob.glob("*.zip") + glob.glob("/mnt/user-data/uploads/*.zip")
    csv_files = glob.glob("*.csv") + glob.glob("houjin*.csv") + glob.glob("00_*.csv")

    dfs = []

    # ZIPがあればそこから
    for zf in zip_files:
        try:
            with zipfile.ZipFile(zf) as z:
                for name in z.namelist():
                    if name.endswith(".csv") and "asc" not in name:
                        print(f"[INFO] ZIPから読み込み: {zf}/{name}")
                        with z.open(name) as f:
                            for enc in ["cp932","utf-8","shift-jis"]:
                                try:
                                    df = pd.read_csv(f, encoding=enc, header=None,
                                                     dtype=str, low_memory=False)
                                    print(f"  → {len(df):,}行 ({enc})")
                                    dfs.append(df); break
                                except: pass
        except Exception as e:
            print(f"[WARN] ZIP読み込み失敗: {e}")

    # 通常CSVも確認
    if not dfs:
        for cf in csv_files:
            for enc in ["cp932","utf-8","shift-jis"]:
                try:
                    df = pd.read_csv(cf, encoding=enc, header=None,
                                     dtype=str, low_memory=False)
                    print(f"[INFO] {cf}: {len(df):,}行")
                    dfs.append(df); break
                except: continue

    if not dfs:
        print("[ERROR] 国税庁CSVが見つかりません")
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True).rename(columns={
        1:"法人番号", 5:"社名", 6:"社名_旧", 7:"社名カナ_旧",
        # 実際の列マッピング（国税庁フォーマット）
    })

    # 正しい列マッピング（インデックスベース）
    col_map = {1:"法人番号", 6:"社名", 28:"社名カナ", 9:"都道府県",
               10:"市区町村", 11:"所在地詳細", 15:"郵便番号", 2:"処理区分"}
    df2 = pd.DataFrame()
    for idx, name in col_map.items():
        if idx in df.columns:
            df2[name] = df[idx]

    if df2.empty:
        print("[ERROR] 列マッピング失敗")
        return pd.DataFrame()

    # 廃業除外
    if "処理区分" in df2.columns:
        df2 = df2[df2["処理区分"] != "4"]

    # 都道府県フィルタ
    if TARGET_PREFECTURE and "都道府県" in df2.columns:
        df2 = df2[df2["都道府県"].str.contains(TARGET_PREFECTURE, na=False)]
        print(f"[INFO] {TARGET_PREFECTURE}絞り込み: {len(df2):,}件")

    # 業種キーワードフィルタ
    if TARGET_KEYWORDS and "社名" in df2.columns:
        pat = "|".join(TARGET_KEYWORDS)
        mask = df2["社名"].str.contains(pat, na=False, case=False)
        if "社名カナ" in df2.columns:
            mask |= df2["社名カナ"].str.contains(pat, na=False, case=False)
        df2 = df2[mask]
        print(f"[INFO] キーワード絞り込み: {len(df2):,}件")

    return df2.reset_index(drop=True)


# ========== 処理済みDB ==========
def load_db():
    if os.path.exists(PROCESSED_DB):
        with open(PROCESSED_DB, encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_db(db):
    with db_lock:
        with open(PROCESSED_DB, "w", encoding="utf-8") as f:
            json.dump(db, f, ensure_ascii=False)


# ========== 企業HP検索 ==========
def find_website(name: str) -> str:
    import random
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    try:
        url = f"https://html.duckduckgo.com/html/?q={quote(name + ' 公式サイト')}"
        res = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        soup = BeautifulSoup(res.text, "html.parser")
        skip = ["duckduckgo","wikipedia","facebook","linkedin","twitter",
                "instagram","youtube","amazon","rakuten","indeed","doda",
                "mynavi","hataraku","job","hotpepper","tabelog","recruit"]
        for r in soup.select(".result__url")[:5]:
            href = r.get_text(strip=True)
            if not href: continue
            if not href.startswith("http"): href = "https://" + href
            if not any(s in href for s in skip):
                return href
    except:
        pass
    return ""


# ========== HP解析 ==========
def analyze(url: str) -> dict:
    import random
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    out = {"has_recruit": False, "recruit_url": "", "phone": ""}
    if not url: return out
    try:
        res = requests.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
        if res.status_code != 200: return out

        soup = BeautifulSoup(res.text, "html.parser")
        text = soup.get_text()

        # 電話番号抽出
        for pat in [r'0\d{1,4}[-－（\(]\d{1,4}[-－）\)]\d{3,4}',
                    r'TEL[：:\s]*([0-9\-０-９]{10,})',
                    r'電話[番号]*[：:\s]*([0-9\-０-９]{10,})']:
            m = re.findall(pat, text)
            if m:
                phone = re.sub(r'[（）\(\)\s　]', '-', str(m[0])).strip('-')
                phone = re.sub(r'-+', '-', phone)
                if len(phone) >= 10:
                    out["phone"] = phone
                    break

        # 採用ページ検出
        for a in soup.find_all("a", href=True):
            href = a.get("href","")
            txt  = a.get_text(strip=True)
            if any(k in href.lower() for k in RECRUIT_URL_KW) or \
               any(k in txt for k in RECRUIT_TEXT_KW):
                out["has_recruit"] = True
                out["recruit_url"] = urljoin(url, href)
                return out

        # サブパス確認
        for path in ["/recruit","/careers","/career","/採用","/jobs","/saiyo"]:
            try:
                r2 = requests.head(urljoin(url, path), headers=HEADERS,
                                   timeout=5, allow_redirects=True)
                if r2.status_code == 200:
                    out["has_recruit"] = True
                    out["recruit_url"] = urljoin(url, path)
                    return out
            except: pass
    except: pass
    return out


# ========== 1社処理（並列から呼ばれる） ==========
def process_one(row_data: tuple) -> dict | None:
    idx, name, corp_id, pref, city, postal, kana = row_data
    try:
        site = find_website(name)
        info = analyze(site)

        result = {
            "社名":          name,
            "社名カナ":      kana,
            "法人番号":      corp_id,
            "都道府県":      pref,
            "市区町村":      city,
            "郵便番号":      postal,
            "企業HP":        site,
            "採用HP有無":    "あり" if info["has_recruit"] else "なし",
            "採用ページURL": info["recruit_url"],
            "代表電話番号":  info["phone"],
            "確認日":        datetime.now().strftime("%Y-%m-%d"),
            "_corp_id":      corp_id,
            "_has_recruit":  info["has_recruit"],
        }
        return result
    except Exception as e:
        return None


# ========== CSV追記（スレッドセーフ） ==========
def append_rows(rows: list):
    if not rows: return
    exists = os.path.exists(OUTPUT_CSV)
    with csv_lock:
        with open(OUTPUT_CSV, "a", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
            if not exists: w.writeheader()
            w.writerows(rows)


# ========== 進捗表示 ==========
def print_progress(total: int, start_time: float):
    elapsed = time.time() - start_time
    done    = counter["done"]
    recruit = counter["recruit"]
    rate    = done / elapsed if elapsed > 0 else 0
    eta_sec = (total - done) / rate if rate > 0 else 0
    eta_min = int(eta_sec // 60)
    pct     = done / total * 100 if total > 0 else 0
    bar_len = 30
    filled  = int(bar_len * done / total) if total > 0 else 0
    bar     = "█" * filled + "░" * (bar_len - filled)

    print(f"\r[{bar}] {pct:.1f}% | {done:,}/{total:,}社 | "
          f"採用HPあり:{recruit:,} | "
          f"{rate:.1f}社/秒 | 残り約{eta_min}分  ", end="", flush=True)


# ========== メイン ==========
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=WORKERS)
    parser.add_argument("--batch",   type=int, default=BATCH_SIZE)
    parser.add_argument("--pref",    type=str, default=TARGET_PREFECTURE)
    args = parser.parse_args()

    workers    = args.workers
    batch_size = args.batch

    print("=" * 60)
    print(f"採用HP自動検出【並列処理版】 workers={workers}")
    print(f"実行: {datetime.now():%Y-%m-%d %H:%M:%S}")
    print("=" * 60)

    df = load_companies()
    if df.empty: return

    db = load_db()
    print(f"[INFO] 処理済み: {len(db):,}社")

    # 未処理を抽出
    unprocessed = []
    for _, row in df.iterrows():
        cid = str(row.get("法人番号","")).strip()
        if cid and cid not in db:
            unprocessed.append((
                _,
                str(row.get("社名","")).strip(),
                cid,
                str(row.get("都道府県","")).strip(),
                str(row.get("市区町村","")).strip(),
                str(row.get("郵便番号","")).strip(),
                str(row.get("社名カナ","")).strip(),
            ))

    batch      = unprocessed[:batch_size]
    total      = len(batch)
    start_time = time.time()

    print(f"[INFO] 未処理: {len(unprocessed):,}社 → 今回: {total:,}社 ({workers}並列)\n")

    # バッファ（50件溜まったらまとめてCSV書き込み）
    buffer   = []
    db_patch = {}
    FLUSH    = 50

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(process_one, row): row for row in batch}

        for future in as_completed(futures):
            result = future.result()

            with cnt_lock:
                counter["done"] += 1
                if result and result.get("_has_recruit"):
                    counter["recruit"] += 1
                if not result:
                    counter["error"] += 1

            if result:
                buffer.append(result)
                db_patch[result["_corp_id"]] = {
                    "name":    result["社名"],
                    "date":    result["確認日"],
                    "recruit": result["_has_recruit"],
                }

            # バッファが溜まったら書き込み
            if len(buffer) >= FLUSH:
                append_rows(buffer)
                db.update(db_patch)
                save_db(db)
                buffer   = []
                db_patch = {}

            print_progress(total, start_time)

    # 残りをフラッシュ
    if buffer:
        append_rows(buffer)
        db.update(db_patch)
        save_db(db)

    elapsed = time.time() - start_time
    remaining = len(unprocessed) - total
    print(f"\n\n{'='*60}")
    print(f"✅ 完了: {counter['done']:,}社処理 | 採用HPあり: {counter['recruit']:,}社")
    print(f"⚡ 処理速度: {counter['done']/elapsed:.1f}社/秒")
    print(f"📊 累計処理済み: {len(db):,}社")
    print(f"📄 出力: {OUTPUT_CSV}")
    if remaining > 0:
        rate = counter["done"] / elapsed
        eta_days = remaining / rate / 3600 / 24 if rate > 0 else 0
        print(f"⏳ 残り: {remaining:,}社 → 約{eta_days:.1f}日で完了見込み")
    print("="*60)


if __name__ == "__main__":
    main()
