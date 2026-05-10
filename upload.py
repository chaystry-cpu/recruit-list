"""
Googleスプレッドシート書き込みスクリプト
companies.csv の内容をスプレッドシートに反映する
"""

import csv, os
from datetime import datetime
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

SPREADSHEET_ID   = os.environ["SPREADSHEET_ID"]
CREDENTIALS_FILE = "credentials.json"
INPUT_CSV        = "companies.csv"
SHEET_NAME       = "採用HP企業リスト"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

HEADER = ["社名","社名カナ","法人番号","都道府県","市区町村","郵便番号",
          "企業HP","採用HP有無","採用ページURL","代表電話番号","確認日"]

# 列幅（ピクセル）
COL_WIDTHS = [220, 180, 150, 80, 100, 90, 280, 90, 300, 130, 100]


def get_service():
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)


def get_sheet_id(sheets):
    meta = sheets.get(spreadsheetId=SPREADSHEET_ID).execute()
    for s in meta["sheets"]:
        if s["properties"]["title"] == SHEET_NAME:
            return s["properties"]["sheetId"]
    return None


def ensure_sheet(sheets):
    """シートがなければ作成する"""
    sid = get_sheet_id(sheets)
    if sid is None:
        sheets.batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": [{
            "addSheet": {"properties": {"title": SHEET_NAME}}
        }]}).execute()
    return get_sheet_id(sheets)


def upload():
    print("[INFO] Googleスプレッドシートに書き込み中...")

    service = get_service()
    sheets  = service.spreadsheets()
    sid     = ensure_sheet(sheets)

    # CSV読み込み
    rows = [HEADER]
    recruit_rows = []
    with open(INPUT_CSV, encoding="utf-8-sig") as f:
        for i, row in enumerate(csv.DictReader(f), 2):
            r = [row.get(h, "") for h in HEADER]
            rows.append(r)
            if row.get("採用HP有無") == "あり":
                recruit_rows.append(i)

    # シートクリア → 書き込み
    sheets.values().clear(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!A:Z"
    ).execute()

    sheets.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!A1",
        valueInputOption="RAW",
        body={"values": rows}
    ).execute()

    # フォーマット：ヘッダー行
    requests_body = [
        # ヘッダー背景・文字
        {"repeatCell": {
            "range": {"sheetId": sid, "startRowIndex": 0, "endRowIndex": 1},
            "cell": {"userEnteredFormat": {
                "backgroundColor": {"red": 0.18, "green": 0.37, "blue": 0.67},
                "textFormat": {"bold": True, "fontSize": 10,
                               "foregroundColor": {"red":1,"green":1,"blue":1}},
                "horizontalAlignment": "CENTER",
                "verticalAlignment": "MIDDLE"
            }},
            "fields": "userEnteredFormat"
        }},
        # 行の高さ（ヘッダー）
        {"updateDimensionProperties": {
            "range": {"sheetId": sid, "dimension": "ROWS",
                      "startIndex": 0, "endIndex": 1},
            "properties": {"pixelSize": 32},
            "fields": "pixelSize"
        }},
        # フィルタ設定
        {"setBasicFilter": {
            "filter": {"range": {
                "sheetId": sid, "startRowIndex": 0,
                "startColumnIndex": 0, "endColumnIndex": len(HEADER)
            }}
        }},
        # ウィンドウ枠固定
        {"updateSheetProperties": {
            "properties": {
                "sheetId": sid,
                "gridProperties": {"frozenRowCount": 1}
            },
            "fields": "gridProperties.frozenRowCount"
        }},
    ]

    # 列幅設定
    for i, w in enumerate(COL_WIDTHS):
        requests_body.append({"updateDimensionProperties": {
            "range": {"sheetId": sid, "dimension": "COLUMNS",
                      "startIndex": i, "endIndex": i+1},
            "properties": {"pixelSize": w},
            "fields": "pixelSize"
        }})

    # 採用HPあり行を緑ハイライト
    for row_idx in recruit_rows:
        requests_body.append({"repeatCell": {
            "range": {"sheetId": sid,
                      "startRowIndex": row_idx-1, "endRowIndex": row_idx,
                      "startColumnIndex": 7, "endColumnIndex": 8},
            "cell": {"userEnteredFormat": {
                "backgroundColor": {"red": 0.85, "green": 0.97, "blue": 0.87},
                "textFormat": {"bold": True,
                               "foregroundColor": {"red":0.1,"green":0.5,"blue":0.2}}
            }},
            "fields": "userEnteredFormat"
        }})

    sheets.batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={"requests": requests_body}
    ).execute()

    total    = len(rows) - 1
    recruit  = len(recruit_rows)
    print(f"✅ 完了: {total:,}社 | 採用HPあり: {recruit:,}社 ({recruit/total*100:.1f}%)")


if __name__ == "__main__":
    upload()
