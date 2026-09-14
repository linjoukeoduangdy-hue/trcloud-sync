"""
TRCloud -> Firebase Firestore Sync (ฟรี 100%, ไม่ต้องผูกบัตร/ยืนยันเอกสาร)
==========================================================================

สคริปต์นี้:
1) ดึงสินค้าทั้งหมดจาก TRCloud (search-inventory.php) แบบวนหน้าอัตโนมัติ
2) บันทึก/อัปเดตแต่ละสินค้าเข้า Firebase Firestore (collection "inventory")
   โดยใช้ product_id เป็น document id (ทำให้ sync ซ้ำแล้วอัปเดตทับของเดิม ไม่ซ้ำ)

ถูกเรียกโดย GitHub Actions ทุก 2 ชั่วโมง (ดู .github/workflows/sync.yml)

ต้องมี environment variables (ตั้งเป็น GitHub Secrets):
  - TRCLOUD_COMPANY_ID
  - TRCLOUD_PASSKEY
  - TRCLOUD_ENCRYPT_HEAD
  - TRCLOUD_ORIGIN
  - FIREBASE_SERVICE_ACCOUNT_JSON   (เนื้อหาไฟล์ service account key แบบ JSON ทั้งก้อน)
"""

import os
import json
import time
import hashlib
import datetime

import requests
import firebase_admin
from firebase_admin import credentials, firestore


# ----------------------------------------------------------------------
# TRCloud config (อ่านจาก environment variables / GitHub Secrets)
# ----------------------------------------------------------------------
TRCLOUD_ENDPOINT = "https://thaidrill.trcloud.co/application/api-connector/end-point/engine-inventory/search-inventory.php"
COMPANY_ID = os.environ["TRCLOUD_COMPANY_ID"]
PASSKEY = os.environ["TRCLOUD_PASSKEY"]
ENCRYPT_HEAD = os.environ["TRCLOUD_ENCRYPT_HEAD"]
ORIGIN = os.environ["TRCLOUD_ORIGIN"]

PAGE_SIZE = 51


def build_secure_key(encrypt_head: str, timestamp: str) -> str:
    raw = f"{encrypt_head}t{timestamp}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def search_page(keyword: str, start: int) -> dict:
    timestamp = str(int(time.time()))
    secure_key = build_secure_key(ENCRYPT_HEAD, timestamp)

    payload = {
        "company_id": COMPANY_ID,
        "passkey": PASSKEY,
        "securekey": secure_key,
        "timestamp": timestamp,
        "keyword": keyword,
        "start": start,
    }
    form_data = {"json": json.dumps(payload)}
    headers = {"Origin": ORIGIN}

    resp = requests.post(TRCLOUD_ENDPOINT, data=form_data, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_all_products() -> list:
    """วนหน้าดึงสินค้าทั้งหมด จนกว่าหน้าล่าสุดจะได้น้อยกว่า PAGE_SIZE"""
    all_items = []
    start = 0

    while True:
        data = search_page(keyword="", start=start)

        if data.get("success") != 1:
            raise RuntimeError(f"TRCloud API error: {data.get('message')}")

        result = data.get("result") or []
        all_items.extend(result)

        if len(result) < PAGE_SIZE:
            break

        start += 1
        time.sleep(0.3)

    return all_items


def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def transform(item: dict, synced_at: str) -> dict:
    return {
        "summary_id": item.get("summary_id"),
        "company_id": item.get("company_id"),
        "product_id": item.get("product_id"),
        "product_name": item.get("product_name"),
        "product_name_en": item.get("product_name_en"),
        "balance": to_float(item.get("balance")),
        "sell_price": to_float(item.get("sell_price")),
        "buy_price": to_float(item.get("buy_price")),
        "std_cost": to_float(item.get("std_cost")),
        "ma": to_float(item.get("ma")),
        "category": item.get("category"),
        "unit": item.get("unit"),
        "tag": item.get("tag"),
        "brand": item.get("brand"),
        "status": item.get("status"),
        "reorder_point": to_float(item.get("reorder_point")),
        "maximum_stock": to_float(item.get("maximum_stock")),
        "update_dt": item.get("update_dt"),
        "raw_json": json.dumps(item, ensure_ascii=False),  # เก็บข้อมูลดิบทั้งหมดไว้เผื่อใช้ภายหลัง
        "synced_at": synced_at,
    }


def init_firestore():
    """เริ่มการเชื่อมต่อ Firebase โดยอ่าน service account จาก environment variable"""
    service_account_info = json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT_JSON"])
    cred = credentials.Certificate(service_account_info)
    firebase_admin.initialize_app(cred)
    return firestore.client()


def main():
    synced_at = datetime.datetime.now(datetime.UTC).isoformat()

    print("กำลังดึงสินค้าทั้งหมดจาก TRCloud ...")
    items = fetch_all_products()
    print(f"ดึงมาได้ {len(items)} รายการ")

    db = init_firestore()
    collection_ref = db.collection("inventory")

    # ใช้ BulkWriter แทน batch() ธรรมดา เพราะ BulkWriter จัดการ retry,
    # rate-limit และ error ชั่วคราว (เช่น DEADLINE_EXCEEDED) ให้อัตโนมัติ
    # เหมาะกับการเขียนข้อมูลจำนวนมาก (หลักพันรายการ) แบบนี้
    bulk_writer = db.bulk_writer()
    total_written = 0
    failed_ids = []

    def _on_batch_error(error, callback_attempts):
        # เก็บ id ที่เขียนไม่สำเร็จไว้ log แต่ไม่ให้ script ล้มทั้งหมด
        failed_ids.append(error.document_reference.id)
        # คืนค่า True หมายถึง "ลองใหม่อีกครั้ง" (สูงสุดตามค่า default ของ BulkWriter)
        return callback_attempts < 3

    bulk_writer.on_write_error(_on_batch_error)

    for item in items:
        product_id = item.get("product_id")
        if not product_id:
            continue

        doc_data = transform(item, synced_at)
        doc_ref = collection_ref.document(product_id)
        bulk_writer.set(doc_ref, doc_data, merge=True)
        total_written += 1

    # รอให้ทุก write เสร็จ (รวมการ retry ที่ค้างอยู่) ก่อนไปขั้นตอนถัดไป
    bulk_writer.close()

    if failed_ids:
        print(f"คำเตือน: มี {len(failed_ids)} รายการที่เขียนไม่สำเร็จหลัง retry: {failed_ids}")

    # เก็บ log การ sync ล่าสุดไว้ดูย้อนหลังได้
    db.collection("sync_logs").document(synced_at).set({
        "synced_at": synced_at,
        "total_items": total_written,
    })

    print(f"บันทึกเข้า Firestore สำเร็จ: {total_written} รายการ เมื่อ {synced_at}")


if __name__ == "__main__":
    main()
