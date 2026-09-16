"""
TRCloud -> Firebase Firestore Sync (ฟรี 100%, ไม่ต้องผูกบัตร/ยืนยันเอกสาร)
==========================================================================

เวอร์ชันนี้ sync แบบ "เขียนเฉพาะรายการที่เปลี่ยนแปลง" (diff-based)
เหตุผล: ระบบมีสินค้าจำนวนมาก (หลักพันตัว) ถ้าเขียนทับทุกตัวทุกรอบ (ทุก 2 ชม. = 12
รอบ/วัน) จะเกินโควตาฟรีของ Firestore (เขียนได้ 20,000 ครั้ง/วัน) ไปมาก

วิธีทำงาน:
1) ดึงสินค้าทั้งหมดจาก TRCloud (search-inventory.php) แบบวนหน้าอัตโนมัติ
2) เทียบกับ state.json (ไฟล์ที่เก็บ hash ของแต่ละสินค้าจากรอบ sync ก่อนหน้า
   ซึ่งถูก commit ไว้ใน git repo เอง)
3) เขียนเข้า Firestore เฉพาะสินค้าที่ hash เปลี่ยนไปจากเดิม (ของใหม่/ค่าเปลี่ยน)
4) อัปเดต state.json ใหม่ทั้งหมด ให้ GitHub Actions commit กลับเข้า repo ต่อ

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
# TRCloud config
# ----------------------------------------------------------------------
TRCLOUD_ENDPOINT = "https://thaidrill.trcloud.co/application/api-connector/end-point/engine-inventory/search-inventory.php"
COMPANY_ID = os.environ["TRCLOUD_COMPANY_ID"]
PASSKEY = os.environ["TRCLOUD_PASSKEY"]
ENCRYPT_HEAD = os.environ["TRCLOUD_ENCRYPT_HEAD"]
ORIGIN = os.environ["TRCLOUD_ORIGIN"]

PAGE_SIZE = 51
STATE_FILE = "state.json"

# กันเหนียว: ถ้ารอบไหนต้องเขียนเกินจำนวนนี้ (ผิดปกติมาก) ให้หยุดแทนที่จะยิงจนชน
# โควตาเต็มวันโดยไม่รู้ตัว (ปกติควรเขียนแค่หลักสิบ-หลักร้อยต่อรอบ ไม่ใช่หลักพัน)
MAX_WRITES_PER_RUN = 15000


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
        "raw_json": json.dumps(item, ensure_ascii=False),
        "synced_at": synced_at,
    }


def compute_hash(doc_data: dict) -> str:
    """hash เนื้อหาของสินค้า (ไม่รวม synced_at ที่เปลี่ยนทุกรอบอยู่แล้ว)
    เพื่อใช้เทียบว่าเปลี่ยนแปลงจริงหรือไม่"""
    comparable = {k: v for k, v in doc_data.items() if k != "synced_at"}
    raw = json.dumps(comparable, sort_keys=True, ensure_ascii=False)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_state(state: dict):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def init_firestore():
    service_account_info = json.loads(os.environ["FIREBASE_SERVICE_ACCOUNT_JSON"])
    cred = credentials.Certificate(service_account_info)
    firebase_admin.initialize_app(cred)
    return firestore.client()


def commit_batch_with_retry(batch, max_retries: int = 5):
    delay = 2
    for attempt in range(1, max_retries + 1):
        try:
            batch.commit()
            return
        except Exception as e:
            if attempt == max_retries:
                raise
            print(f"  batch commit ล้มเหลว (ครั้งที่ {attempt}): {e} -> รอ {delay}s แล้วลองใหม่")
            time.sleep(delay)
            delay *= 2


def main():
    synced_at = datetime.datetime.utcnow().isoformat()

    print("กำลังดึงสินค้าทั้งหมดจาก TRCloud ...")
    items = fetch_all_products()
    print(f"ดึงมาได้ {len(items)} รายการ")

    old_state = load_state()
    print(f"สถานะรอบก่อนหน้า: มีข้อมูล {len(old_state)} รายการใน state.json")

    # หาว่ารายการไหนเปลี่ยนแปลงจริง (ต่างจาก hash เดิม หรือเป็นสินค้าใหม่)
    changed_docs = []  # list of (product_id, doc_data)
    new_state = {}

    for item in items:
        product_id = item.get("product_id")
        if not product_id:
            continue

        doc_data = transform(item, synced_at)
        content_hash = compute_hash(doc_data)
        new_state[product_id] = content_hash

        if old_state.get(product_id) != content_hash:
            changed_docs.append((product_id, doc_data))

    print(f"พบรายการที่เปลี่ยนแปลง/ใหม่: {len(changed_docs)} จากทั้งหมด {len(items)} รายการ")

    if len(changed_docs) > MAX_WRITES_PER_RUN:
        raise RuntimeError(
            f"จำนวนรายการที่ต้องเขียน ({len(changed_docs)}) เกิน MAX_WRITES_PER_RUN "
            f"({MAX_WRITES_PER_RUN}) -> หยุดไว้ก่อนกันชน quota เต็มวันโดยไม่ตั้งใจ "
            f"กรุณาตรวจสอบว่าข้อมูลเปลี่ยนแปลงเยอะผิดปกติหรือไม่"
        )

    if changed_docs:
        db = init_firestore()
        collection_ref = db.collection("inventory")

        BATCH_SIZE = 300
        BATCH_DELAY_SECONDS = 1.5

        batch = db.batch()
        batch_count = 0
        total_written = 0

        for product_id, doc_data in changed_docs:
            doc_ref = collection_ref.document(product_id)
            batch.set(doc_ref, doc_data, merge=True)
            batch_count += 1
            total_written += 1

            if batch_count >= BATCH_SIZE:
                commit_batch_with_retry(batch)
                print(f"  บันทึกแล้ว {total_written} / {len(changed_docs)} รายการที่เปลี่ยนแปลง")
                batch = db.batch()
                batch_count = 0
                time.sleep(BATCH_DELAY_SECONDS)

        if batch_count > 0:
            commit_batch_with_retry(batch)

        print(f"บันทึกเข้า Firestore สำเร็จ: {total_written} รายการ เมื่อ {synced_at}")

        try:
            db.collection("sync_logs").document(synced_at).set({
                "synced_at": synced_at,
                "total_items_checked": len(items),
                "total_items_written": total_written,
            })
        except Exception as e:
            print(f"หมายเหตุ: บันทึก sync_logs ไม่สำเร็จ (ไม่กระทบข้อมูลหลักที่บันทึกไปแล้ว): {e}")
    else:
        print("ไม่มีรายการเปลี่ยนแปลง -> ข้ามการเขียน Firestore รอบนี้ (ประหยัด quota)")

    # อัปเดต state.json เสมอ (ให้ workflow commit กลับเข้า repo ต่อ)
    save_state(new_state)
    print(f"อัปเดต {STATE_FILE} เรียบร้อย ({len(new_state)} รายการ)")


if __name__ == "__main__":
    main()
