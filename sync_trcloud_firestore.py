"""
TRCloud -> Firebase Firestore Sync (ฟรี 100%, ไม่ต้องผูกบัตร/ยืนยันเอกสาร)
==========================================================================

เวอร์ชันนี้ sync แบบ "เขียนเฉพาะรายการที่เปลี่ยนแปลง" (diff-based)
เหตุผล: ระบบมีสินค้าจำนวนมาก (หลักพันตัว) ถ้าเขียนทับทุกตัวทุกรอบ (ทุก 2 ชม. = 12
รอบ/วัน) จะเกินโควตาฟรีของ Firestore (เขียนได้ 20,000 ครั้ง/วัน) ไปมาก

เพิ่มความทนทาน (v2):
- batch เล็กลง (100) + delay ระหว่าง batch นานขึ้น (3s) กันชน quota ของฐานข้อมูล
  ที่เพิ่งสร้างใหม่ (ช่วงแรกมีเพดานความเร็วต่ำ ค่อยๆ ขยับขึ้นเองภายในไม่กี่วัน)
- บันทึก state.json สะสมทีละ batch (ไม่รอจบทั้งหมด) กันข้อมูลหายถ้ารันไม่จบ
- ถ้าบาง batch ล้มเหลวแม้ retry ครบแล้ว จะข้ามไปก่อน (ไม่ทำให้ทั้ง script ล้ม)
  แล้วปล่อยให้รอบ sync ถัดไป (อีก 2 ชม.) ลองใหม่เฉพาะรายการที่ยังไม่สำเร็จ

วิธีทำงาน:
1) ดึงสินค้าทั้งหมดจาก TRCloud (search-inventory.php) แบบวนหน้าอัตโนมัติ
2) เทียบกับ state.json (ไฟล์ที่เก็บ hash ของแต่ละสินค้าจากรอบ sync ก่อนหน้า
   ซึ่งถูก commit ไว้ใน git repo เอง)
3) เขียนเข้า Firestore เฉพาะสินค้าที่ hash เปลี่ยนไปจากเดิม (ของใหม่/ค่าเปลี่ยน)
4) อัปเดต state.json ให้ GitHub Actions commit กลับเข้า repo ต่อ

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


def commit_batch_with_retry(batch, max_retries: int = 5, base_delay: int = 3, max_delay: int = 60) -> bool:
    """commit batch พร้อม retry แบบ exponential backoff (มี cap ไม่ให้รอนานเกินไป)
    และเพิ่ม timeout ให้ยาวขึ้น (120s) เพราะฐานข้อมูลใหม่บางครั้งตอบช้ากว่าปกติ
    คืนค่า True ถ้าสำเร็จ, False ถ้าล้มเหลวครบทุกครั้ง (ไม่ raise exception ทำให้ script ล้มทั้งหมด)"""
    delay = base_delay
    for attempt in range(1, max_retries + 1):
        try:
            batch.commit(timeout=120)
            return True
        except Exception as e:
            print(f"  batch commit ล้มเหลว (ครั้งที่ {attempt}/{max_retries}): {e}")
            if attempt == max_retries:
                return False
            print(f"    รอ {delay}s แล้วลองใหม่ ...")
            time.sleep(delay)
            delay = min(delay * 2, max_delay)
    return False


def main():
    synced_at = datetime.datetime.utcnow().isoformat()

    print("กำลังดึงสินค้าทั้งหมดจาก TRCloud ...")
    items = fetch_all_products()
    print(f"ดึงมาได้ {len(items)} รายการ")

    old_state = load_state()
    print(f"สถานะรอบก่อนหน้า: มีข้อมูล {len(old_state)} รายการใน state.json")

    changed_docs = []       # list of (product_id, doc_data, content_hash)
    unchanged_state = {}    # hash เดิมของรายการที่ไม่เปลี่ยน

    for item in items:
        product_id = item.get("product_id")
        if not product_id:
            continue

        doc_data = transform(item, synced_at)
        content_hash = compute_hash(doc_data)

        if old_state.get(product_id) != content_hash:
            changed_docs.append((product_id, doc_data, content_hash))
        else:
            unchanged_state[product_id] = content_hash

    print(f"พบรายการที่เปลี่ยนแปลง/ใหม่: {len(changed_docs)} จากทั้งหมด {len(items)} รายการ")

    if len(changed_docs) > MAX_WRITES_PER_RUN:
        raise RuntimeError(
            f"จำนวนรายการที่ต้องเขียน ({len(changed_docs)}) เกิน MAX_WRITES_PER_RUN "
            f"({MAX_WRITES_PER_RUN}) -> หยุดไว้ก่อนกันชน quota เต็มวันโดยไม่ตั้งใจ "
            f"กรุณาตรวจสอบว่าข้อมูลเปลี่ยนแปลงเยอะผิดปกติหรือไม่"
        )

    # state ที่จะบันทึกจริง = รายการที่ไม่เปลี่ยน + จะเติมรายการที่เขียนสำเร็จเพิ่มทีละ chunk
    confirmed_state = dict(unchanged_state)
    save_state(confirmed_state)

    success_count = 0
    failed_count = 0

    if changed_docs:
        db = init_firestore()
        collection_ref = db.collection("inventory")

        BATCH_SIZE = 100
        BATCH_DELAY_SECONDS = 3

        def flush_chunk(chunk_items):
            nonlocal success_count, failed_count
            if not chunk_items:
                return
            batch = db.batch()
            for product_id, doc_data, _ in chunk_items:
                doc_ref = collection_ref.document(product_id)
                batch.set(doc_ref, doc_data, merge=True)

            ok = commit_batch_with_retry(batch)
            if ok:
                for product_id, _, content_hash in chunk_items:
                    confirmed_state[product_id] = content_hash
                success_count += len(chunk_items)
                print(f"  บันทึกสำเร็จ {success_count} / {len(changed_docs)} รายการที่เปลี่ยนแปลง")
            else:
                failed_count += len(chunk_items)
                print(f"  !! ข้าม chunk นี้ไป ({len(chunk_items)} รายการ) จะลองใหม่รอบ sync ถัดไปแทน")

            save_state(confirmed_state)
            time.sleep(BATCH_DELAY_SECONDS)

        chunk = []
        for entry in changed_docs:
            chunk.append(entry)
            if len(chunk) >= BATCH_SIZE:
                flush_chunk(chunk)
                chunk = []
        flush_chunk(chunk)

        print(f"\nสรุป: สำเร็จ {success_count} รายการ, ล้มเหลว {failed_count} รายการ (จะลองใหม่รอบถัดไป)")

        try:
            db.collection("sync_logs").document(synced_at).set({
                "synced_at": synced_at,
                "total_items_checked": len(items),
                "total_items_written": success_count,
                "total_items_failed": failed_count,
            })
        except Exception as e:
            print(f"หมายเหตุ: บันทึก sync_logs ไม่สำเร็จ (ไม่กระทบข้อมูลหลักที่บันทึกไปแล้ว): {e}")
    else:
        print("ไม่มีรายการเปลี่ยนแปลง -> ข้ามการเขียน Firestore รอบนี้ (ประหยัด quota)")

    print(f"อัปเดต {STATE_FILE} เรียบร้อย ({len(confirmed_state)} รายการที่ยืนยันสำเร็จสะสม)")

    if changed_docs and success_count == 0:
        raise RuntimeError("เขียนเข้า Firestore ไม่สำเร็จเลยสักรายการในรอบนี้")


if __name__ == "__main__":
    main()
