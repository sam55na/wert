import os
import time
import threading
import requests
import json
from flask import Flask, request, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from queue import Queue

# ==========================================================
# إعدادات التطبيق الأساسية
# ==========================================================
app = Flask(__name__)

# قراءة رابط قاعدة البيانات من متغيرات البيئة في Render
DATABASE_URL = os.environ.get("DATABASE_URL")

# رابط واجهة شام كاش الأصلية
SHAMCASH_API_URL = "https://api.syriacash.com/api/v1"

# ==========================================================
# نظام الطابور والقفل (Queue & Lock) لمنع التخايل والغش
# ==========================================================
verification_queue = Queue()
processing_lock = threading.Lock()
last_processed_time = 0
MIN_INTERVAL = 5  # 5 ثوانٍ بين كل عملية تحقق


def process_queue_worker():
    """
    عامل خلفي يعالج الطلبات من الطابور بفاصل زمني لا يقل عن 5 ثوانٍ.
    هذا يمنع إرسال طلبات متتالية سريعة إلى شام كاش.
    """
    global last_processed_time
    while True:
        task = None
        try:
            # جلب مهمة من الطابور (ينتظر إذا كان فارغاً)
            task = verification_queue.get()

            # تطبيق القفل والفواصل الزمنية
            with processing_lock:
                current_time = time.time()
                time_since_last = current_time - last_processed_time
                if time_since_last < MIN_INTERVAL:
                    wait_time = MIN_INTERVAL - time_since_last
                    print(f"⏳ انتظار {wait_time:.2f} ثانية لتنفيذ العملية...")
                    time.sleep(wait_time)

                # تنفيذ التحقق الفعلي من شام كاش
                result = verify_with_shamcash(
                    task['tx_id'],
                    task['api_key'],
                    task['gsm']
                )

                # تحديث وقت آخر معالجة
                last_processed_time = time.time()

            # إعادة النتيجة إلى الطلب المنتظر
            task['result_queue'].put(result)

        except Exception as e:
            print(f"❌ خطأ في عامل الطابور: {e}")
            if task and 'result_queue' in task:
                task['result_queue'].put({
                    "success": False,
                    "message": f"خطأ داخلي: {str(e)}",
                    "code": "INTERNAL_ERROR"
                })
        finally:
            if task:
                verification_queue.task_done()


# بدء العامل الخلفي عند تشغيل التطبيق
threading.Thread(target=process_queue_worker, daemon=True).start()


# ==========================================================
# دوال قاعدة البيانات
# ==========================================================
def get_db_connection():
    """إنشاء اتصال بقاعدة البيانات"""
    if not DATABASE_URL:
        raise Exception("DATABASE_URL غير معرف في متغيرات البيئة")
    return psycopg2.connect(DATABASE_URL, sslmode='require')


def init_db():
    """إنشاء الجدول إذا لم يكن موجوداً"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS verified_transactions (
                id SERIAL PRIMARY KEY,
                tx_id VARCHAR(255) UNIQUE NOT NULL,
                amount DECIMAL(20, 2) NOT NULL,
                currency VARCHAR(10) DEFAULT 'SYP',
                sender_name VARCHAR(255),
                gsm VARCHAR(50),
                verified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                raw_response JSONB
            );
        """)
        conn.commit()
        cursor.close()
        conn.close()
        print("✅ تم تهيئة جدول قاعدة البيانات بنجاح")
    except Exception as e:
        print(f"❌ فشل تهيئة قاعدة البيانات: {e}")


def is_transaction_processed(tx_id):
    """التحقق مما إذا كانت العملية قد عولجت مسبقاً"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT tx_id FROM verified_transactions WHERE tx_id = %s",
            (tx_id,)
        )
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return result is not None
    except Exception as e:
        print(f"⚠️ خطأ في التحقق من قاعدة البيانات: {e}")
        return True  # في حالة الخطأ، نعتبرها معالجة للأمان


def save_transaction(tx_id, amount, currency, sender_name, gsm, raw_data):
    """
    حفظ العملية في قاعدة البيانات.
    يستخدم ON CONFLICT DO NOTHING لمنع الأخطاء في حال التكرار.
    يرجع True إذا تم الحفظ فعلياً (عملية جديدة)، و False إذا كانت موجودة مسبقاً.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO verified_transactions
            (tx_id, amount, currency, sender_name, gsm, raw_response)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (tx_id) DO NOTHING
        """, (tx_id, amount, currency, sender_name, gsm, json.dumps(raw_data)))
        
        # التحقق من عدد الصفوف التي تم إدراجها
        rows_inserted = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        
        # إذا تم إدراج صف واحد، فهذه عملية جديدة
        return rows_inserted > 0
    except Exception as e:
        print(f"⚠️ خطأ في حفظ العملية: {e}")
        return False


# ==========================================================
# دالة التحقق من شام كاش (الأصلية)
# ==========================================================
def verify_with_shamcash(tx_id, api_key, gsm_number):
    """التحقق من العملية عبر واجهة شام كاش الأصلية"""
    try:
        params = {
            "api_key": api_key,
            "resource": "syriatel",
            "action": "find_tx",
            "tx": tx_id,
            "gsm": gsm_number
        }

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json"
        }

        response = requests.get(
            SHAMCASH_API_URL,
            params=params,
            headers=headers,
            timeout=20
        )

        if response.status_code == 200:
            data = response.json()
            if data.get("success") and data.get("data", {}).get("found"):
                transaction = data["data"].get("transaction", {})
                api_amount = float(transaction.get("amount", 0))
                sender = transaction.get("sender_name", "غير معروف")

                return {
                    "success": True,
                    "data": {
                        "found": True,
                        "transaction": {
                            "amount": api_amount,
                            "sender_name": sender
                        }
                    },
                    "message": "تم التحقق بنجاح"
                }
            else:
                return {
                    "success": False,
                    "message": data.get("message", "العملية غير موجودة"),
                    "code": "TX_NOT_FOUND"
                }
        else:
            return {
                "success": False,
                "message": f"خطأ من شام كاش: HTTP {response.status_code}",
                "code": "SHAMCASH_ERROR"
            }

    except Exception as e:
        print(f"❌ خطأ في التحقق من شام كاش: {e}")
        return {"success": False, "message": str(e), "code": "EXCEPTION"}


# ==========================================================
# المسار الرئيسي (Endpoint) الذي يستدعيه البوت
# ==========================================================
@app.route('/verify', methods=['POST'])
def verify_transaction():
    """
    نقطة النهاية التي يستدعيها البوت للتحقق من عملية شام كاش.
    
    طبقات الحماية:
    1. فحص أولي سريع من قاعدة البيانات (لمنع الطلبات المتكررة السريعة).
    2. طابور المعالجة مع قفل 5 ثوانٍ (لمنع التخايل).
    3. فحص نهائي من قاعدة البيانات بعد الحفظ (لمنع الحالة الحرجة للطلبين المتزامنين).
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({
                "success": False,
                "message": "لا توجد بيانات في الطلب",
                "code": "BAD_REQUEST"
            }), 400

        tx_id = data.get('tx_id')
        api_key = data.get('api_key')
        gsm = data.get('gsm')

        if not tx_id or not api_key:
            return jsonify({
                "success": False,
                "message": "tx_id و api_key مطلوبان",
                "code": "MISSING_PARAMS"
            }), 400

        # ==================================================
        # الطبقة 1: الفحص الأولي (التحقق السريع من قاعدة البيانات)
        # ==================================================
        if is_transaction_processed(tx_id):
            print(f"🛑 [Layer 1] محاولة تكرار محظورة (فحص أولي): {tx_id}")
            return jsonify({
                "success": False,
                "message": "رقم العملية مستخدم مسبقاً",
                "code": "TX_ALREADY_USED"
            }), 200

        # ==================================================
        # الطبقة 2: الطابور والقفل (معالجة متسلسلة بفاصل 5 ثوانٍ)
        # ==================================================
        result_queue = Queue()
        task = {
            'tx_id': tx_id,
            'api_key': api_key,
            'gsm': gsm,
            'result_queue': result_queue
        }
        verification_queue.put(task)

        # الانتظار حتى يتم معالجة المهمة (بحد أقصى 120 ثانية)
        try:
            result = result_queue.get(timeout=120)
        except Exception:
            print(f"⏰ [Layer 2] انتهت مهلة الانتظار: {tx_id}")
            return jsonify({
                "success": False,
                "message": "انتهت مهلة الانتظار في الطابور",
                "code": "QUEUE_TIMEOUT"
            }), 200

        # ==================================================
        # الطبقة 3: الفحص النهائي (لمنع حالة الطلبين المتزامنين)
        # ==================================================
        if result.get("success"):
            tx_data = result.get("data", {}).get("transaction", {})
            
            # محاولة الحفظ. إذا كانت العملية موجودة، لن يتم إدراج صف جديد
            saved = save_transaction(
                tx_id=tx_id,
                amount=tx_data.get("amount", 0),
                currency="SYP",
                sender_name=tx_data.get("sender_name", "غير معروف"),
                gsm=gsm,
                raw_data=result
            )
            
            # إذا لم يتم الحفظ، فهذا يعني أن عملية أخرى (متزامنة) سجلته للتو
            if not saved:
                print(f"🛑 [Layer 3] محاولة تكرار محظورة (فحص نهائي): {tx_id}")
                return jsonify({
                    "success": False,
                    "message": "رقم العملية مستخدم مسبقاً",
                    "code": "TX_ALREADY_USED"
                }), 200

        return jsonify(result), 200

    except Exception as e:
        print(f"❌ خطأ في المعالج الرئيسي: {e}")
        return jsonify({
            "success": False,
            "message": f"خطأ داخلي: {str(e)}",
            "code": "INTERNAL_ERROR"
        }), 500


# ==========================================================
# نقطة نهاية لفحص صحة الخادم (Health Check)
# ==========================================================
@app.route('/health', methods=['GET'])
def health_check():
    """للتحقق من صحة الخادم وقاعدة البيانات"""
    try:
        conn = get_db_connection()
        conn.close()
        return jsonify({
            "status": "healthy",
            "database": "connected",
            "queue_size": verification_queue.qsize()
        }), 200
    except Exception as e:
        return jsonify({
            "status": "unhealthy",
            "error": str(e)
        }), 500


# ==========================================================
# التشغيل
# ==========================================================
if __name__ == '__main__':
    init_db()
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
else:
    # عند التشغيل عبر gunicorn (في بيئة الإنتاج)
    init_db()
