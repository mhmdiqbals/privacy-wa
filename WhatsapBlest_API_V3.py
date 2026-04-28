"""
SISTEM PENGIRIMAN WHATSAPP MASSAL V3 - FULL META INTEGRATION
Menggunakan WhatsApp Cloud API dengan template dari Meta
PT. Dirja Sasak Utama

Template: kredivo (APPROVED)
Parameter: 
  {{1}} = Nama
  {{2}} = Nomor
  {{3}} = Total (Rp)
  {{4}} = Nomor Telepon CS

🔥 FIXED VERSION 3.3:
- Template khusus "kredivo" dengan 4 parameter
- Auto-detect parameter dari template Meta
- Optimasi untuk template yang sudah APPROVED
"""

import os
import re
import time
import json
import threading
import requests
import pandas as pd
import logging
import sqlite3
import secrets
import signal
import sys
from datetime import datetime
from functools import wraps
from threading import Semaphore
from werkzeug.utils import secure_filename
from dotenv import load_dotenv
from flask import Flask, request, jsonify, render_template_string, redirect, session, send_file

# =========================
# FIX: SQLITE3 DATETIME ADAPTER FOR PYTHON 3.12+
# =========================
def adapt_datetime(dt):
    """Adapt datetime to ISO format string for sqlite3"""
    return dt.isoformat()

def convert_datetime(s):
    """Convert ISO format string back to datetime"""
    return datetime.fromisoformat(s)

sqlite3.register_adapter(datetime, adapt_datetime)
sqlite3.register_converter("timestamp", convert_datetime)
sqlite3.register_converter("TIMESTAMP", convert_datetime)

# =========================
# LOAD ENVIRONMENT VARIABLES
# =========================
load_dotenv()

# =========================
# KONFIGURASI DASAR
# =========================

CONFIG_FILE = "config.json"
UPLOAD_FOLDER = "uploads"
DATABASE_FILE = "messages.db"
LOG_FILE = "app.log"
EXPORT_FOLDER = "exports"

# Rate limiting yang aman
WAIT_BETWEEN_MESSAGES = 3  # Jeda antar pengiriman (detik)
MAX_FILE_SIZE = 10 * 1024 * 1024
ALLOWED_EXTENSIONS = {'xlsx', 'xls'}
MAX_CONCURRENT_SENDS = 5
MAX_RETRIES = 3
RETRY_DELAY = 2

# Template default untuk kredivo
DEFAULT_TEMPLATE_NAME = "kredivo"
DEFAULT_PARAM_NAMES = ["nama", "nomor", "total", "cs_number"]
DEFAULT_TEMPLATE_LANGUAGE = "id"

# Membuat folder yang diperlukan
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(EXPORT_FOLDER, exist_ok=True)

# =========================
# LOGGING CONFIGURATION
# =========================
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

logger = logging.getLogger(__name__)

# =========================
# FLASK APP INITIALIZATION
# =========================
app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', secrets.token_hex(16))

# =========================
# RATE LIMITER
# =========================
rate_limiter = Semaphore(MAX_CONCURRENT_SENDS)

def rate_limit(seconds=WAIT_BETWEEN_MESSAGES):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with rate_limiter:
                result = func(*args, **kwargs)
                time.sleep(seconds)
                return result
        return wrapper
    return decorator

# =========================
# PROGRESS DATA
# =========================
progress_data = {
    "total": 0,
    "current": 0,
    "status": "idle",
    "success": 0,
    "failed": 0,
    "retried": 0,
    "errors": [],
    "start_time": None,
    "estimated_time_remaining": None,
    "message": ""
}

# =========================
# DATABASE INITIALIZATION WITH MIGRATION
# =========================
def init_database():
    try:
        conn = sqlite3.connect(DATABASE_FILE, detect_types=sqlite3.PARSE_DECLTYPES)
        c = conn.cursor()
        
        # Tabel pesan keluar
        c.execute('''CREATE TABLE IF NOT EXISTS messages
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      phone_number TEXT,
                      template_name TEXT,
                      status TEXT,
                      response TEXT,
                      params TEXT,
                      created_at TIMESTAMP)''')
        
        # Tabel pesan masuk
        c.execute('''CREATE TABLE IF NOT EXISTS incoming_messages
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      wa_id TEXT,
                      sender_name TEXT,
                      message_id TEXT UNIQUE,
                      message_type TEXT,
                      message_content TEXT,
                      raw_data TEXT,
                      is_read BOOLEAN DEFAULT 0,
                      replied BOOLEAN DEFAULT 0,
                      received_at TIMESTAMP,
                      read_at TIMESTAMP,
                      replied_at TIMESTAMP,
                      reply_message TEXT)''')
        
        # Tabel log pengiriman
        c.execute('''CREATE TABLE IF NOT EXISTS sending_logs
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      batch_id TEXT,
                      total_messages INTEGER,
                      success_count INTEGER,
                      failed_count INTEGER,
                      start_time TIMESTAMP,
                      end_time TIMESTAMP,
                      file_name TEXT)''')
        
        # Tabel template mapping
        c.execute('''CREATE TABLE IF NOT EXISTS template_mapping
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      template_name TEXT,
                      template_language TEXT,
                      param_count INTEGER,
                      param_names TEXT,
                      component_type TEXT DEFAULT 'body',
                      created_at TIMESTAMP,
                      updated_at TIMESTAMP)''')
        
        # ========== MIGRASI DATABASE ==========
        c.execute("PRAGMA table_info(messages)")
        columns = [col[1] for col in c.fetchall()]
        
        if 'error_code' not in columns:
            c.execute("ALTER TABLE messages ADD COLUMN error_code TEXT")
            logger.info("Added error_code column to messages")
        
        if 'error_message' not in columns:
            c.execute("ALTER TABLE messages ADD COLUMN error_message TEXT")
            logger.info("Added error_message column to messages")
        
        if 'retry_count' not in columns:
            c.execute("ALTER TABLE messages ADD COLUMN retry_count INTEGER DEFAULT 0")
            logger.info("Added retry_count column to messages")
        
        c.execute("PRAGMA table_info(sending_logs)")
        log_columns = [col[1] for col in c.fetchall()]
        
        if 'retry_count' not in log_columns:
            c.execute("ALTER TABLE sending_logs ADD COLUMN retry_count INTEGER DEFAULT 0")
            logger.info("Added retry_count column to sending_logs")
        
        # Insert default template mapping for kredivo
        c.execute("SELECT id FROM template_mapping WHERE template_name = ?", (DEFAULT_TEMPLATE_NAME,))
        if not c.fetchone():
            c.execute("""INSERT INTO template_mapping 
                         (template_name, template_language, param_count, param_names, component_type, created_at, updated_at) 
                         VALUES (?, ?, ?, ?, ?, ?, ?)""",
                      (DEFAULT_TEMPLATE_NAME, DEFAULT_TEMPLATE_LANGUAGE, 4, 
                       json.dumps(DEFAULT_PARAM_NAMES), "body", datetime.now(), datetime.now()))
            logger.info("Inserted default template mapping for kredivo")
        
        conn.commit()
        conn.close()
        logger.info("Database berhasil diinisialisasi dengan migrasi")
        
    except Exception as e:
        logger.error(f"Gagal inisialisasi database: {e}")

init_database()

# =========================
# FUNGSI INTI: MENGAMBIL TEMPLATE DARI META
# =========================

def fetch_templates_from_meta(access_token, waba_id, graph_version="v18.0"):
    """
    Mengambil daftar template WhatsApp dari Meta
    WAJIB menggunakan WABA ID (bukan Phone Number ID)
    """
    try:
        url = f"https://graph.facebook.com/{graph_version}/{waba_id}/message_templates"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        logger.info(f"Mengambil template dari Meta menggunakan WABA ID: {waba_id}")
        logger.info(f"URL: {url}")
        
        response = requests.get(url, headers=headers, timeout=30)
        data = response.json()

        logger.info("===== RESPONSE META =====")
        logger.info(json.dumps(data, indent=2)[:1000])
        logger.info("========================")
        
        if 'data' in data:
            templates = []
            for template in data['data']:
                templates.append({
                    'name': template.get('name'),
                    'status': template.get('status'),
                    'language': template.get('language'),
                    'category': template.get('category'),
                    'components': template.get('components', [])
                })
                logger.info(f"Found template: {template.get('name')} - Status: {template.get('status')}")
            logger.info(f"Berhasil mengambil {len(templates)} template dari Meta")
            return templates
        elif 'error' in data:
            logger.error(f"Error dari Meta: {data['error'].get('message', 'Unknown error')}")
            return []
        else:
            logger.error(f"Gagal mengambil template: {data}")
            return []
            
    except Exception as e:
        logger.error(f"Error mengambil template dari Meta: {e}")
        return []

def get_template_structure_from_meta(access_token, waba_id, template_name, language="id"):
    """
    Mendapatkan struktur template (jumlah parameter, format) dari Meta
    WAJIB menggunakan WABA ID
    """
    try:
        url = f"https://graph.facebook.com/v18.0/{waba_id}/message_templates"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        logger.info(f"Mencari template '{template_name}' menggunakan WABA ID: {waba_id}")
        
        response = requests.get(url, headers=headers, timeout=30)
        data = response.json()
        
        logger.info(f"Response Meta API: {json.dumps(data, indent=2)[:500]}")
        
        if 'data' in data and len(data['data']) > 0:
            for template in data['data']:
                if template.get('name', '').lower() == template_name.lower():
                    original_name = template.get('name')
                    logger.info(f"Template ditemukan: {original_name} (dicari: {template_name})")
                    
                    # Cari komponen BODY
                    template_text = ""
                    param_count = 0
                    
                    for component in template.get('components', []):
                        if component.get('type') == 'BODY':
                            text = component.get('text', '')
                            template_text = text
                            
                            # Hitung parameter dari placeholder {{1}}, {{2}}, dll
                            placeholders = re.findall(r'\{\{(\d+)\}\}', text)
                            if placeholders:
                                param_count = len(set(placeholders))
                            
                            # Cek dari example jika ada
                            example = component.get('example', {})
                            if 'body_text' in example and example['body_text']:
                                if not param_count:
                                    param_count = len(example['body_text'][0]) if example['body_text'][0] else 0
                            
                            break
                    
                    # Untuk template kredivo, pastikan param_count = 4
                    if template_name.lower() == "kredivo" and param_count == 0:
                        param_count = 4
                        template_text = """Halo {{1}},

Kami dari PT. DIRJA SASAK UTAMA Agent yang bekerja sama dengan Kredivo.

Kami menginformasikan bahwa terdapat kewajiban pembayaran yang masih tertunda.

Nomor: {{2}}
Total: Rp {{3}}

Untuk informasi atau penyelesaian lebih lanjut dan metode pembayaran, Anda dapat menghubungi tim kami di {{4}}.

Terima kasih."""
                    
                    return {
                        'param_count': param_count,
                        'template_text': template_text,
                        'language': template.get('language'),
                        'status': template.get('status'),
                        'name': original_name,
                        'category': template.get('category')
                    }
            
            logger.warning(f"Template '{template_name}' tidak ditemukan dalam daftar")
            available = [t.get('name') for t in data['data']]
            logger.info(f"Templates available: {available}")
            return None
        
        logger.error(f"Tidak ada data template dari Meta: {data}")
        return None
        
    except Exception as e:
        logger.error(f"Error mendapatkan struktur template: {e}")
        return None

# =========================
# FUNGSI UTAMA PENGIRIMAN
# =========================

def format_phone_number(nomor):
    """
    Format nomor telepon ke format internasional (62...)
    """
    try:
        nomor = str(nomor).strip()
        nomor = re.sub(r"[^\d]", "", nomor)
        
        if not nomor:
            return ""
        
        if nomor.startswith("0"):
            nomor = "62" + nomor[1:]
        elif nomor.startswith("62"):
            pass
        elif nomor.startswith("+"):
            nomor = nomor[1:]
        else:
            nomor = "62" + nomor
            
        return nomor
    except Exception as e:
        logger.error(f"Error formatting number {nomor}: {e}")
        return ""

def get_template_with_correct_case(access_token, waba_id, template_name, graph_version="v18.0"):
    """
    Mendapatkan nama template dengan case yang benar dari Meta
    """
    try:
        url = f"https://graph.facebook.com/{graph_version}/{waba_id}/message_templates"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        data = response.json()
        
        if 'data' in data:
            for template in data['data']:
                if template.get('name', '').lower() == template_name.lower():
                    return template.get('name')
        return template_name
    except Exception as e:
        logger.error(f"Error getting correct case: {e}")
        return template_name

def format_currency(value):
    """Format angka ke format Rupiah"""
    try:
        if isinstance(value, (int, float)):
            # Format dengan pemisah ribuan
            return f"{value:,.0f}".replace(",", ".")
        return str(value)
    except:
        return str(value)

@rate_limit(WAIT_BETWEEN_MESSAGES)
def send_whatsapp_template(config, phone_number, parameters, attempt=0):
    """
    KIRIM PESAN: WAJIB pakai Phone Number ID (bukan WABA ID)
    Parameters untuk template kredivo:
      param1: Nama
      param2: Nomor
      param3: Total (Rp)
      param4: Nomor CS
    """
    url = f"https://graph.facebook.com/{config['graph_version']}/{config['phone_number_id']}/messages"
    
    headers = {
        "Authorization": f"Bearer {config['access_token']}",
        "Content-Type": "application/json"
    }
    
    # Format parameters sesuai dengan template
    formatted_params = []
    for i, param in enumerate(parameters):
        # Untuk parameter ke-3 (total), format sebagai currency
        if i == 2:  # Parameter total (indeks ke-3, 0-based)
            formatted_params.append({
                "type": "text",
                "text": format_currency(param) if param else "0"
            })
        else:
            formatted_params.append({
                "type": "text",
                "text": str(param) if param else ""
            })
    
    # Get template name with correct case
    correct_template_name = get_template_with_correct_case(
        config['access_token'],
        config['waba_id'],
        config["template_name"],
        config['graph_version']
    )
    
    # Try multiple language codes
    language_codes_to_try = [config.get("template_language", "id"), "id", "id_ID"]
    language_codes_to_try = list(dict.fromkeys(language_codes_to_try))
    
    last_error = None
    
    for lang_code in language_codes_to_try:
        payload = {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": phone_number,
            "type": "template",
            "template": {
                "name": correct_template_name,
                "language": {
                    "code": lang_code
                },
                "components": [
                    {
                        "type": "body",
                        "parameters": formatted_params
                    }
                ]
            }
        }
        
        logger.info(f"Mengirim pesan ke {phone_number} dengan template: {correct_template_name}, language: {lang_code} (attempt {attempt + 1})")
        logger.info(f"Parameters: Nama={parameters[0] if len(parameters)>0 else ''}, Nomor={parameters[1] if len(parameters)>1 else ''}, Total=Rp{parameters[2] if len(parameters)>2 else ''}, CS={parameters[3] if len(parameters)>3 else ''}")
        
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            response_data = response.json()
            
            if response.status_code == 200 and 'messages' in response_data:
                logger.info(f"Berhasil mengirim ke {phone_number} dengan language: {lang_code}")
                log_message(phone_number, correct_template_name, 'success', response_data, parameters)
                return {"success": True, "data": response_data}
            else:
                error_msg = response_data.get('error', {}).get('message', 'Unknown error')
                error_code = response_data.get('error', {}).get('code', 0)
                logger.warning(f"Gagal dengan language {lang_code}: {error_code} - {error_msg}")
                last_error = {"error": error_msg, "code": error_code}
                
                if error_code != 132001:
                    break
                    
        except Exception as e:
            logger.error(f"Error sending: {e}")
            last_error = {"error": str(e), "code": 0}
    
    if last_error:
        logger.error(f"Gagal kirim ke {phone_number}: {last_error.get('code')} - {last_error.get('error')}")
        log_message(phone_number, config['template_name'], 'failed', {}, parameters, 
                   last_error.get('code'), last_error.get('error'), attempt)
        return {"success": False, "error": last_error.get('error'), "code": last_error.get('code')}
    
    return {"success": False, "error": "Unknown error"}

def send_with_retry(config, phone_number, parameters):
    """
    Mengirim dengan mekanisme retry
    """
    result = None
    for attempt in range(MAX_RETRIES):
        result = send_whatsapp_template(config, phone_number, parameters, attempt)
        
        if result["success"]:
            return result, attempt
        
        no_retry_codes = [100, 131026, 132001, 132002, 132003, 132005, 132006, 132007, 132008, 132009]
        
        if result.get("code") in no_retry_codes:
            logger.error(f"Error permanen, tidak di-retry: {result.get('code')}")
            return result, attempt
        
        if attempt < MAX_RETRIES - 1:
            wait_time = RETRY_DELAY * (attempt + 1)
            logger.warning(f"Retry {attempt + 1}/{MAX_RETRIES} dalam {wait_time} detik")
            time.sleep(wait_time)
    
    return result, MAX_RETRIES - 1

def process_excel_file(filepath):
    """
    Memproses file Excel dan mengirim pesan
    Format Excel yang diharapkan:
    - Kolom: nomor, nama, total, cs_number (atau sesuai mapping)
    """
    config = load_config()
    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + secrets.token_hex(4)
    
    if not config.get('phone_number_id') or len(config.get('phone_number_id', '')) < 10:
        error_msg = "Phone Number ID tidak valid atau belum diisi. Silakan isi di halaman Pengaturan API."
        logger.error(error_msg)
        progress_data.update({
            "status": "error",
            "message": error_msg,
            "errors": [error_msg]
        })
        return
    
    # Validate template exists before starting
    if config.get('access_token') and config.get('waba_id') and config.get('template_name'):
        correct_name = get_template_with_correct_case(
            config['access_token'],
            config['waba_id'],
            config['template_name'],
            config.get('graph_version', 'v18.0')
        )
        if correct_name != config['template_name']:
            logger.warning(f"Template name case corrected: {config['template_name']} -> {correct_name}")
            config['template_name'] = correct_name
            save_config(config)
    
    try:
        df = pd.read_excel(filepath)
        logger.info(f"Memproses {filepath} dengan {len(df)} baris")
        logger.info(f"Kolom yang tersedia: {list(df.columns)}")
        
        # Cari kolom nomor (case insensitive)
        nomor_column = None
        for col in df.columns:
            if col.lower() in ['nomor', 'phone', 'no', 'telepon', 'number']:
                nomor_column = col
                break
        
        if nomor_column is None:
            error_msg = "Kolom 'nomor' tidak ditemukan di Excel. Kolom yang tersedia: " + ", ".join(df.columns)
            logger.error(error_msg)
            progress_data.update({
                "status": "error",
                "message": error_msg,
                "errors": [error_msg]
            })
            return
        
        # Mapping parameter untuk template kredivo
        # Default: nama, nomor, total, cs_number
        param_mapping = {
            'nama': None,
            'nomor': None,
            'total': None,
            'cs_number': None
        }
        
        # Cari kolom yang sesuai untuk setiap parameter
        for param in param_mapping.keys():
            for col in df.columns:
                if col.lower() == param.lower():
                    param_mapping[param] = col
                    break
            # Jika tidak ditemukan, coba dengan nama alternatif
            if param == 'nama' and not param_mapping['nama']:
                for col in df.columns:
                    if col.lower() in ['name', 'customer', 'debitur', 'pelanggan']:
                        param_mapping['nama'] = col
                        break
            elif param == 'total' and not param_mapping['total']:
                for col in df.columns:
                    if col.lower() in ['tagihan', 'amount', 'nominal', 'utang', 'payment']:
                        param_mapping['total'] = col
                        break
            elif param == 'cs_number' and not param_mapping['cs_number']:
                # Gunakan nomor CS default dari config atau yang sudah diset
                param_mapping['cs_number'] = config.get('default_cs_number', '08123456789')
        
        progress_data.update({
            "total": len(df),
            "current": 0,
            "status": "running",
            "success": 0,
            "failed": 0,
            "retried": 0,
            "errors": [],
            "start_time": time.time(),
            "message": "Memulai pengiriman..."
        })
        
        log_batch_start(batch_id, len(df), os.path.basename(filepath))
        total_retries = 0
        
        for index, row in df.iterrows():
            try:
                if progress_data["status"] == "stopped":
                    logger.info("Proses dihentikan user")
                    progress_data["message"] = "Proses dihentikan"
                    break
                
                phone_number = format_phone_number(row[nomor_column])
                
                if not phone_number:
                    progress_data["failed"] += 1
                    progress_data["errors"].append(f"Baris {index+2}: Nomor tidak valid: {row[nomor_column]}")
                    progress_data["current"] += 1
                    continue
                
                # Siapkan parameters untuk template (4 parameter)
                # {{1}} = Nama
                # {{2}} = Nomor
                # {{3}} = Total
                # {{4}} = Nomor CS
                
                nama = str(row[param_mapping['nama']]) if param_mapping['nama'] and param_mapping['nama'] in row else ""
                nomor_tagihan = str(row[param_mapping['nomor']]) if param_mapping['nomor'] and param_mapping['nomor'] in row else phone_number
                total = row[param_mapping['total']] if param_mapping['total'] and param_mapping['total'] in row else 0
                cs_number = param_mapping['cs_number'] if isinstance(param_mapping['cs_number'], str) else str(param_mapping['cs_number']) if param_mapping['cs_number'] else config.get('default_cs_number', '08123456789')
                
                parameters = [nama, nomor_tagihan, total, cs_number]
                
                logger.info(f"Baris {index+2}: Mengirim ke {phone_number} - Nama: {nama[:20]}...")
                
                result, retry_used = send_with_retry(config, phone_number, parameters)
                total_retries += retry_used
                
                if result["success"]:
                    progress_data["success"] += 1
                    logger.info(f"Berhasil: {phone_number}")
                else:
                    progress_data["failed"] += 1
                    error_msg = result.get('error', 'Unknown error')
                    progress_data["errors"].append(f"Baris {index+2} ({phone_number}): {error_msg}")
                    logger.error(f"Gagal: {phone_number} - {error_msg}")
                
                progress_data["retried"] = total_retries
                progress_data["current"] += 1
                
                if progress_data["current"] > 0 and progress_data["start_time"]:
                    elapsed = time.time() - progress_data["start_time"]
                    avg_time = elapsed / progress_data["current"]
                    remaining = progress_data["total"] - progress_data["current"]
                    progress_data["estimated_time_remaining"] = avg_time * remaining
                
                if progress_data["current"] % 5 == 0:
                    progress_data["message"] = f"Terkirim {progress_data['current']}/{progress_data['total']}"
                    
            except Exception as e:
                logger.error(f"Error processing row {index+2}: {e}")
                progress_data["failed"] += 1
                progress_data["errors"].append(f"Baris {index+2}: {str(e)}")
                progress_data["current"] += 1
        
        progress_data["status"] = "done"
        progress_data["message"] = f"Selesai! Sukses: {progress_data['success']} | Gagal: {progress_data['failed']} | Retry: {total_retries}"
        
        log_batch_end(batch_id, progress_data['success'], progress_data['failed'], total_retries)
        logger.info(f"Batch {batch_id} selesai. Sukses: {progress_data['success']}, Gagal: {progress_data['failed']}")
        
    except Exception as e:
        error_msg = f"Error processing Excel: {e}"
        logger.error(error_msg)
        progress_data.update({
            "status": "error",
            "message": error_msg,
            "errors": [error_msg]
        })
    finally:
        try:
            if os.path.exists(filepath):
                os.remove(filepath)
        except Exception as e:
            logger.error(f"Gagal hapus file: {e}")

# =========================
# DATABASE FUNCTIONS
# =========================

def log_message(phone_number, template_name, status, response, params=None, error_code=None, error=None, retry_count=0):
    try:
        conn = sqlite3.connect(DATABASE_FILE, detect_types=sqlite3.PARSE_DECLTYPES)
        c = conn.cursor()
        now = datetime.now()
        c.execute("""INSERT INTO messages 
                     (phone_number, template_name, status, response, params, error_code, error_message, retry_count, created_at) 
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                  (phone_number, template_name, status, json.dumps(response), 
                   json.dumps(params) if params else None, error_code, error, retry_count, now))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Gagal log ke database: {e}")

def log_batch_start(batch_id, total, file_name):
    try:
        conn = sqlite3.connect(DATABASE_FILE, detect_types=sqlite3.PARSE_DECLTYPES)
        c = conn.cursor()
        now = datetime.now()
        c.execute("""INSERT INTO sending_logs 
                     (batch_id, total_messages, start_time, file_name, success_count, failed_count, retry_count) 
                     VALUES (?, ?, ?, ?, 0, 0, 0)""",
                  (batch_id, total, now, file_name))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Gagal log batch start: {e}")

def log_batch_end(batch_id, success, failed, retries=0):
    try:
        conn = sqlite3.connect(DATABASE_FILE, detect_types=sqlite3.PARSE_DECLTYPES)
        c = conn.cursor()
        now = datetime.now()
        c.execute("""UPDATE sending_logs 
                     SET end_time = ?, success_count = ?, failed_count = ?, retry_count = ? 
                     WHERE batch_id = ?""",
                  (now, success, failed, retries, batch_id))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Gagal log batch end: {e}")

# =========================
# CONFIGURATION HANDLERS
# =========================

def load_config():
    config = {}
    
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r") as f:
                config = json.load(f)
        except Exception as e:
            logger.error(f"Gagal baca config: {e}")
    
    # Set default untuk template kredivo
    config['access_token'] = os.getenv('WHATSAPP_ACCESS_TOKEN', config.get('access_token', ''))
    config['phone_number_id'] = os.getenv('WHATSAPP_PHONE_ID', config.get('phone_number_id', ''))
    config['waba_id'] = os.getenv('WHATSAPP_WABA_ID', config.get('waba_id', ''))
    config['graph_version'] = os.getenv('GRAPH_VERSION', config.get('graph_version', 'v18.0'))
    config['template_name'] = os.getenv('TEMPLATE_NAME', config.get('template_name', DEFAULT_TEMPLATE_NAME))
    config['template_language'] = os.getenv('TEMPLATE_LANGUAGE', config.get('template_language', DEFAULT_TEMPLATE_LANGUAGE))
    config['param_names'] = config.get('param_names', DEFAULT_PARAM_NAMES)
    config['webhook_verify_token'] = config.get('webhook_verify_token', 'token_rahasia_123')
    config['default_cs_number'] = config.get('default_cs_number', '08123456789')
    
    if config.get('phone_number_id') and len(config.get('phone_number_id', '')) < 10:
        logger.warning(f"Phone Number ID mungkin tidak valid: {config['phone_number_id']}")
    
    if config.get('waba_id') and len(config.get('waba_id', '')) < 10:
        logger.warning(f"WABA ID mungkin tidak valid: {config['waba_id']}")
    
    return config

def save_config(data):
    try:
        save_data = {
            'access_token': data.get('access_token', ''),
            'phone_number_id': data.get('phone_number_id', ''),
            'waba_id': data.get('waba_id', ''),
            'graph_version': data.get('graph_version', 'v18.0'),
            'template_name': data.get('template_name', DEFAULT_TEMPLATE_NAME),
            'template_language': data.get('template_language', DEFAULT_TEMPLATE_LANGUAGE),
            'param_names': data.get('param_names', DEFAULT_PARAM_NAMES),
            'webhook_verify_token': data.get('webhook_verify_token', 'token_rahasia_123'),
            'default_cs_number': data.get('default_cs_number', '08123456789')
        }
        with open(CONFIG_FILE, "w") as f:
            json.dump(save_data, f, indent=4)
        logger.info("Konfigurasi tersimpan")
        return True
    except Exception as e:
        logger.error(f"Gagal simpan config: {e}")
        return False

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# =========================
# FLASK ROUTES
# =========================

@app.route("/")
def dashboard():
    return render_template_string(BASE_TEMPLATE, content=DASHBOARD_HTML, active="dashboard")

@app.route("/settings", methods=["GET", "POST"])
def settings():
    config = load_config()
    
    if request.method == "POST":
        try:
            config.update(request.form.to_dict())
            if 'param_names' in request.form and isinstance(request.form.get('param_names'), str):
                config['param_names'] = [p.strip() for p in request.form.get('param_names', '').split(',') if p.strip()]
            
            if save_config(config):
                return redirect("/settings?success=1")
            else:
                return render_template_string(BASE_TEMPLATE, content=SETTINGS_HTML, config=config, active="settings", error="Gagal simpan")
        except Exception as e:
            return render_template_string(BASE_TEMPLATE, content=SETTINGS_HTML, config=config, active="settings", error=str(e))
    
    success = request.args.get('success')
    return render_template_string(BASE_TEMPLATE, content=SETTINGS_HTML, config=config, active="settings", success=success)

@app.route("/template", methods=["GET", "POST"])
def template():
    config = load_config()
    
    meta_templates = []
    if config.get('access_token') and config.get('waba_id'):
        logger.info(f"Mengambil template menggunakan WABA ID: {config['waba_id']}")
        meta_templates = fetch_templates_from_meta(
            config['access_token'], 
            config['waba_id'],
            config.get('graph_version', 'v18.0')
        )
    else:
        if not config.get('waba_id'):
            logger.warning("WABA ID belum diisi, tidak bisa mengambil template dari Meta")
    
    if request.method == "POST":
        try:
            config['template_name'] = request.form.get('template_name', DEFAULT_TEMPLATE_NAME)
            config['template_language'] = request.form.get('template_language', DEFAULT_TEMPLATE_LANGUAGE)
            param_names_str = request.form.get('param_names', '')
            if param_names_str:
                config['param_names'] = [p.strip() for p in param_names_str.split(',') if p.strip()]
            else:
                config['param_names'] = DEFAULT_PARAM_NAMES
            config['default_cs_number'] = request.form.get('default_cs_number', '08123456789')
            
            if save_config(config):
                return redirect("/template?success=1")
            else:
                return render_template_string(BASE_TEMPLATE, content=TEMPLATE_HTML, config=config, active="template", error="Gagal simpan", meta_templates=meta_templates)
        except Exception as e:
            return render_template_string(BASE_TEMPLATE, content=TEMPLATE_HTML, config=config, active="template", error=str(e), meta_templates=meta_templates)
    
    success = request.args.get('success')
    return render_template_string(BASE_TEMPLATE, content=TEMPLATE_HTML, config=config, active="template", success=success, meta_templates=meta_templates)

@app.route("/fetch-template/<template_name>")
def fetch_template_detail(template_name):
    """API untuk mengambil detail template dari Meta menggunakan WABA ID"""
    config = load_config()
    
    if not config.get('access_token'):
        return jsonify({"error": "Token tidak ditemukan. Silakan isi pengaturan API terlebih dahulu."}), 400
    
    if not config.get('waba_id'):
        return jsonify({"error": "WABA ID tidak ditemukan. Silakan isi WABA ID di pengaturan API terlebih dahulu."}), 400
    
    logger.info(f"Mengambil detail template '{template_name}' menggunakan WABA ID: {config['waba_id']}")
    
    template_detail = get_template_structure_from_meta(
        config['access_token'],
        config['waba_id'],
        template_name,
        config.get('template_language', 'id')
    )
    
    if template_detail:
        return jsonify(template_detail)
    else:
        return jsonify({"error": f"Template '{template_name}' tidak ditemukan. Pastikan nama template benar dan sudah disetujui (APPROVED) di Meta Business Platform."}), 404

@app.route("/upload", methods=["POST"])
def upload():
    try:
        if 'file' not in request.files:
            return jsonify({"error": "Tidak ada file"}), 400
        
        file = request.files["file"]
        
        if file.filename == '':
            return jsonify({"error": "Nama file kosong"}), 400
        
        if not allowed_file(file.filename):
            return jsonify({"error": "File harus .xlsx atau .xls"}), 400
        
        filename = secure_filename(file.filename)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_filename = f"{timestamp}_{filename}"
        filepath = os.path.join(UPLOAD_FOLDER, safe_filename)
        file.save(filepath)
        
        thread = threading.Thread(target=process_excel_file, args=(filepath,))
        thread.daemon = True
        thread.start()
        
        return jsonify({"status": "started", "message": "Pengiriman dimulai"})
        
    except Exception as e:
        logger.error(f"Error upload: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/progress")
def progress():
    return jsonify(progress_data)

@app.route("/stop", methods=["POST"])
def stop_sending():
    progress_data["status"] = "stopped"
    return jsonify({"status": "stopping"})

@app.route("/history")
def history():
    try:
        conn = sqlite3.connect(DATABASE_FILE, detect_types=sqlite3.PARSE_DECLTYPES)
        c = conn.cursor()
        c.execute("SELECT * FROM sending_logs ORDER BY start_time DESC LIMIT 50")
        logs = c.fetchall()
        conn.close()
        
        history_html = """
        <div class="card">
            <div class="card-header">
                <h3 class="card-title">Riwayat Pengiriman</h3>
            </div>
            <div class="card-body">
                <div class="table-responsive">
                    <table class="table table-bordered table-hover">
                        <thead>
                            <tr>
                                <th>Batch ID</th>
                                <th>File</th>
                                <th>Total</th>
                                <th>Sukses</th>
                                <th>Gagal</th>
                                <th>Retry</th>
                                <th>Waktu Mulai</th>
                                <th>Waktu Selesai</th>
                            </tr>
                        </thead>
                        <tbody>
        """
        
        for log in logs:
            history_html += f"""
                <tr>
                    <td><code>{log[1]}</code></td>
                    <td>{log[7] if len(log) > 7 else '-'}</td>
                    <td>{log[2]}</td>
                    <td class="text-success">{log[4]}</td>
                    <td class="text-danger">{log[5]}</td>
                    <td class="text-warning">{log[8] if len(log) > 8 else 0}</td>
                    <td>{log[3]}</td>
                    <td>{log[6] if len(log) > 6 and log[6] else '-'}</td>
                </tr>
            """
        
        history_html += """
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
        """
        
        return render_template_string(BASE_TEMPLATE, content=history_html, active="history")
        
    except Exception as e:
        return render_template_string(BASE_TEMPLATE, content=f"<div class='alert alert-danger'>Error: {e}</div>", active="history")

@app.route("/webhook", methods=["GET", "POST"])
def webhook():
    config = load_config()
    verify_token = config.get('webhook_verify_token', 'token_rahasia_123')
    
    if request.method == "GET":
        mode = request.args.get("hub.mode")
        token = request.args.get("hub.verify_token")
        challenge = request.args.get("hub.challenge")
        
        if mode == "subscribe" and token == verify_token:
            logger.info("Webhook verified")
            return challenge, 200
        return "Verification failed", 403
    
    elif request.method == "POST":
        try:
            data = request.get_json()
            logger.info(f"Webhook received: {json.dumps(data, indent=2)[:500]}")
            
            if data and 'entry' in data:
                for entry in data['entry']:
                    for change in entry.get('changes', []):
                        if change.get('field') == 'messages':
                            value = change.get('value', {})
                            contacts = value.get('contacts', [])
                            for contact in contacts:
                                wa_id = contact.get('wa_id')
                                profile_name = contact.get('profile', {}).get('name', 'Unknown')
                                messages = value.get('messages', [])
                                for message in messages:
                                    save_incoming_message(message, wa_id, profile_name)
            
            return "OK", 200
            
        except Exception as e:
            logger.error(f"Webhook error: {e}")
            return "Error", 500

def save_incoming_message(message, wa_id, sender_name):
    try:
        message_id = message.get('id')
        message_type = message.get('type')
        
        message_content = ""
        if message_type == 'text':
            message_content = message.get('text', {}).get('body', '')
        elif message_type == 'image':
            caption = message.get('image', {}).get('caption', '')
            message_content = f"[Gambar] {caption}" if caption else "[Gambar]"
        else:
            message_content = f"[{message_type}]"
        
        conn = sqlite3.connect(DATABASE_FILE, detect_types=sqlite3.PARSE_DECLTYPES)
        c = conn.cursor()
        
        c.execute("SELECT id FROM incoming_messages WHERE message_id = ?", (message_id,))
        if c.fetchone():
            conn.close()
            return
        
        now = datetime.now()
        c.execute("""
            INSERT INTO incoming_messages 
            (wa_id, sender_name, message_id, message_type, message_content, raw_data, received_at) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (wa_id, sender_name, message_id, message_type, message_content, json.dumps(message), now))
        
        conn.commit()
        conn.close()
        logger.info(f"Pesan baru dari {sender_name}: {message_content[:50]}")
        
    except Exception as e:
        logger.error(f"Gagal simpan pesan: {e}")

@app.route("/inbox")
def inbox():
    try:
        conn = sqlite3.connect(DATABASE_FILE, detect_types=sqlite3.PARSE_DECLTYPES)
        c = conn.cursor()
        c.execute("""
            SELECT id, wa_id, sender_name, message_type, message_content, received_at, is_read, replied 
            FROM incoming_messages ORDER BY received_at DESC
        """)
        messages = c.fetchall()
        
        c.execute("UPDATE incoming_messages SET is_read = 1, read_at = ? WHERE is_read = 0", (datetime.now(),))
        conn.commit()
        conn.close()
        
        inbox_html = """
        <div class="card">
            <div class="card-header">
                <h3 class="card-title">Kotak Masuk</h3>
                <div class="card-tools">
                    <span class="badge badge-primary">""" + str(len(messages)) + """ Pesan</span>
                </div>
            </div>
            <div class="card-body p-0">
                <div class="table-responsive">
                    <table class="table table-hover">
                        <thead>
                            <tr>
                                <th>Waktu</th>
                                <th>Pengirim</th>
                                <th>Nomor</th>
                                <th>Pesan</th>
                                <th>Status</th>
                                <th>Aksi</th>
                            </tr>
                        </thead>
                        <tbody>
        """
        
        for msg in messages:
            msg_id, wa_id, sender, msg_type, content, received_at, is_read, replied = msg
            
            try:
                if isinstance(received_at, datetime):
                    waktu = received_at.strftime("%d/%m/%Y %H:%M")
                else:
                    waktu = str(received_at)
            except:
                waktu = str(received_at)
            
            status = "Dibalas" if replied else ("Dibaca" if is_read else "Baru")
            status_class = "success" if replied else ("info" if is_read else "warning")
            
            inbox_html += f"""
                <tr class="table-{status_class}">
                    <td>{waktu}</td>
                    <td><strong>{sender}</strong></td>
                    <td>{wa_id}</td>
                    <td style="max-width: 300px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;">{content}</td>
                    <td>{status}</td>
                    <td>
                        <button class="btn btn-sm btn-info" onclick="viewDetail('{msg_id}')">Detail</button>
                        <button class="btn btn-sm btn-primary" onclick="replyMessage('{wa_id}', '{sender}')">Balas</button>
                      </td>
                  </tr>
            """
        
        inbox_html += """
                        </tbody>
                      </table>
                </div>
            </div>
        </div>
        
        <div class="modal fade" id="detailModal" tabindex="-1">
            <div class="modal-dialog">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title">Detail Pesan</h5>
                        <button type="button" class="close" data-dismiss="modal">&times;</button>
                    </div>
                    <div class="modal-body" id="detailContent"></div>
                </div>
            </div>
        </div>
        
        <div class="modal fade" id="replyModal" tabindex="-1">
            <div class="modal-dialog">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title">Balas Pesan</h5>
                        <button type="button" class="close" data-dismiss="modal">&times;</button>
                    </div>
                    <div class="modal-body">
                        <input type="hidden" id="replyNumber">
                        <div class="form-group">
                            <label>Kepada:</label>
                            <input type="text" id="replyTo" class="form-control" readonly>
                        </div>
                        <div class="form-group">
                            <label>Pesan:</label>
                            <textarea id="replyMessage" class="form-control" rows="5" placeholder="Ketik pesan..."></textarea>
                        </div>
                    </div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-secondary" data-dismiss="modal">Batal</button>
                        <button type="button" class="btn btn-primary" onclick="sendReply()">Kirim</button>
                    </div>
                </div>
            </div>
        </div>
        
        <script>
        function viewDetail(id) {
            $('#detailContent').html('Loading...');
            $('#detailModal').modal('show');
            
            fetch('/api/message/' + id)
                .then(r => r.json())
                .then(data => {
                    let html = '<div class="list-group">';
                    html += '<div class="list-group-item"><strong>Pengirim:</strong> ' + data.sender_name + '</div>';
                    html += '<div class="list-group-item"><strong>Nomor:</strong> ' + data.wa_id + '</div>';
                    html += '<div class="list-group-item"><strong>Waktu:</strong> ' + data.received_at + '</div>';
                    html += '<div class="list-group-item"><strong>Pesan:</strong><br>' + data.message_content + '</div>';
                    if(data.reply_message) {
                        html += '<div class="list-group-item"><strong>Balasan:</strong><br>' + data.reply_message + '</div>';
                    }
                    html += '</div>';
                    $('#detailContent').html(html);
                });
        }
        
        function replyMessage(wa_id, sender) {
            $('#replyNumber').val(wa_id);
            $('#replyTo').val(sender + ' (' + wa_id + ')');
            $('#replyMessage').val('');
            $('#replyModal').modal('show');
        }
        
        function sendReply() {
            let number = $('#replyNumber').val();
            let message = $('#replyMessage').val();
            
            if(!message.trim()) {
                alert('Pesan tidak boleh kosong');
                return;
            }
            
            fetch('/api/reply', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({to: number, text: message})
            })
            .then(r => r.json())
            .then(data => {
                if(data.success) {
                    alert('Balasan terkirim!');
                    location.reload();
                } else {
                    alert('Gagal: ' + data.error);
                }
            });
        }
        </script>
        """
        
        return render_template_string(BASE_TEMPLATE, content=inbox_html, active="inbox")
        
    except Exception as e:
        return render_template_string(BASE_TEMPLATE, content=f"<div class='alert alert-danger'>Error: {e}</div>", active="inbox")

@app.route("/api/message/<int:message_id>")
def api_get_message(message_id):
    try:
        conn = sqlite3.connect(DATABASE_FILE, detect_types=sqlite3.PARSE_DECLTYPES)
        c = conn.cursor()
        c.execute("SELECT * FROM incoming_messages WHERE id = ?", (message_id,))
        msg = c.fetchone()
        conn.close()
        
        if msg:
            return jsonify({
                "id": msg[0],
                "wa_id": msg[1],
                "sender_name": msg[2],
                "message_content": msg[5],
                "received_at": str(msg[9]) if msg[9] else None,
                "reply_message": msg[11]
            })
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/reply", methods=["POST"])
def api_reply():
    try:
        data = request.get_json()
        phone_number = format_phone_number(data.get('to'))
        message = data.get('text')
        
        if not phone_number or not message:
            return jsonify({"success": False, "error": "Nomor dan pesan wajib"}), 400
        
        config = load_config()
        
        url = f"https://graph.facebook.com/{config['graph_version']}/{config['phone_number_id']}/messages"
        headers = {
            "Authorization": f"Bearer {config['access_token']}",
            "Content-Type": "application/json"
        }
        payload = {
            "messaging_product": "whatsapp",
            "to": phone_number,
            "type": "text",
            "text": {"body": message}
        }
        
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        response_data = response.json()
        
        if response.status_code == 200:
            conn = sqlite3.connect(DATABASE_FILE, detect_types=sqlite3.PARSE_DECLTYPES)
            c = conn.cursor()
            c.execute("""
                UPDATE incoming_messages 
                SET replied = 1, replied_at = ?, reply_message = ?
                WHERE wa_id = ? AND replied = 0
            """, (datetime.now(), message, phone_number))
            conn.commit()
            conn.close()
            
            return jsonify({"success": True, "response": response_data})
        else:
            return jsonify({"success": False, "error": response_data.get('error', {}).get('message', 'Unknown error')})
            
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# =========================
# HTML TEMPLATES
# =========================

BASE_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>WhatsApp Cloud API - PT. Dirja Sasak Utama</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/admin-lte@3.2/dist/css/adminlte.min.css">
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
<script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/admin-lte@3.2/dist/js/adminlte.min.js"></script>
</head>
<body class="hold-transition sidebar-mini">
<div class="wrapper">
<nav class="main-header navbar navbar-expand navbar-white navbar-light">
    <ul class="navbar-nav">
        <li class="nav-item">
            <a class="nav-link" data-widget="pushmenu" href="#"><i class="fas fa-bars"></i></a>
        </li>
    </ul>
    <span class="navbar-brand ml-3">WhatsApp Cloud API V3 - Template Kredivo</span>
</nav>

<aside class="main-sidebar sidebar-dark-primary elevation-4">
    <a href="/" class="brand-link">
        <span class="brand-text font-weight-light">PT. Dirja Sasak Utama</span>
    </a>
    <div class="sidebar">
        <nav class="mt-2">
            <ul class="nav nav-pills nav-sidebar flex-column">
                <li class="nav-item">
                    <a href="/" class="nav-link {% if active=='dashboard' %}active{% endif %}">
                        <i class="nav-icon fas fa-home"></i>
                        <p>Dashboard</p>
                    </a>
                </li>
                <li class="nav-item">
                    <a href="/inbox" class="nav-link {% if active=='inbox' %}active{% endif %}">
                        <i class="nav-icon fas fa-inbox"></i>
                        <p>Kotak Masuk</p>
                    </a>
                </li>
                <li class="nav-item">
                    <a href="/settings" class="nav-link {% if active=='settings' %}active{% endif %}">
                        <i class="nav-icon fas fa-cog"></i>
                        <p>Pengaturan API</p>
                    </a>
                </li>
                <li class="nav-item">
                    <a href="/template" class="nav-link {% if active=='template' %}active{% endif %}">
                        <i class="nav-icon fas fa-file-alt"></i>
                        <p>Template Pesan</p>
                    </a>
                </li>
                <li class="nav-item">
                    <a href="/history" class="nav-link {% if active=='history' %}active{% endif %}">
                        <i class="nav-icon fas fa-history"></i>
                        <p>Riwayat</p>
                    </a>
                </li>
            </ul>
        </nav>
    </div>
</aside>

<div class="content-wrapper p-4">
    {% if error %}
    <div class="alert alert-danger">{{ error }}</div>
    {% endif %}
    {% if success %}
    <div class="alert alert-success">Sukses! Data tersimpan.</div>
    {% endif %}
    {{ content|safe }}
</div>

<footer class="main-footer">
    <strong>&copy; 2024 PT. Dirja Sasak Utama</strong>
</footer>
</div>
</body>
</html>
"""

DASHBOARD_HTML = """
<div class="row">
    <div class="col-md-12">
        <div class="alert alert-info">
            <i class="fas fa-info-circle"></i> <strong>Template Kredivo</strong><br>
            Template ini memiliki 4 parameter:
            <ol>
                <li><strong>{{1}}</strong> = Nama Debitur</li>
                <li><strong>{{2}}</strong> = Nomor Tagihan</li>
                <li><strong>{{3}}</strong> = Total Tagihan (format Rupiah)</li>
                <li><strong>{{4}}</strong> = Nomor Telepon CS</li>
            </ol>
        </div>
    </div>
</div>

<div class="row">
    <div class="col-lg-3 col-6">
        <div class="small-box bg-info">
            <div class="inner"><h3 id="totalBox">0</h3><p>Total Data</p></div>
            <div class="icon"><i class="fas fa-database"></i></div>
        </div>
    </div>
    <div class="col-lg-3 col-6">
        <div class="small-box bg-success">
            <div class="inner"><h3 id="successBox">0</h3><p>Berhasil</p></div>
            <div class="icon"><i class="fas fa-check-circle"></i></div>
        </div>
    </div>
    <div class="col-lg-3 col-6">
        <div class="small-box bg-danger">
            <div class="inner"><h3 id="failedBox">0</h3><p>Gagal</p></div>
            <div class="icon"><i class="fas fa-times-circle"></i></div>
        </div>
    </div>
    <div class="col-lg-3 col-6">
        <div class="small-box bg-warning">
            <div class="inner"><h3 id="retryBox">0</h3><p>Retry</p></div>
            <div class="icon"><i class="fas fa-sync-alt"></i></div>
        </div>
    </div>
</div>

<div class="card">
    <div class="card-header">
        <h3 class="card-title"><i class="fas fa-upload"></i> Upload Data Debitur Kredivo</h3>
    </div>
    <div class="card-body">
        <form id="uploadForm" enctype="multipart/form-data">
            <div class="form-group">
                <label>File Excel (.xlsx atau .xls)</label>
                <input type="file" name="file" class="form-control" accept=".xlsx,.xls" required>
                <small class="text-muted">
                    Kolom yang diperlukan:
                    <ul>
                        <li><strong>nomor</strong> - Nomor WhatsApp tujuan (format: 628xxx)</li>
                        <li><strong>nama</strong> - Nama debitur</li>
                        <li><strong>total</strong> - Total tagihan (otomatis diformat ke Rupiah)</li>
                    </ul>
                    Kolom opsional: nomor_tagihan, cs_number<br>
                    Contoh file: <a href="/example-format" class="btn btn-link btn-sm">Download template Excel</a>
                </small>
            </div>
            <button type="submit" class="btn btn-primary"><i class="fas fa-paper-plane"></i> Mulai Kirim</button>
            <button type="button" id="stopBtn" class="btn btn-danger" style="display:none;"><i class="fas fa-stop"></i> Hentikan</button>
        </form>
        
        <div id="progressSection" style="display:none; margin-top:20px;">
            <h5>Progress Pengiriman</h5>
            <div class="progress">
                <div id="progressBar" class="progress-bar bg-success" style="width:0%">0%</div>
            </div>
            <div id="statusText" class="mt-2"></div>
            <div id="errorList" class="mt-2" style="max-height:200px; overflow-y:auto;"></div>
        </div>
    </div>
</div>

<script>
let progressInterval;

$("#uploadForm").submit(function(e){
    e.preventDefault();
    let formData = new FormData(this);
    $("#progressSection").show();
    $("#stopBtn").show();
    
    fetch("/upload", {method:"POST", body:formData})
        .then(r=>r.json())
        .then(data=>{if(data.error) alert(data.error); else startProgress();});
});

$("#stopBtn").click(()=>{if(confirm("Hentikan pengiriman?")) fetch("/stop", {method:"POST"});});

function startProgress(){
    if(progressInterval) clearInterval(progressInterval);
    progressInterval = setInterval(()=>{
        fetch("/progress").then(r=>r.json()).then(data=>{
            let percent = data.total ? Math.floor((data.current/data.total)*100) : 0;
            $("#progressBar").css("width", percent+"%").text(percent+"%");
            $("#statusText").html(`${data.current}/${data.total} pesan<br>${data.success} Sukses | ${data.failed} Gagal | ${data.retried} Retry<br>${data.message || ''}`);
            $("#totalBox").text(data.total);
            $("#successBox").text(data.success);
            $("#failedBox").text(data.failed);
            $("#retryBox").text(data.retried);
            
            if(data.errors && data.errors.length){
                $("#errorList").html('<div class="alert alert-warning small">'+data.errors.slice(-3).join('<br>')+'</div>');
            }
            
            if(data.status === "done" || data.status === "error"){
                clearInterval(progressInterval);
                $("#stopBtn").hide();
                if(data.status === "done") {
                    alert("Pengiriman selesai! Sukses: " + data.success + ", Gagal: " + data.failed);
                }
            }
        });
    }, 1000);
}
</script>
"""

SETTINGS_HTML = """
<div class="card">
    <div class="card-header"><h3>Pengaturan API WhatsApp</h3></div>
    <div class="card-body">
        <form method="POST">
            <div class="form-group">
                <label>Access Token</label>
                <input type="password" name="access_token" class="form-control" value="{{config.get('access_token','')}}">
                <small>Dapatkan dari Meta Developer Console</small>
            </div>
            <div class="form-group">
                <label>Phone Number ID (UNTUK KIRIM PESAN)</label>
                <input type="text" name="phone_number_id" class="form-control" value="{{config.get('phone_number_id','')}}">
                <small class="text-primary"><i class="fas fa-info-circle"></i> ID nomor WhatsApp Business - digunakan untuk endpoint /messages</small>
            </div>
            <div class="form-group">
                <label>WABA ID (UNTUK AMBIL TEMPLATE)</label>
                <input type="text" name="waba_id" class="form-control" value="{{config.get('waba_id','')}}">
                <small class="text-primary"><i class="fas fa-info-circle"></i> WhatsApp Business Account ID - digunakan untuk endpoint /message_templates</small>
            </div>
            <div class="form-group">
                <label>Graph Version</label>
                <input type="text" name="graph_version" class="form-control" value="{{config.get('graph_version','v18.0')}}">
            </div>
            <div class="form-group">
                <label>Default CS Number (Nomor CS)</label>
                <input type="text" name="default_cs_number" class="form-control" value="{{config.get('default_cs_number','08123456789')}}">
                <small>Nomor telepon customer service (akan digunakan jika tidak ada di Excel)</small>
            </div>
            <div class="form-group">
                <label>Webhook Verify Token</label>
                <input type="text" name="webhook_verify_token" class="form-control" value="{{config.get('webhook_verify_token','token_rahasia_123')}}">
            </div>
            <button type="submit" class="btn btn-success">Simpan</button>
        </form>
        <div class="alert alert-info mt-3">
            <strong>PENTING:</strong><br>
            <strong>Phone Number ID</strong> = untuk KIRIM pesan (endpoint: /{phone_number_id}/messages)<br>
            <strong>WABA ID</strong> = untuk AMBIL template (endpoint: /{waba_id}/message_templates)<br>
            <hr>
            <strong>Webhook URL:</strong> <code>{{ request.host_url }}webhook</code><br>
            <strong>Verify Token:</strong> <code>{{ config.get('webhook_verify_token', 'token_rahasia_123') }}</code>
        </div>
    </div>
</div>
"""

TEMPLATE_HTML = """
<div class="card">
    <div class="card-header"><h3>Template WhatsApp - Kredivo</h3></div>
    <div class="card-body">
        <form method="POST" id="templateForm">
            <div class="form-group">
                <label>Nama Template (dari Meta)</label>
                <input type="text" name="template_name" id="templateName" class="form-control" value="{{config.get('template_name','kredivo')}}">
                <small>Nama template yang sudah dibuat di Meta (biasanya: kredivo)</small>
            </div>
            <div class="form-group">
                <label>Bahasa</label>
                <input type="text" name="template_language" class="form-control" value="{{config.get('template_language','id')}}">
                <small>id = Indonesia</small>
            </div>
            <div class="form-group">
                <label>Parameter Template (pisahkan dengan koma)</label>
                <input type="text" name="param_names" class="form-control" value="{{','.join(config.get('param_names', ['nama','nomor_tagihan','total','cs_number']))}}">
                <small>Urutan parameter sesuai template Meta ({{1}} = nama, {{2}} = nomor, {{3}} = total, {{4}} = cs_number)</small>
            </div>
            <div class="form-group">
                <label>Default CS Number</label>
                <input type="text" name="default_cs_number" class="form-control" value="{{config.get('default_cs_number','08123456789')}}">
                <small>Nomor CS default jika tidak ada di Excel</small>
            </div>
            <button type="submit" class="btn btn-primary">Simpan Template</button>
            <button type="button" class="btn btn-info ml-2" onclick="fetchTemplateFromMeta()">Ambil dari Meta</button>
        </form>
        
        <div id="templateInfo" style="display:none;" class="mt-3">
            <div class="card card-info">
                <div class="card-header"><h5>Detail Template dari Meta</h5></div>
                <div class="card-body" id="templateDetail"></div>
            </div>
        </div>
        
        <div class="alert alert-success mt-3">
            <strong>Preview Template Kredivo:</strong>
            <pre class="bg-light p-2 mt-2">
Halo [NAMA],

Kami dari PT. DIRJA SASAK UTAMA Agent yang bekerja sama dengan Kredivo.

Kami menginformasikan bahwa terdapat kewajiban pembayaran yang masih tertunda.

Nomor: [NOMOR TAGIHAN]
Total: Rp [TOTAL]

Untuk informasi atau penyelesaian lebih lanjut dan metode pembayaran, 
Anda dapat menghubungi tim kami di [NOMOR CS].

Terima kasih.
            </pre>
        </div>
    </div>
</div>

<script>
function fetchTemplateFromMeta() {
    let templateName = document.getElementById('templateName').value;
    
    if(!templateName) {
        alert("Masukkan nama template terlebih dahulu");
        return;
    }
    
    document.getElementById('templateInfo').style.display = 'block';
    document.getElementById('templateDetail').innerHTML = '<div class="text-center"><i class="fas fa-spinner fa-spin"></i> Mengambil data dari Meta...</div>';
    
    fetch('/fetch-template/' + encodeURIComponent(templateName))
        .then(response => response.json())
        .then(data => {
            if(data.error) {
                document.getElementById('templateDetail').innerHTML = '<div class="alert alert-danger">' + data.error + '</div>';
            } else {
                let html = '';
                html += '<p><strong>Nama Template:</strong> ' + (data.name || templateName) + '</p>';
                
                let statusClass = 'secondary';
                if(data.status === 'APPROVED') statusClass = 'success';
                else if(data.status === 'PENDING') statusClass = 'warning';
                else if(data.status === 'REJECTED') statusClass = 'danger';
                
                html += '<p><strong>Status:</strong> <span class="badge badge-' + statusClass + '">' + (data.status || 'Unknown') + '</span></p>';
                html += '<p><strong>Jumlah parameter:</strong> <strong class="text-primary">' + data.param_count + '</strong></p>';
                html += '<p><strong>Bahasa:</strong> ' + (data.language || 'id') + '</p>';
                html += '<p><strong>Isi template:</strong></p>';
                html += '<pre class="bg-light p-2 border rounded">' + (data.template_text || 'Tidak ada text') + '</pre>';
                
                document.getElementById('templateDetail').innerHTML = html;
            }
        })
        .catch(error => {
            console.error('Error:', error);
            document.getElementById('templateDetail').innerHTML = '<div class="alert alert-danger"> Gagal mengambil template: ' + error.message + '</div>';
        });
}

// Auto fetch when page loads
$(document).ready(function() {
    let templateName = document.getElementById('templateName').value;
    if(templateName && templateName !== '') {
        fetchTemplateFromMeta();
    }
});
</script>
"""

# =========================
# MAIN
# =========================

def signal_handler(sig, frame):
    logger.info("Shutting down...")
    progress_data["status"] = "stopped"
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

if __name__ == "__main__":
    print("="*60)
    print("WHATSAPP CLOUD API V3 - TEMPLATE KREDIVO")
    print("PT. DIRJA SASAK UTAMA")
    print("="*60)
    print("Template: kredivo (4 parameter)")
    print("  {{1}} = Nama Debitur")
    print("  {{2}} = Nomor Tagihan")
    print("  {{3}} = Total Tagihan (Rp)")
    print("  {{4}} = Nomor Telepon CS")
    print("="*60)
    print("Akses: http://localhost:5000")
    print(f"Webhook: http://localhost:5000/webhook")
    print(f"Database: {DATABASE_FILE}")
    print(f"Log: {LOG_FILE}")
    print("="*60)
    
    app.run(debug=True, host='0.0.0.0', port=5000)