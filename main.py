import pymysql
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import hashlib
import random
import base64
import urllib
import hmac
import json
import requests
import lzstring
import pandas as pd
from io import StringIO
from Crypto.Cipher import AES
import time
from pydantic import BaseModel
from fastapi import FastAPI, Request, HTTPException , Depends , Response, status, Query , Header
from fastapi.security import HTTPBearer , HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, Any, Optional
import logging
import sqlite3
import os
import pytz
from fastapi.responses import FileResponse

load_dotenv()
api_url_antrol = os.getenv("URL")
consid_antrol = os.getenv("CID")
secret_antrol = os.getenv("SCTKEY")
userkey_antrol = os.getenv("USERKEY")
token_antrol = os.getenv("TOKEN")

# Database configuration
DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT"))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db_connection():
    """Create and return a MySQL database connection"""
    try:
        connection = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        return connection
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

def insert_into_table(table_name: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Insert data into a specified table
    
    Args:
        table_name (str): Name of the table to insert data into
        data (Dict[str, Any]): Dictionary containing column names and values to insert
    
    Returns:
        Dict[str, Any]: Result of the insert operation
    """
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        
        # Build the INSERT query dynamically
        columns = list(data.keys())
        values = list(data.values())
        placeholders = ', '.join(['%s'] * len(columns))
        column_names = ', '.join(columns)
        
        query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
        
        logger.info(f"Executing query: {query}")
        logger.info(f"Values: {values}")
        
        cursor.execute(query, values)
        connection.commit()
        
        # Get the inserted ID
        inserted_id = cursor.lastrowid
        
        result = {
            "success": True,
            "message": f"Data inserted successfully into {table_name}",
            "inserted_id": inserted_id,
            "affected_rows": cursor.rowcount
        }
        
        logger.info(f"Insert successful: {result}")
        return result
        
    except Exception as e:
        if connection:
            connection.rollback()
        logger.error(f"Database insert error: {e}")
        raise HTTPException(status_code=500, detail=f"Database insert failed: {str(e)}")
    
    finally:
        if connection:
            connection.close()

def select_from_table(table_name: str, columns: list = None, where_conditions: Dict[str, Any] = None, 
                     order_by: str = None, limit: int = None, fetch_all: bool = True) -> Dict[str, Any]:
    """
    Select data from a specified table with dynamic conditions
    
    Args:
        table_name (str): Name of the table to select from
        columns (list): List of column names to select (None for all columns)
        where_conditions (Dict[str, Any]): Dictionary of WHERE conditions {column: value}
        order_by (str): ORDER BY clause (e.g., "id DESC", "name ASC")
        limit (int): LIMIT clause for number of rows to return
        fetch_all (bool): If True, fetch all rows; if False, fetch only first row
    
    Returns:
        Dict[str, Any]: Result containing data and metadata
    """
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        
        # Build SELECT clause
        if columns is None or len(columns) == 0:
            select_clause = "*"
        else:
            select_clause = ', '.join(columns)
        
        # Build WHERE clause
        where_clause = ""
        where_values = []
        if where_conditions:
            where_parts = []
            for column, value in where_conditions.items():
                if isinstance(value, dict):
                    # Support operators: {"op": "!=", "val": "something"}
                    # Example usage:
                    # where_conditions = {
                    #     "status": {"op": "!=", "val": "deleted"},
                    #     "age": {"op": ">", "val": 18}
                    # }
                    op = value.get("op", "=")
                    val = value.get("val")
                    if val is None:
                        where_parts.append(f"{column} IS NULL")
                    else:
                        where_parts.append(f"{column} {op} %s")
                        where_values.append(val)
                else:
                    if value is None:
                        where_parts.append(f"{column} IS NULL")
                    else:
                        where_parts.append(f"{column} = %s")
                        where_values.append(value)
            where_clause = "WHERE " + " AND ".join(where_parts)
        
        # Build ORDER BY clause
        order_clause = ""
        if order_by:
            order_clause = f"ORDER BY {order_by}"
        
        # Build LIMIT clause
        limit_clause = ""
        if limit:
            limit_clause = f"LIMIT {limit}"
        
        # Construct final query
        query = f"SELECT {select_clause} FROM {table_name} {where_clause} {order_clause} {limit_clause}".strip()
        
        logger.info(f"Executing query: {query}")
        logger.info(f"Where values: {where_values}")
        
        cursor.execute(query, where_values)
        
        # Fetch results
        if fetch_all:
            results = cursor.fetchall()
        else:
            results = cursor.fetchone()
        
        result = {
            "success": True,
            "message": f"Data selected successfully from {table_name}",
            "data": results,
            "row_count": len(results) if fetch_all and results else (1 if results else 0),
            "query": query
        }
        
        logger.info(f"Select successful: {len(results) if fetch_all and results else (1 if results else 0)} rows returned")
        return result
        
    except Exception as e:
        logger.error(f"Database select error: {e}")
        return {
            "success": False,
            "message": f"Database select error: {e}",
            "data": None,
            "row_count": 0,
            "query": query if 'query' in locals() else None
        }
    
    finally:
        if connection:
            connection.close()

def insert_checkin_log(kodebooking: str, status: str, response_data: Optional[Dict] = None):
    """
    Insert check-in log into the database
    
    Args:
        kodebooking (str): The booking code
        status (str): Status of the check-in operation
        response_data (Optional[Dict]): Response data from the API
    """
    try:
        log_data = {
            "kodebooking": kodebooking,
            "status": status,
            "response_data": json.dumps(response_data) if response_data else None,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # return insert_into_table("checkin_logs", log_data)
        return log_data
        
    except Exception as e:
        logger.error(f"Error inserting check-in log: {e}")
        return {"success": False, "message": f"Failed to log check-in: {str(e)}"}


class Checkin(BaseModel):
    kodebooking: str

class CheckinTaskid(BaseModel):
    kodebooking: str
    taskid: int
    waktu: Optional[str] = None

def get_sqlite_connection(db_path: str = None):
    """
    Create and return a SQLite database connection.
    If the database file does not exist, it will be created.
    """
    if db_path is None:
        db_path = os.getenv("DB_PATH", "data_antrol.db")
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row  # Enable column access by name
        return conn
    except Exception as e:
        logger.error(f"SQLite connection error: {e}")
        raise HTTPException(status_code=500, detail=f"SQLite connection failed: {str(e)}")

def insert_into_sqlite_table(table_name: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Insert data into a specified SQLite table.
    
    Args:
        table_name (str): Name of the table to insert data into
        data (Dict[str, Any]): Dictionary containing column names and values to insert
    
    Returns:
        Dict[str, Any]: Result of the insert operation
    """
    conn = None
    try:
        conn = get_sqlite_connection()
        cursor = conn.cursor()
        
        # Build the INSERT query dynamically
        columns = list(data.keys())
        values = list(data.values())
        placeholders = ', '.join(['?' for _ in columns])  # SQLite uses ? as placeholder
        column_names = ', '.join(columns)
        
        query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
        
        logger.info(f"Executing SQLite query: {query}")
        logger.info(f"Values: {values}")
        
        cursor.execute(query, values)
        conn.commit()
        
        # Get the inserted row ID
        inserted_id = cursor.lastrowid
        
        result = {
            "success": True,
            "message": f"Data inserted successfully into {table_name}",
            "inserted_id": inserted_id,
            "affected_rows": cursor.rowcount
        }
        
        logger.info(f"SQLite insert successful: {result}")
        return result
        
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"SQLite insert error: {e}")
        raise HTTPException(status_code=500, detail=f"SQLite insert failed: {str(e)}")
    
    finally:
        if conn:
            conn.close()

def read_from_sqlite_table(table_name: str, columns: Optional[list] = None, where: Optional[str] = None, params: Optional[list] = None, limit: Optional[int] = None, offset: Optional[int] = None) -> list:
    """
    Read data from a specified SQLite table.

    Args:
        table_name (str): Name of the table to read from
        columns (Optional[list]): List of columns to select; if None, selects all columns
        where (Optional[str]): WHERE clause without the word 'WHERE'
        params (Optional[list]): Parameters for the WHERE clause
        limit (Optional[int]): Maximum number of rows to return
        offset (Optional[int]): Number of rows to skip

    Returns:
        list: List of dictionaries representing rows
    """
    conn = None
    try:
        conn = get_sqlite_connection()
        cursor = conn.cursor()

        # Build SELECT clause
        select_clause = ', '.join(columns) if columns else '*'

        # Build query
        query = f"SELECT {select_clause} FROM {table_name}"
        if where:
            query += f" WHERE {where}"
        if limit is not None:
            query += f" LIMIT {limit}"
        if offset is not None:
            if limit is not None:
                offset = offset * limit
            query += f" OFFSET {offset}"

        logger.info(f"Executing SQLite query: {query}")
        logger.info(f"Parameters: {params}")

        cursor.execute(query, params or [])
        rows = cursor.fetchall()

        # Convert rows to list of dicts
        result = [dict(row) for row in rows]

        logger.info(f"SQLite read successful: {len(result)} rows fetched")
        return result

    except Exception as e:
        logger.error(f"SQLite read error: {e}")
        raise HTTPException(status_code=500, detail=f"SQLite read failed: {str(e)}")

    finally:
        if conn:
            conn.close()

def empty_sqlite_db():
    conn = None
    try:
        conn = get_sqlite_connection()
        cursor = conn.cursor()
        
        # Example: create checkin_logs table
        cursor.execute("""
            delete from data_antrol
        """)
        
        # Example: create mlite_antrian_referensi_taskid table
        cursor.execute("""
            delete from data_taskid
        """)
        
        conn.commit()
        logger.info("SQLite tables initialized successfully")
        
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"SQLite initialization error: {e}")
        raise HTTPException(status_code=500, detail=f"SQLite initialization failed: {str(e)}")
    
    finally:
        if conn:
            conn.close()

def init_sqlite_db():
    """
    Initialize SQLite database with required tables if they don't exist.
    """
    conn = None
    try:
        conn = get_sqlite_connection()
        cursor = conn.cursor()
        
        # Example: create checkin_logs table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS data_antrol (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nomor_referensi TEXT,
                kode_booking TEXT,
                no_rekam_medis TEXT,
                nik TEXT,
                no_kapst TEXT,
                sumber_data TEXT,
                status TEXT
            )
        """)
        
        # Example: create mlite_antrian_referensi_taskid table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS data_taskid (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                wakturs TEXT,
                waktu TEXT,
                taskname TEXT,
                taskid INTEGER,
                kodebooking TEXT
            )
        """)
        
        conn.commit()
        logger.info("SQLite tables initialized successfully")
        
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"SQLite initialization error: {e}")
        raise HTTPException(status_code=500, detail=f"SQLite initialization failed: {str(e)}")
    
    finally:
        if conn:
            conn.close()


def create_signature(times):
    signature = ''
    conside = f"{consid_antrol}&{times}"
    data = conside.encode('utf-8')
    secretkey = secret_antrol.encode('utf-8')
    signature = hmac.new(secretkey, msg=data, digestmod=hashlib.sha256).digest()
    encodedSignature = base64.b64encode(signature).decode('utf-8')
    signature = encodedSignature
    return signature
    # print(signature)

def decrypt(key, txt_enc):
    
    x = lzstring.LZString()

    key_hash = hashlib.sha256(key.encode('utf-8')).digest()

    mode = AES.MODE_CBC

    # decrypt
    decryptor = AES.new(key_hash[0:32], mode, IV=key_hash[0:16])
    plain = decryptor.decrypt(base64.b64decode(txt_enc))
    decompress = x.decompressFromEncodedURIComponent(plain.decode('utf-8'))

    return decompress

def get_antrol(url,method,times,payload=None):
    signature = create_signature(times)
    headers = {
        'X-cons-id': consid_antrol, 
        'X-timestamp': times, 
        'X-signature': signature, 
        'user_key': userkey_antrol, 
        'Content-Type': 'Application/x-www-form-urlencoded',
        'Accept': '*/*'
    }
    payload = json.dumps(payload)
    url = api_url_antrol+url
    if method == "GET":
        res = requests.get(url, headers=headers)
    if method == "POST":
        res = requests.post(url, data=payload, headers=headers)
    if method == "PUT":
        res = requests.put(url, data=payload, headers=headers)
    res = res.json()
    return res

def check_kodebooking(kodebooking):
    timestamp = str(int(datetime.today().timestamp()))
    keys = consid_antrol+secret_antrol+timestamp
    url = f"antrean/getlisttask"
    payload = {
        "kodebooking": kodebooking
    }
    print(f"Payload : {payload}")
    data = get_antrol(url,'POST',timestamp, payload)
    if data['metadata']['code'] == 200:
        dec = decrypt(keys,data['response'])
        return dec
    else:
        print(f"Error: {data['metadata']['message']}")
        return

def checkin_kodebooking(kodebooking):
    timestamp = str(int(datetime.today().timestamp()))
    keys = consid_antrol+secret_antrol+timestamp
    url = f"antrean/updatewaktu"
    current_time = int(time.time() * 1000)  # Get current time in milliseconds
    payload = {
        "kodebooking": kodebooking,
        "taskid": 3,
        "waktu": current_time
    }
    print(f"Payload : {payload}")
    data = get_antrol(url,'POST',timestamp, payload)
    if data['metadata']['code'] == 200:
        insert_into_table("mlite_antrian_referensi_taskid",{
            "tanggal_periksa":datetime.now().strftime("%Y-%m-%d"),
            "nomor_referensi":kodebooking,
            "taskid":3,
            "waktu":current_time,
            "status":"Sudah",
            "keterangan":data['metadata']['code']
        })
        return data
    else:
        print(f"Error: {data['metadata']['message']}")
        return data

def task_kodebooking(kodebooking,taskid,current_time: Optional[str] = None):
    jakarta = pytz.timezone('Asia/Jakarta')
    if current_time is None:
        now_dt = datetime.now(jakarta)
        data_date = now_dt.strftime("%Y-%m-%d")
        current_time = int(now_dt.timestamp() * 1000)
    else:
        data_date = current_time.split(" ")[0]
        dt = datetime.strptime(current_time, "%Y-%m-%d %H:%M:%S")
        dt = jakarta.localize(dt)
        current_time = int(dt.timestamp() * 1000)
    timestamp = str(int(datetime.today().timestamp()))
    keys = consid_antrol+secret_antrol+timestamp
    url = "antrean/updatewaktu"
    payload = {
        "kodebooking": kodebooking,
        "taskid": taskid,
        "waktu": current_time
    }
    print(f"Payload : {payload}")
    data = get_antrol(url,'POST',timestamp, payload)
    if data['metadata']['code'] == 200:
        insert_into_table("mlite_antrian_referensi_taskid",{
            "tanggal_periksa":data_date,
            "nomor_referensi":kodebooking,
            "taskid":taskid,
            "waktu":current_time,
            "status":"Sudah",
            "keterangan":data['metadata']['code']
        })
        return data
    else:
        print(f"Error: {data['metadata']['message']}")
        return data

def task_batal(kodebooking):
    jakarta = pytz.timezone('Asia/Jakarta')
    current_time = int(datetime.now(jakarta).timestamp() * 1000)
    data_date = datetime.fromtimestamp(current_time / 1000, tz=jakarta).strftime("%Y-%m-%d")
    timestamp = str(int(datetime.today().timestamp()))
    keys = consid_antrol+secret_antrol+timestamp
    url = f"antrean/batal"
    payload = {
        "kodebooking": kodebooking,
        "keterangan": f"Batal {kodebooking}"
    }
    print(f"Payload : {payload}")
    data = get_antrol(url,'POST',timestamp, payload)
    print(f"Data : {data}")
    if data['metadata']['code'] == 200:
        print(f"Pemeriksaan dibatalkan {kodebooking}")
        delete_payload = {
            "tanggal_batal":data_date,
            "nomor_referensi":kodebooking,
            "keterangan": f"Batal {kodebooking} via BOT"
        }
        print(f"Delete Payload : {delete_payload}")
        insert_into_table("mlite_antrian_referensi_batal",delete_payload)
        return data
    else:
        print(f"Error: {data['metadata']['message']}")
        return data

def get_list_antri_tampil(tanggal):
    timestamp = str(int(datetime.today().timestamp()))
    keys = consid_antrol+secret_antrol+timestamp
    url = f"antrean/pendaftaran/tanggal/{tanggal}"
    data = get_antrol(url,'GET',timestamp)
    dec = decrypt(keys,data['response'])
    df = pd.read_json(StringIO(dec))
    response = []
    for index, row in df.iterrows():
        no_ref = row['nomorreferensi']
        sumber = row['sumberdata']
        if no_ref and sumber == 'Mobile JKN':
            response.append({
                "nomor_referensi": no_ref,
                "kode_booking": row['kodebooking'],
                "no_rekam_medis": row['norekammedis'],
                "nik": row['nik'],
                "no_kapst": row['nokapst'],
                "sumber_data": sumber,
                "status": row['status']
            })
    return response

def get_list_antri(tanggal):
    empty_sqlite_db()
    timestamp = str(int(datetime.today().timestamp()))
    keys = consid_antrol+secret_antrol+timestamp
    url = f"antrean/pendaftaran/tanggal/{tanggal}"
    data = get_antrol(url,'GET',timestamp)
    dec = decrypt(keys,data['response'])
    df = pd.read_json(StringIO(dec))
    response = []
    for index, row in df.iterrows():
        no_ref = row['nomorreferensi']
        sumber = row['sumberdata']
        if no_ref:
            insert_into_sqlite_table("data_antrol",{
                "nomor_referensi": no_ref,
                "kode_booking": row['kodebooking'],
                "no_rekam_medis": str(row['norekammedis']).zfill(6),
                "nik": row['nik'],
                "no_kapst": str(row['nokapst']).zfill(13),
                "sumber_data": sumber,
                "status": row['status']
            })
    # Check if response is not empty
    if not response:
        response.append({
            "message": "Data Inserted"
        })
        return response
    
    return response

def loadtask(keys='',timestamp='',kodebooking=''):
    url = f"antrean/getlisttask"
    payload = {
        "kodebooking":kodebooking
    }
    print(f"Payload : {payload}")
    data = get_antrol(url,'POST',timestamp, payload)
    print(data)
    if keys == '':
        return data
    if data['metadata']['code'] != 200:
        response = []
        get_data_antrian = select_from_table(table_name="mlite_antrian_referensi",columns=["no_rkm_medis","tanggal_periksa"],where_conditions={"kodebooking":kodebooking}, limit=1,fetch_all=False)
        if get_data_antrian['row_count'] > 0:
            data_antrian = get_data_antrian['data']
            print("Load All Needed Data")
            get_no_rawat = select_from_table(table_name="reg_periksa",columns=["no_rawat","stts"],where_conditions={"no_rkm_medis":data_antrian['no_rkm_medis'],"tgl_registrasi":data_antrian['tanggal_periksa'],"kd_poli": {"op": "!=", "val": "IGD01"}}, limit=1,fetch_all=False)
            if get_no_rawat['row_count'] > 0:
                get_sep = select_from_table(table_name="bridging_sep",columns=["no_rawat","tglsep"],where_conditions={"no_rawat":get_no_rawat['data']['no_rawat']}, limit=1,fetch_all=False)
                if get_sep['row_count'] > 0:
                    data_sep = get_sep['data']
                else:
                    data_sep = {
                        "no_rawat": '',
                        "tglsep": ''
                    }
                data_no_rawat = get_no_rawat['data']
                print("Load Periksa")
            else:
                data_no_rawat = {
                    "no_rawat": '',
                    "stts": ''
                }
                print("No Rawat Not Found")
            get_waktu = select_from_table(table_name="mutasi_berkas",columns=["dikirim","diterima"],where_conditions={"no_rawat":data_no_rawat['no_rawat']}, limit=1,fetch_all=False)
            if get_waktu['row_count'] > 0 and get_waktu['data']['dikirim'] != '0000-00-00 00:00:00':
                waktu_dikirim = get_waktu['data']['dikirim'].strftime("%Y-%m-%d %H:%M:%S")
            else:
                waktu_dikirim = 'Kosong'
            print(f"Load Waktu Dikirim {waktu_dikirim}")
            if get_waktu['row_count'] > 0 and get_waktu['data']['diterima'] != '0000-00-00 00:00:00':
                waktu_diterima = get_waktu['data']['diterima'].strftime("%Y-%m-%d %H:%M:%S")
            else:
                waktu_diterima = '0000-00-00 00:00:00'
            print(f"Load Waktu Diterima {waktu_diterima}")
            get_pemeriksaan = select_from_table(table_name="pemeriksaan_ralan",columns=["tgl_perawatan","jam_rawat"],where_conditions={"no_rawat":data_no_rawat['no_rawat']}, limit=1,fetch_all=False)
            if get_pemeriksaan['row_count'] > 0:
                waktu_perawatan = f"{get_pemeriksaan['data']['tgl_perawatan']} {get_pemeriksaan['data']['jam_rawat']}"
            else:
                waktu_perawatan = '0000-00-00 00:00:00'
            print(f"Load Waktu Perawatan {waktu_perawatan}")
        else:
            print("Data Tidak Ada")
            data_no_rawat = {
                "no_rawat": '',
                "stts": 'Tidak ada data di antrian MJKN'
            }
            data_sep = {
                "no_rawat": '',
                "tglsep": ''
            }
            waktu_perawatan = 'Kosong'
            waktu_dikirim = 'Kosong'
            waktu_diterima = 'Kosong'
        print("Response Sebelum Append")
        response.append({
            "no_rawat": data_no_rawat['no_rawat'],
            "stts": data_no_rawat['stts'],
            "waktu_perawatan": waktu_perawatan,
            "waktu_dikirim": waktu_dikirim,
            "waktu_diterima": waktu_diterima,
            "data_sep": data_sep['no_rawat']
        })
        print(f"Error: {data['metadata']['message']}")
        return {"message": "Error", "detail": data['metadata']['message'] , "response": response}
    dec = decrypt(keys,data['response'])
    df = pd.read_json(StringIO(dec))
    response = []
    task = []
    for index, row in df.iterrows():
        print(f"Row {index}: {row}")
        task.append({
            "taskid": row['taskid'],
            "waktu": row['waktu'],
            "wakturs": row['wakturs'],
            "taskname": row['taskname'],
            "kodebooking": row['kodebooking']   
        })
    get_data_antrian = select_from_table(table_name="mlite_antrian_referensi",columns=["no_rkm_medis","tanggal_periksa"],where_conditions={"kodebooking":kodebooking}, limit=1,fetch_all=False)
    print(get_data_antrian)
    if get_data_antrian['row_count'] > 0:
        print("Load All Needed Data If Data Found And Get Code 200")
        data_no_rawat = []
        data_sep = []
        waktu_perawatan = ''
        waktu_dikirim = ''
        waktu_diterima = ''
        get_no_rawat = select_from_table(table_name="reg_periksa",columns=["no_rawat","stts"],where_conditions={"no_rkm_medis":get_data_antrian['data']['no_rkm_medis'],"tgl_registrasi":get_data_antrian['data']['tanggal_periksa'],"kd_poli": {"op": "!=", "val": "IGD01"}}, limit=1,fetch_all=False)
        print(get_no_rawat)
        if get_no_rawat['row_count'] > 0 and get_no_rawat['data']['stts'] != 'Batal':
            data_no_rawat = {
                "no_rawat": get_no_rawat['data']['no_rawat'],
                "stts": get_no_rawat['data']['stts']
            }
            get_sep = select_from_table(table_name="bridging_sep",columns=["no_rawat","tglsep"],where_conditions={"no_rawat":get_no_rawat['data']['no_rawat']}, limit=1,fetch_all=False)
            print(get_sep)
            if get_sep['row_count'] > 0:
                data_sep = {
                    "no_rawat": get_sep['data']['no_rawat'],
                    "tglsep": get_sep['data']['tglsep']
                }
                print(f"Load SEP {get_sep['data']['no_rawat']}")
            print(f"Load Periksa {get_no_rawat['data']['no_rawat']}")
            get_waktu = select_from_table(table_name="mutasi_berkas",columns=["dikirim","diterima"],where_conditions={"no_rawat":get_no_rawat['data']['no_rawat']}, limit=1,fetch_all=False)
            print(get_waktu)
            if get_waktu['row_count'] > 0 and get_waktu['data']['dikirim'] != '0000-00-00 00:00:00':
                waktu_dikirim = get_waktu['data']['dikirim'].strftime("%Y-%m-%d %H:%M:%S")
                print(f"Load Waktu Dikirim {waktu_dikirim}")
            if get_waktu['row_count'] > 0 and get_waktu['data']['diterima'] != '0000-00-00 00:00:00':
                waktu_diterima = get_waktu['data']['diterima'].strftime("%Y-%m-%d %H:%M:%S")
                print(f"Load Waktu Diterima {waktu_diterima}")
            get_pemeriksaan = select_from_table(table_name="pemeriksaan_ralan",columns=["tgl_perawatan","jam_rawat"],where_conditions={"no_rawat":get_no_rawat['data']['no_rawat']}, limit=1,fetch_all=False)
            print(get_pemeriksaan)
            if get_pemeriksaan['row_count'] > 0:
                waktu_perawatan = f"{get_pemeriksaan['data']['tgl_perawatan']} {get_pemeriksaan['data']['jam_rawat']}"
                print(f"Load Waktu Perawatan {waktu_perawatan}")
            else:
                waktu_perawatan = '0000-00-00 00:00:00'
                print(f"Load Waktu Perawatan {waktu_perawatan}")
        if get_no_rawat['row_count'] > 0 and get_no_rawat['data']['stts'] == 'Batal':
            get_sep = select_from_table(table_name="bridging_sep",columns=["no_rawat","tglsep"],where_conditions={"no_rawat":get_no_rawat['data']['no_rawat']}, limit=1,fetch_all=False)
            print(get_sep)
            print("Data Tidak Ada")
            data_no_rawat = {
                "no_rawat": get_no_rawat['data']['no_rawat'],
                "stts": get_no_rawat['data']['stts']
            }
            if get_sep['row_count'] > 0:
                data_sep = {
                    "no_rawat": get_no_rawat['data']['no_rawat'],
                    "tglsep": get_sep['data']['tglsep']
                }
            else:
                data_sep = {
                    "no_rawat": 'Kosong',
                    "tglsep": ''
                }
            waktu_perawatan = 'Kosong'
            waktu_dikirim = 'Kosong'
            waktu_diterima = 'Kosong'
    else:
        print("Data Tidak Ada")
        data_no_rawat = {
            "no_rawat": '',
            "stts": 'Tidak ada data di database rumah sakit'
        }
        data_sep = {
            "no_rawat": '',
            "tglsep": ''
        }
        waktu_perawatan = 'Kosong'
        waktu_dikirim = 'Kosong'
        waktu_diterima = 'Kosong'
    print("Load Datanya Sampai Disini Saja ?")
    print("=="*30)
    response.append({
        "no_rawat": data_no_rawat['no_rawat'],
        "stts": data_no_rawat['stts'],
        "waktu_perawatan": waktu_perawatan,
        "waktu_dikirim": waktu_dikirim,
        "waktu_diterima": waktu_diterima,
        "task": task,
        "data_sep": data_sep['no_rawat']
    })
    print(f"Response : {response}")
    return response

def listTable(tanggal):
    data = read_from_sqlite_table(table_name="data_antrol",where="status='Belum dilayani' or status='Sedang dilayani'")
    data_count = read_from_sqlite_table(table_name="data_antrol",columns=["count(*) as row_count"],where="status='Belum dilayani' or status='Sedang dilayani'", limit=1)
    response = []
    response.append({
        "total_data": data_count[0]['row_count'],
        "data": data
    })
    return response

def batal_mjkn(no_rawat='',kodebooking='',skip_check_sep=False):
    print("Pemeriksaan dibatalkan")
    print("Check Bridging SEP")
    if skip_check_sep == False:
        print("Check Bridging SEP")
        get_sep = select_from_table(table_name="bridging_sep",columns=["no_rawat"],where_conditions={"no_rawat":no_rawat,"jnspelayanan":"2"}, limit=1,fetch_all=False)
        print(f"get_sep : {get_sep}")
        if get_sep['row_count'] == 0:
            print("No Bridging SEP")
            print("Send Task Batal")
            print(f"kode_booking : {kodebooking}")
            task_batal(kodebooking)
            print("Pemeriksaan dibatalkan")
        else:
            print("Data SEP ada")
    else:
        print("Skip Check Bridging SEP And Send Task Batal")
        task_batal(kodebooking)

def bacajson(offset: int = 0):
    data = read_from_sqlite_table(table_name="data_antrol",where="status='Belum dilayani' or status='Sedang dilayani'", limit=5, offset=offset)
    data_count = read_from_sqlite_table(table_name="data_antrol",columns=["count(*) as row_count"],where="status='Belum dilayani' or status='Sedang dilayani'", limit=1)
    response = []
    print(f"Data Antrol Belum Dilayani: {data}")
    total_page = int(data_count[0]['row_count'] / 5)
    print(f"Data Count : {data_count[0]['row_count']} rows \n Page : {offset} \n Total Page : {total_page}")
    response.append({
        "total_data": data_count[0]['row_count'],
        "page": offset,
        "total_page": total_page,
        "next_page_url": f"/list-json/{offset+1}"
    })
    for value in data:
        timestamp = str(int(datetime.today().timestamp()))
        keys = consid_antrol+secret_antrol+timestamp
        data = loadtask(timestamp=timestamp,kodebooking=value['kode_booking'])
        print(f"Data Task: {data}")
        
        if not data:
            continue
        
        if isinstance(data, dict) and 'metadata' not in data:
            print("Metadata tidak ditemukan")
            continue
        
        if data.get('metadata', {}).get('code') == 200:
            dec = decrypt(keys,data['response'])
            df = pd.read_json(StringIO(dec))
            print(f"Data Task: {df.to_dict(orient='records')}")
            counter_task_3 = 0
            counter_task_4 = 0
            counter_task_5 = 0
            for index, row in df.iterrows():
                if row['taskid'] == 3:
                    counter_task_3 += 1
                elif row['taskid'] == 4:
                    counter_task_4 += 1
                elif row['taskid'] == 5:
                    counter_task_5 += 1
                response.append({
                    "taskid": row['taskid'],
                    "waktu": row['waktu'],
                    "wakturs": row['wakturs'],
                    "taskname": row['taskname'],
                    "kodebooking": row['kodebooking']   
                })
            print(f"counter_task_3 : {counter_task_3}")
            print(f"counter_task_4 : {counter_task_4}")
            print(f"counter_task_5 : {counter_task_5}")
            print("Pengecekan Task 3,4,5")
            # if counter_task_3 == 0 or counter_task_3 == 1 or counter_task_4 == 0 or counter_task_5 == 0:
            get_data_antrian = select_from_table(table_name="mlite_antrian_referensi",columns=["no_rkm_medis","tanggal_periksa"],where_conditions={"kodebooking":value['kode_booking']}, limit=1,fetch_all=False)
            print(f"get_data_antrian : {get_data_antrian}")
            if get_data_antrian['row_count'] > 0:
                get_no_rawat = select_from_table(
                    table_name="reg_periksa",
                    columns=["no_rawat", "stts"],
                    where_conditions={
                        "no_rkm_medis": get_data_antrian['data']['no_rkm_medis'],
                        "tgl_registrasi": get_data_antrian['data']['tanggal_periksa'],
                        "kd_poli": {"op": "!=", "val": "IGD01"}
                    },
                    limit=1,
                    fetch_all=False
                )
                print(f"get_no_rawat : {get_no_rawat}")
                if get_no_rawat['row_count'] == 0:
                    print("Data reg_periksa tidak ditemukan")
                elif get_no_rawat['data']['stts'] == 'Batal':
                    print("Pemeriksaan dibatalkan")
                    batal_mjkn(get_no_rawat['data']['no_rawat'],value['kode_booking'])
                    time.sleep(2)
                else:
                    is_send_3 = False
                    is_send_4 = False
                    print("Pengecekan Task 3")
                    get_waktu_3 = select_from_table(table_name="mutasi_berkas",columns=["dikirim","diterima"],where_conditions={"no_rawat":get_no_rawat['data']['no_rawat']}, limit=1,fetch_all=False)
                    if get_waktu_3['row_count'] > 0 and get_waktu_3['data']['dikirim'] != '0000-00-00 00:00:00':
                        kirim_task_3 = task_kodebooking(value['kode_booking'],3,get_waktu_3['data']['dikirim'].strftime("%Y-%m-%d %H:%M:%S"))
                        print(f"kirim_task 3: {kirim_task_3}")
                        if kirim_task_3['metadata']['code'] == 200 or kirim_task_3['metadata']['code'] == 208:
                            is_send_3 = True
                    time.sleep(2)
                    if is_send_3 == True:
                        print("Pengecekan Task 4")
                        get_waktu_4 = select_from_table(table_name="mutasi_berkas",columns=["dikirim","diterima"],where_conditions={"no_rawat":get_no_rawat['data']['no_rawat']}, limit=1,fetch_all=False)
                        print(f"get_waktu_4 : {get_waktu_4}")
                        if get_waktu_4['row_count'] > 0 and get_waktu_4['data']['diterima'] != '0000-00-00 00:00:00':
                            print("Coba Kirim Task ID 4 Jika Diterima Tidak Kosong")
                            kirim_task_4 = task_kodebooking(value['kode_booking'],4,get_waktu_4['data']['diterima'].strftime("%Y-%m-%d %H:%M:%S"))
                            if "tidak boleh kurang atau sama dengan waktu sebelumnya" in kirim_task_4['metadata']['message']:
                                sample = kirim_task_4['metadata']['message']
                                get_new_date_4 = sample.split("(")[1].split(" ")[0]
                                get_new_waktu_4 = sample.split("(")[1].split(" ")[1]
                                print(f"get_new_4 : {get_new_date_4} {get_new_waktu_4}")
                                new_time_4 = get_new_date_4+" "+get_new_waktu_4
                                new_add_10_min = datetime.strptime(new_time_4, "%Y-%m-%d %H:%M:%S") + timedelta(minutes=10)
                                kirim_task_4 = task_kodebooking(value['kode_booking'],4,new_add_10_min.strftime("%Y-%m-%d %H:%M:%S"))
                            if "lebih besar dari pada TaskId=4" in kirim_task_4['metadata']['message']:
                                sample = kirim_task_4['metadata']['message']
                                get_new_date_4_task_3 = sample.split("(")[1].split(" ")[0]
                                get_new_waktu_4_task_3 = sample.split("(")[1].split(" ")[1]
                                print(f"get_new_4_task_3 : {get_new_date_4_task_3} {get_new_waktu_4_task_3}")
                                new_time_4 = get_new_date_4_task_3+" "+get_new_waktu_4_task_3
                                print("Coba Kirim Task ID 4 Dengan Waktu Baru")
                                new_add_10_min = datetime.strptime(new_time_4, "%Y-%m-%d %H:%M:%S") + timedelta(minutes=10)
                                print(f"new_add_10_min : {new_add_10_min}")
                                kirim_task_4 = task_kodebooking(value['kode_booking'],4,new_add_10_min.strftime("%Y-%m-%d %H:%M:%S"))
                            print(f"kirim_task 4: {kirim_task_4}")
                            if kirim_task_4['metadata']['code'] == 200 or kirim_task_4['metadata']['code'] == 208:
                                is_send_4 = True
                        if get_waktu_4['row_count'] > 0 and get_waktu_4['data']['diterima'] == '0000-00-00 00:00:00':
                            new_waktu_4 = get_waktu_4['data']['dikirim'] + timedelta(minutes=10)
                            print("Coba Kirim Task ID 4 Jika Diterima Kosong")
                            print(f"Data Dikirim : {new_waktu_4}")
                            print(f"Data Kodebooking : {value['kode_booking']}")
                            kirim_task_4 = task_kodebooking(value['kode_booking'],4,new_waktu_4.strftime("%Y-%m-%d %H:%M:%S"))
                            if "lebih besar dari pada TaskId=4" in kirim_task_4['metadata']['message']:
                                sample = kirim_task_4['metadata']['message']
                                get_new_date_4_task_3 = sample.split("(")[1].split(" ")[0]
                                get_new_waktu_4_task_3 = sample.split("(")[1].split(" ")[1]
                                print(f"get_new_4_task_3 : {get_new_date_4_task_3} {get_new_waktu_4_task_3}")
                                new_time_4 = get_new_date_4_task_3+" "+get_new_waktu_4_task_3
                                print("Coba Kirim Task ID 4 Dengan Waktu Baru")
                                new_add_10_min = datetime.strptime(new_time_4, "%Y-%m-%d %H:%M:%S") + timedelta(minutes=10)
                                print(f"new_add_10_min : {new_add_10_min}")
                                kirim_task_4 = task_kodebooking(value['kode_booking'],4,new_add_10_min.strftime("%Y-%m-%d %H:%M:%S"))
                            print(f"kirim_task 4: {kirim_task_4}")
                            if kirim_task_4['metadata']['code'] == 200 or kirim_task_4['metadata']['code'] == 208:
                                is_send_4 = True
                        time.sleep(2)
                    if is_send_4 == True:
                        print("Pengecekan Task 5")
                        get_waktu_4 = select_from_table(table_name="mutasi_berkas",columns=["dikirim","diterima"],where_conditions={"no_rawat":get_no_rawat['data']['no_rawat']}, limit=1,fetch_all=False)
                        get_pemeriksaan = select_from_table(table_name="pemeriksaan_ralan",columns=["tgl_perawatan","jam_rawat"],where_conditions={"no_rawat":get_no_rawat['data']['no_rawat']}, limit=1,fetch_all=False)
                        if get_pemeriksaan['row_count'] > 0 and get_pemeriksaan['data']['tgl_perawatan'] != '0000-00-00' and get_pemeriksaan['data']['jam_rawat'] != '00:00:00':
                            waktu = f"{get_pemeriksaan['data']['tgl_perawatan']} {get_pemeriksaan['data']['jam_rawat']}"
                            kirim_task_5 = task_kodebooking(value['kode_booking'],5,waktu)
                            if "tidak boleh kurang atau sama dengan waktu sebelumnya" in kirim_task_5['metadata']['message']:
                                random_minutes = random.randint(60, 84)
                                print(f"Random Minutes: {random_minutes}")
                                if get_waktu_4['data']['diterima'] != '0000-00-00 00:00:00':
                                    new_time_5 = get_waktu_4['data']['diterima']
                                    new_add_10_min = new_time_5 + timedelta(minutes=random_minutes)
                                else:
                                    sample = kirim_task_5['metadata']['message']
                                    get_new_date_5_task_4 = sample.split("(")[1].split(" ")[0]
                                    get_new_waktu_5_task_4 = sample.split("(")[1].split(" ")[1]
                                    print(f"get_new_5_task_4 : {get_new_date_5_task_4} {get_new_waktu_5_task_4}")
                                    new_time_5 = get_new_date_5_task_4+" "+get_new_waktu_5_task_4
                                    new_add_10_min = datetime.strptime(new_time_5, "%Y-%m-%d %H:%M:%S") + timedelta(minutes=random_minutes)
                                print("Coba Kirim Task ID 5 Dengan Waktu Baru")
                                print(f"new_add_10_min : {new_add_10_min}")
                                kirim_task_5 = task_kodebooking(value['kode_booking'],5,new_add_10_min.strftime("%Y-%m-%d %H:%M:%S"))
                            if "Tanggal pelayanan untuk Kode Booking tersebut adalah" in kirim_task_5['metadata']['message']:
                                if get_waktu_4['row_count'] > 0 and get_waktu_4['data']['diterima'] == '0000-00-00 00:00:00':
                                    new_time_5 = get_waktu_4['data']['dikirim']
                                else:
                                    new_time_5 = get_waktu_4['data']['diterima']
                                random_minutes = random.randint(60, 84)
                                new_add_10_min = new_time_5 + timedelta(minutes=random_minutes)
                                kirim_task_5 = task_kodebooking(value['kode_booking'],5,new_add_10_min.strftime("%Y-%m-%d %H:%M:%S"))
                            print(f"kirim_task 5: {kirim_task_5}")
                        if get_pemeriksaan['row_count'] == 0:
                            random_minutes = random.randint(20, 30)
                            print("Creating Fake Task 5 jika antrian ada tapi tidak ada data pemeriksaan")
                            if get_waktu_4['data']['diterima'] != '0000-00-00 00:00:00':
                                print("Pakai Data Diterima")
                                diterima_dt = get_waktu_4['data']['diterima']
                            if get_waktu_4['data']['diterima'] == '0000-00-00 00:00:00':
                                print("Pakai Data Dikirim")
                                diterima_dt = get_waktu_4['data']['dikirim']
                            print(f"Random Minutes: {random_minutes}")
                            waktu_dt = diterima_dt + timedelta(minutes=random_minutes)
                            waktu = waktu_dt.strftime("%Y-%m-%d %H:%M:%S")
                            print(f"waktu: {waktu}")
                            kirim_task_5 = task_kodebooking(value['kode_booking'],5,waktu)
                            print(f"kirim_task 5: {kirim_task_5}")
                        time.sleep(2)
            if get_data_antrian['row_count'] == 0:
                if "k" in value["nomor_referensi"].lower():
                    parameter_search_nya = "noskdp"
                else:
                    parameter_search_nya = "no_rujukan"
                cari_pasien = select_from_table(table_name="pasien",columns=["no_rkm_medis","no_peserta"],where_conditions={"no_ktp":value['nik']}, limit=1,fetch_all=False)
                if cari_pasien['row_count'] > 0:
                    print(f"cari_pasien : {cari_pasien}")
                    print(f"parameter_search_nya : {parameter_search_nya}")
                    print("Coba cari di reg_periksa sama bridging_sep")
                    get_sep = select_from_table(table_name="bridging_sep",columns=["no_rawat","tglsep"],where_conditions={"nomr":cari_pasien['data']['no_rkm_medis'],parameter_search_nya:value['nomor_referensi']}, limit=1,fetch_all=False)
                    if get_sep['row_count'] == 0:
                        print("Data bridging_sep tidak ditemukan")
                        continue
                    get_no_rawat = select_from_table(table_name="reg_periksa",columns=["no_rawat","stts"],where_conditions={"no_rkm_medis":cari_pasien['data']['no_rkm_medis'],"tgl_registrasi":get_sep['data']['tglsep'],"kd_poli": {"op": "!=", "val": "IGD01"}}, limit=1,fetch_all=False)
                    if get_no_rawat['row_count'] == 0:
                        print("Data reg_periksa tidak ditemukan")
                        continue
                    if get_no_rawat['data']['stts'] == 'Batal':
                        print("Pemeriksaan dibatalkan")
                        batal_mjkn(get_no_rawat['data']['no_rawat'],value['kode_booking'])
                        time.sleep(2)
                        continue
                    if get_no_rawat['data']['stts'] != 'Batal':
                        is_send_3 = False
                        is_send_4 = False
                        # if counter_task_3 == 0 or counter_task_3 == 1:
                        print("Pengecekan Task 3")
                        get_waktu_3 = select_from_table(table_name="mutasi_berkas",columns=["dikirim","diterima"],where_conditions={"no_rawat":get_no_rawat['data']['no_rawat']}, limit=1,fetch_all=False)
                        if get_waktu_3['row_count'] > 0 and get_waktu_3['data']['dikirim'] != '0000-00-00 00:00:00':
                            kirim_task_3 = task_kodebooking(value['kode_booking'],3,get_waktu_3['data']['dikirim'].strftime("%Y-%m-%d %H:%M:%S"))
                            print(f"kirim_task 3: {kirim_task_3}")
                            if kirim_task_3['metadata']['code'] == 200 or kirim_task_3['metadata']['code'] == 208:
                                is_send_3 = True
                        time.sleep(2)
                        if is_send_3 == True:
                            print("Pengecekan Task 4")
                            get_waktu_4 = select_from_table(table_name="mutasi_berkas",columns=["dikirim","diterima"],where_conditions={"no_rawat":get_no_rawat['data']['no_rawat']}, limit=1,fetch_all=False)
                            print(f"get_waktu_4 : {get_waktu_4}")
                            if get_waktu_4['row_count'] > 0 and get_waktu_4['data']['diterima'] != '0000-00-00 00:00:00':
                                print("Coba Kirim Task ID 4 Jika Diterima Tidak Kosong Dan Antrian Kosong")
                                kirim_task_4 = task_kodebooking(value['kode_booking'],4,get_waktu_4['data']['diterima'].strftime("%Y-%m-%d %H:%M:%S"))
                                print(f"kirim_task 4: {kirim_task_4}")
                                if kirim_task_4['metadata']['code'] == 200 or kirim_task_4['metadata']['code'] == 208:
                                    is_send_4 = True
                            if get_waktu_4['row_count'] > 0 and get_waktu_4['data']['diterima'] == '0000-00-00 00:00:00':
                                print("Coba Kirim Task ID 4 Jika Diterima Kosong Dan Antrian Kosong")
                                new_waktu_4 = get_waktu_4['data']['dikirim'] + timedelta(minutes=10)
                                kirim_task_4 = task_kodebooking(value['kode_booking'],4,new_waktu_4.strftime("%Y-%m-%d %H:%M:%S"))
                                print(f"kirim_task 4: {kirim_task_4}")
                                if kirim_task_4['metadata']['code'] == 200 or kirim_task_4['metadata']['code'] == 208:
                                    is_send_4 = True
                            time.sleep(2)
                        if is_send_4 == True:
                            print("Pengecekan Task 5")
                            get_pemeriksaan = select_from_table(table_name="pemeriksaan_ralan",columns=["tgl_perawatan","jam_rawat"],where_conditions={"no_rawat":get_no_rawat['data']['no_rawat']}, limit=1,fetch_all=False)
                            if get_pemeriksaan['row_count'] > 0 and get_pemeriksaan['data']['tgl_perawatan'] != '0000-00-00' and get_pemeriksaan['data']['jam_rawat'] != '00:00:00':
                                waktu = f"{get_pemeriksaan['data']['tgl_perawatan']} {get_pemeriksaan['data']['jam_rawat']}"
                                kirim_task_5 = task_kodebooking(value['kode_booking'],5,waktu)
                                print(f"kirim_task 5: {kirim_task_5}")
                                if "tidak boleh kurang atau sama dengan waktu sebelumnya" in kirim_task_5['metadata']['message']:
                                    random_minutes = random.randint(60, 84)
                                    sample = kirim_task_5['metadata']['message']
                                    get_new_date_5 = sample.split("(")[1].split(" ")[0]
                                    get_new_waktu_5 = sample.split("(")[1].split(" ")[1]
                                    print(f"get_new_5 : {get_new_date_5} {get_new_waktu_5}")
                                    new_time_5 = get_new_date_5+" "+get_new_waktu_5
                                    new_add_10_min = datetime.strptime(new_time_5, "%Y-%m-%d %H:%M:%S") + timedelta(minutes=random_minutes)
                                    kirim_task_5 = task_kodebooking(value['kode_booking'],5,new_add_10_min.strftime("%Y-%m-%d %H:%M:%S"))
                                if "Tanggal pelayanan untuk Kode Booking tersebut adalah" in kirim_task_5['metadata']['message']:
                                    get_waktu_4 = select_from_table(table_name="mutasi_berkas",columns=["dikirim","diterima"],where_conditions={"no_rawat":get_no_rawat['data']['no_rawat']}, limit=1,fetch_all=False)
                                    if get_waktu_4['row_count'] > 0 and get_waktu_4['data']['diterima'] == '0000-00-00 00:00:00':
                                        new_time_5 = get_waktu_4['data']['dikirim']
                                    else:
                                        new_time_5 = get_waktu_4['data']['diterima']
                                    random_minutes = random.randint(60, 84)
                                    new_add_10_min = new_time_5 + timedelta(minutes=random_minutes)
                                    kirim_task_5 = task_kodebooking(value['kode_booking'],5,new_add_10_min.strftime("%Y-%m-%d %H:%M:%S"))
                                    print(f"kirim_task 5: {kirim_task_5}")
                            if get_pemeriksaan['row_count'] == 0:
                                random_minutes = random.randint(60, 84)
                                print("Creating Fake Task 5 jika antrian tidak ada")
                                if get_waktu_4['row_count'] > 0 and get_waktu_4['data']['diterima'] != '0000-00-00 00:00:00':
                                    diterima_dt = get_waktu_4['data']['diterima']
                                else:
                                    diterima_dt = get_waktu_4['data']['dikirim']
                                waktu_dt = diterima_dt + timedelta(minutes=random_minutes)
                                waktu = waktu_dt.strftime("%Y-%m-%d %H:%M:%S")
                                print(f"waktu: {waktu}")
                                kirim_task_5 = task_kodebooking(value['kode_booking'],5,waktu)
                                print(f"kirim_task 5: {kirim_task_5}")
                            time.sleep(2)
                    if get_no_rawat['data']['stts'] == 'Batal':
                            batal_mjkn(get_no_rawat['data']['no_rawat'],value['kode_booking'])
                            time.sleep(2)

        if data.get('metadata', {}).get('code') == 204:
            get_data_antrian = select_from_table(table_name="mlite_antrian_referensi",columns=["no_rkm_medis","tanggal_periksa"],where_conditions={"kodebooking":value['kode_booking']}, limit=1,fetch_all=False)
            print(f"get_data_antrian : {get_data_antrian}")
            if get_data_antrian['row_count'] > 0:
                print("Pengecekan Registrasi")
                get_no_rawat = select_from_table(table_name="reg_periksa",columns=["no_rawat","stts"],where_conditions={"no_rkm_medis":get_data_antrian['data']['no_rkm_medis'],"tgl_registrasi":get_data_antrian['data']['tanggal_periksa'],"kd_poli": {"op": "!=", "val": "IGD01"}}, limit=1,fetch_all=False)
                if get_no_rawat['row_count'] == 0:
                    print("Data Tidak Ditemukan. Memulai Penghapusan Antrian")
                    batal_mjkn(kodebooking=value['kode_booking'],skip_check_sep=True)
                    time.sleep(2)
                if get_no_rawat['row_count'] > 0 and get_no_rawat['data']['stts'] != 'Batal':
                    get_waktu = select_from_table(table_name="mutasi_berkas",columns=["dikirim","diterima"],where_conditions={"no_rawat":get_no_rawat['data']['no_rawat']}, limit=1,fetch_all=False)
                    get_pemeriksaan = select_from_table(table_name="pemeriksaan_ralan",columns=["tgl_perawatan","jam_rawat"],where_conditions={"no_rawat":get_no_rawat['data']['no_rawat']}, limit=1,fetch_all=False)
                    if get_waktu['row_count'] > 0:
                        is_send_3 = False
                        is_send_4 = False
                        if get_waktu['data']['dikirim'] != '0000-00-00 00:00:00':
                            kirim_task_3 = task_kodebooking(value['kode_booking'],3,get_waktu['data']['dikirim'].strftime("%Y-%m-%d %H:%M:%S"))
                            if kirim_task_3['metadata']['code'] == 200:
                                is_send_3 = True
                            print(f"kirim_task 3: {kirim_task_3}")
                            time.sleep(2)
                        if get_waktu['data']['diterima'] != '0000-00-00 00:00:00' and is_send_3 == True:
                            kirim_task_4 = task_kodebooking(value['kode_booking'],4,get_waktu['data']['diterima'].strftime("%Y-%m-%d %H:%M:%S"))
                            if kirim_task_4['metadata']['code'] == 200:
                                is_send_4 = True
                            print(f"kirim_task 4: {kirim_task_4}")
                            time.sleep(2)
                        if get_pemeriksaan['row_count'] > 0 and is_send_4 == True:
                            print("Sending Task 5")
                            waktu = f"{get_pemeriksaan['data']['tgl_perawatan']} {get_pemeriksaan['data']['jam_rawat']}"
                            kirim_task_5 = task_kodebooking(value['kode_booking'],5,waktu)
                            print(f"kirim_task 5: {kirim_task_5}")
                            time.sleep(2)
                        if get_pemeriksaan['row_count'] == 0 and is_send_4 == True:
                            print("Creating Fake Task 5")
                            # diterima_dt = datetime.strptime(get_waktu['data']['diterima'], "%Y-%m-%d %H:%M:%S")
                            # print(f"diterima_dt: {diterima_dt}")
                            waktu_dt = get_waktu['data']['diterima'] + timedelta(minutes=random.randint(10, 20))
                            print(f"waktu_dt: {waktu_dt}")
                            waktu = waktu_dt.strftime("%Y-%m-%d %H:%M:%S")
                            print(f"waktu: {waktu}")
                            kirim_task_5 = task_kodebooking(value['kode_booking'],5,waktu)
                            print(f"kirim_task 5: {kirim_task_5}")
                            time.sleep(2)
                if get_no_rawat['data']['stts'] == 'Batal':
                    batal_mjkn(get_no_rawat['data']['no_rawat'],value['kode_booking'])
                    time.sleep(2)
            if get_data_antrian['row_count'] == 0:
                if "k" in value["nomor_referensi"].lower():
                    parameter_search_nya = "noskdp"
                else:
                    parameter_search_nya = "no_rujukan"
                cari_pasien = select_from_table(table_name="pasien",columns=["no_rkm_medis","no_peserta"],where_conditions={"no_ktp":value['nik']}, limit=1,fetch_all=False)
                if cari_pasien['row_count'] > 0:
                    print(f"cari_pasien : {cari_pasien}")
                    print(f"parameter_search_nya : {parameter_search_nya}")
                    print("Coba cari di reg_periksa sama bridging_sep Jika tidak ada data antrian")
                    if parameter_search_nya == "noskdp":
                        print("Cari di bridging_sep berdasarkan nomr dan noskdp")
                        get_sep = select_from_table(table_name="bridging_sep",columns=["no_rawat","tglsep"],where_conditions={"nomr":cari_pasien['data']['no_rkm_medis'],"noskdp":value['nomor_referensi']}, limit=1,fetch_all=False)
                        print(f"get_sep : {get_sep}")
                    else:
                        print("Cari di bridging_sep berdasarkan nomr dan no_rujukan")
                        get_sep = select_from_table(table_name="bridging_sep",columns=["no_rawat","tglsep"],where_conditions={"nomr":cari_pasien['data']['no_rkm_medis'],"no_rujukan":value['nomor_referensi']}, limit=1,fetch_all=False)
                        print(f"get_sep : {get_sep}")
                    if get_sep['row_count'] > 0:
                        get_no_rawat = select_from_table(table_name="reg_periksa",columns=["no_rawat","stts"],where_conditions={"no_rkm_medis":cari_pasien['data']['no_rkm_medis'],"tgl_registrasi":get_sep['data']['tglsep'],"kd_poli": {"op": "!=", "val": "IGD01"}}, limit=1,fetch_all=False)
                        print(f"get_no_rawat : {get_no_rawat}")
                        # if get_sep['row_count'] == 0:
                        #     batal_mjkn(get_no_rawat['data']['no_rawat'],value['kode_booking'])
                        #     time.sleep(2)
                        if get_no_rawat['row_count'] > 0 and get_no_rawat['data']['stts'] != 'Batal':
                            get_waktu = select_from_table(table_name="mutasi_berkas",columns=["dikirim","diterima"],where_conditions={"no_rawat":get_no_rawat['data']['no_rawat']}, limit=1,fetch_all=False)
                            get_pemeriksaan = select_from_table(table_name="pemeriksaan_ralan",columns=["tgl_perawatan","jam_rawat"],where_conditions={"no_rawat":get_no_rawat['data']['no_rawat']}, limit=1,fetch_all=False)
                            if get_waktu['row_count'] > 0:
                                is_send_3 = False
                                is_send_4 = False
                                if get_waktu['data']['dikirim'] != '0000-00-00 00:00:00':
                                    kirim_task_3 = task_kodebooking(value['kode_booking'],3,get_waktu['data']['dikirim'].strftime("%Y-%m-%d %H:%M:%S"))
                                    if kirim_task_3['metadata']['code'] == 200:
                                        is_send_3 = True
                                    print(f"kirim_task 3: {kirim_task_3}")
                                    time.sleep(2)
                                if get_waktu['data']['diterima'] != '0000-00-00 00:00:00' and is_send_3 == True:
                                    kirim_task_4 = task_kodebooking(value['kode_booking'],4,get_waktu['data']['diterima'].strftime("%Y-%m-%d %H:%M:%S"))
                                    if kirim_task_4['metadata']['code'] == 200:
                                        is_send_4 = True
                                    print(f"kirim_task 4: {kirim_task_4}")
                                    time.sleep(2)
                                if get_pemeriksaan['row_count'] > 0 and is_send_4 == True:
                                    print("Sending Task 5")
                                    waktu = f"{get_pemeriksaan['data']['tgl_perawatan']} {get_pemeriksaan['data']['jam_rawat']}"
                                    kirim_task_5 = task_kodebooking(value['kode_booking'],5,waktu)
                                    print(f"kirim_task 5: {kirim_task_5}")
                                    time.sleep(2)
                                if get_pemeriksaan['row_count'] == 0 and is_send_4 == True:
                                    print("Creating Fake Task 5 Jika tidak ada data antrian")
                                    diterima_dt = datetime.strptime(get_waktu['data']['diterima'], "%Y-%m-%d %H:%M:%S")
                                    print(f"diterima_dt: {diterima_dt}")
                                    random_minutes = random.randint(10, 20)
                                    print(f"random_minutes: {random_minutes}")
                                    waktu_dt = diterima_dt + timedelta(minutes=random_minutes)
                                    waktu = waktu_dt.strftime("%Y-%m-%d %H:%M:%S")
                                    print(f"waktu: {waktu}")
                                    kirim_task_5 = task_kodebooking(value['kode_booking'],5,waktu)
                                    print(f"kirim_task 5: {kirim_task_5}")
                                    time.sleep(2)
                        if get_no_rawat['data']['stts'] == 'Batal':
                            batal_mjkn(get_no_rawat['data']['no_rawat'],value['kode_booking'])
                            time.sleep(2)
                    if get_sep['row_count'] == 0:
                        print(f"Data {cari_pasien['data']['no_rkm_medis']} Tidak ADA di Bridging SEP")
                        print(f"Kode Booking : {value['kode_booking']}")
                        batal_mjkn(kodebooking=value['kode_booking'],skip_check_sep=True)
        time.sleep(5)
    return response

def verify_api_key(x_api_key: str = Header(...)):
    valid_keys = [token_antrol]
    
    if x_api_key not in valid_keys:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    return x_api_key

app = FastAPI(
    docs_url=None,  # Disable /docs
    redoc_url=None,  # Disable /redoc
    openapi_url=None  # Disable /openapi.json
)

@app.on_event("startup")
def startup_event():
    init_sqlite_db()

@app.get("/")
def read_root():
    return FileResponse("html/load.html")

@app.get("/load.html")
def read_load():
    return FileResponse("html/load.html")

@app.get("/health")
def read_root():
    return {"message": "API is running"}

@app.post("/checkin")
async def checkin(request: Request, model: Checkin, x_api_key: str = Depends(verify_api_key)):
    try:
        body = await request.json()
        check_kode = checkin_kodebooking(body.get("kodebooking"))
        print(body.get("kodebooking"))
        print(f"Check-in response: {check_kode}")
        return {"message": "Check-in successful", "kodebooking": check_kode}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/checkin_taskid")
async def checkin_taskid(request: Request, model: CheckinTaskid, x_api_key: str = Depends(verify_api_key)):
    try:
        body = await request.json()
        print(body)
        waktu = body.get("waktu")
        if waktu:
            check_kode = task_kodebooking(body.get("kodebooking"), body.get("taskid"), waktu)
        else:
            check_kode = task_kodebooking(body.get("kodebooking"), body.get("taskid"))
        print(body.get("kodebooking"))
        print(f"Check-in response: {check_kode}")
        return {"message": "Send to taskid successful", "kodebooking": check_kode}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/list-antri/{tanggal}")
async def list_antri(tanggal: str, x_api_key: str = Depends(verify_api_key)):
    try:
        list_antri = get_list_antri(tanggal)
        return {"message": "List of antrian", "data": list_antri}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/list-antri-tampil/{tanggal}")
async def list_antri_tampil(tanggal: str, x_api_key: str = Depends(verify_api_key)):
    try:
        list_antri = get_list_antri_tampil(tanggal)
        return {"message": "List of antrian", "data": list_antri}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/list-table/{tanggal}")
async def list_table(tanggal: str, x_api_key: str = Depends(verify_api_key)):
    try:
        list_antri = listTable(tanggal)
        return {"message": "List of antrian", "data": list_antri}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/show-task/{kodebooking}")
async def show_task(kodebooking: str, x_api_key: str = Depends(verify_api_key)):
    try:
        timestamp = str(int(datetime.today().timestamp()))
        keys = consid_antrol+secret_antrol+timestamp
        list_antri = loadtask(keys=keys,timestamp=timestamp,kodebooking=kodebooking)
        return {"message": "List of task", "data": list_antri}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/list-json/")
@app.post("/list-json/{offset}")
def list_json(offset: int = 0,x_api_key: str = Depends(verify_api_key)):
    try:
        load = bacajson(offset)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return load
