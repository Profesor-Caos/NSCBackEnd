import datetime
import logging
import sys
import os
from dotenv import load_dotenv
from flask import jsonify
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values, DictCursor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),  # Send logs to stdout for Render to capture
        logging.StreamHandler(sys.stderr)   # Optional: Send errors to stderr for Render's error logs
    ]
)


load_dotenv()
DATABASE_URL = os.getenv('DATABASE_URL')

def get_db_connection():
    """Create a new database connection."""
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=DictCursor)
    return conn

def init_db():
    logging.info("init_db entered successfully.")
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()

            cursor.execute('DROP TABLE IF EXISTS logs')
            cursor.execute('DROP TABLE IF EXISTS students')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS students (
                    id SERIAL PRIMARY KEY,
                    student_id INTEGER UNIQUE NOT NULL,
                    grade_level INTEGER NOT NULL,
                    test_group INTEGER NOT NULL,
                    sheet_id INTEGER NOT NULL
                );
            ''')
            

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS logs (
                    id SERIAL PRIMARY KEY,
                    student_id INTEGER NOT NULL REFERENCES students(student_id),
                    timestamp TIMESTAMP NOT NULL,
                    time_passed INTERVAL NOT NULL,
                    page_number INTEGER NOT NULL,
                    log_data TEXT NOT NULL
                );
            ''')

            conn.commit()
    except Exception as e:
        logging.error("Error initializing DB - " + str(e))

def reset_logs():
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()

            cursor.execute('DROP TABLE IF EXISTS logs')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS logs (
                    id SERIAL PRIMARY KEY,
                    student_id INTEGER NOT NULL REFERENCES students(student_id),
                    timestamp TIMESTAMP NOT NULL,
                    time_passed INTERVAL NOT NULL,
                    page_number INTEGER NOT NULL,
                    log_data TEXT NOT NULL
                );
            ''')

            conn.commit()
    except Exception as e:
        logging.error("Error resetting logs - " + str(e))

def get_student_sheet(sheet_id):
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM students WHERE sheet_id = %s", (sheet_id,))
            rows = cursor.fetchall()
            data = [{"id": row[0], "StudentID": row[1], "GradeLevel": row[2], "TestGroup": row[3], "SheetID": row[4]} for row in rows]
            return jsonify(data)
    except psycopg2.Error as e:
        return jsonify({"error": str(e) }), 400
    
def list_students():
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM students")
        rows = cursor.fetchall()
        data = [{"id": row[0], "StudentID": row[1], "GradeLevel": row[2], "TestGroup": row[3], "SheetID": row[4]} for row in rows]
    return jsonify(data)

def get_student(student_id):
    if student_id < 100000 or student_id > 999999:
        return jsonify({"error": "Student ID outside the acceptable range"}), 400
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM students WHERE student_id = %s", (student_id,))
            row = cursor.fetchone()
            if row is None:
                return jsonify({"error": f"Student ID {student_id} does not exist."}), 404
            
            return jsonify({
                "ID" : row[0],
                "StudentID" : row[1],
                "GradeLevel" : row[2],
                "TestGroup": row[3],
                "SheetID" : row[4]
            })
    except psycopg2.Error as e:
        return jsonify({"error": str(e) }), 400

def add_log(request):
    data = request.json
    student_id = data.get('StudentID')
    timestamp = data.get('Timestamp')
    time_passed = data.get("TimePassed")
    page_number = data.get('PageNumber')
    log_data = data.get('LogData')

    if not all([student_id, timestamp, time_passed, page_number, log_data]):
        return jsonify({"error": "StudentID, Timestamp, TimePassed, PageNumber, and LogData are required"}), 400

    # Validate timestamp format
    try:
        datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        return jsonify({"error": "Invalid Timestamp format. Use 'YYYY-MM-DD HH:MM:SS.FFF'"}), 400

     # Add the log entry to the queue
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO logs (student_id, timestamp, time_passed, page_number, log_data) VALUES (%s, %s, %s, %s, %s)", (student_id, timestamp, time_passed, page_number, log_data))
            conn.commit()
        return jsonify({"message": "Log posted successfully"}), 201
    except psycopg2.Error as e:
        return jsonify({"error": str(e) }), 400

def get_logs_for_student(student_id):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM logs WHERE student_id = %s", (student_id,))
        logs = [
            {
                "ID": row[0],
                "StudentID": row[1],
                "Timestamp": row[2],
                "TimePassed": timedelta_to_string(row[3]),
                "PageNumber": row[4],
                "LogData": row[5]
            }
            for row in cursor.fetchall()
        ]
    return jsonify(logs)

def list_logs():
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM logs")
        logs = [
            {
                "ID": row[0],
                "StudentID": row[1],
                "Timestamp": row[2],
                "TimePassed": timedelta_to_string(row[3]),
                "PageNumber": row[4],
                "LogData": row[5]
            }
            for row in cursor.fetchall()
        ]
    return jsonify(logs)

def timedelta_to_string(td):
    total_seconds = int(td.total_seconds())
    days, remainder = divmod(total_seconds, 86400)  # 86400 seconds in a day
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    milliseconds = td.microseconds // 1000

    if days > 0:
        return f"{days} {hours}:{minutes:02}:{seconds:02}.{milliseconds:03}"
    return f"{hours}:{minutes:02}:{seconds:02}.{milliseconds:03}"