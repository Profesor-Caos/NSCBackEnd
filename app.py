from datetime import datetime
from flask import Flask, request, jsonify
import psycopg2
from psycopg2.extras import DictCursor
import os
import threading
import queue

app = Flask(__name__)

DATABASE_URL = os.getenv('DATABASE_URL')

def get_db_connection():
    """Create a new database connection."""
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=DictCursor)
    return conn


# Queue to hold log entries
log_queue = queue.Queue()

# Batch size for writing to the database
BATCH_SIZE = 100

def init_db():
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL") # Enable Write Ahead Logging for better write throughput

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS students (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                student_id INTEGER NOT NULL UNIQUE
            );
        ''')
        
        cursor.execute('DROP TABLE logs')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                student_id INTEGER NOT NULL,
                timestamp DATETIME NOT NULL,
                page_number INTEGER NOT NULL,
                log_data TEXT NOT NULL,
                FOREIGN KEY(student_id) REFERENCES students(student_id)
            );
        ''')

        conn.commit()

# Worker function to process the log queue in batches
def process_queue():
    while True:
        try:
            batch = []
            while len(batch) < BATCH_SIZE:
                log_entry = log_queue.get(timeout=1)  # Wait for new entries
                if log_entry is None:  # Stop signal
                    return
                batch.append(log_entry)
            
            if batch:
                write_logs_to_db(batch)
        except queue.Empty:
            if batch:
                write_logs_to_db(batch)  # Write remaining logs if timeout
        except Exception as e:
            print(f"Error processing queue: {e}")

def write_logs_to_db(batch):
    """Batch insert logs into the database."""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.executemany(
            "INSERT INTO logs (student_id, timestamp, page_number, log_data) VALUES (?, ?, ?, ?)",
            batch
        )
        conn.commit()
    print(f"Written {len(batch)} logs to the database.")

# Endpoint to add a new student
@app.route('/students', methods=['POST'])
def add_student():
    data = request.json
    student_id = data.get('StudentID')

    if not student_id:
        return jsonify({"error": "StudentID is required"}), 400

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO students (student_id) VALUES (?)", (student_id,))
            conn.commit()
        return jsonify({"message": "Student added successfully"}), 201
    except psycopg2.Error as e:
        return jsonify({"error": "StudentID already exists - " + str(e) }), 400
    
# Endpoint to list all students
@app.route('/students', methods=['GET'])
def list_students():
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM students")
        rows = cursor.fetchall()
        data = [{"id": row[0], "StudentID": row[1]} for row in rows]
    return jsonify(data)

# Endpoint to add a log entry
@app.route('/logs', methods=['POST'])
def add_log():
    data = request.json
    student_id = data.get('StudentID')
    timestamp = data.get('Timestamp')
    page_number = data.get('PageNumber')
    log_data = data.get('LogData')

    if not all([student_id, timestamp, page_number, log_data]):
        return jsonify({"error": "StudentID, Timestamp, PageNumber, and LogData are required"}), 400

    # Validate timestamp format
    try:
        datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        return jsonify({"error": "Invalid Timestamp format. Use 'YYYY-MM-DD HH:MM:SS.FFF'"}), 400

     # Add the log entry to the queue
    log_queue.put(item=(student_id, timestamp, page_number, log_data))
    return jsonify({"message": "Log entry queued"}), 201

# Endpoint to get logs for a specific student
@app.route('/logs/<string:student_id>', methods=['GET'])
def get_logs_for_student(student_id):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM logs WHERE student_id = ?", (student_id,))
        logs = [
            {
                "ID": row[0],
                "StudentID": row[1],
                "Timestamp": row[2],
                "PageNumber": row[3],
                "LogData": row[4]
            }
            for row in cursor.fetchall()
        ]
    return jsonify(logs)

# Endpoint to list all logs
@app.route('/logs', methods=['GET'])
def list_logs():
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM logs")
        logs = [
            {
                "ID": row[0],
                "StudentID": row[1],
                "Timestamp": row[2],
                "PageNumber": row[3],
                "LogData": row[4]
            }
            for row in cursor.fetchall()
        ]
    return jsonify(logs)

if __name__ == '__main__':
    init_db()  # Initialize the database
    
    # Start the worker thread for batch processing
    worker_thread = threading.Thread(target=process_queue, daemon=True)
    worker_thread.start()

    try:
        app.run(debug=True, threaded=True)
    finally:
        # Gracefully stop the worker thread
        log_queue.put(None)  # Stop signal
        worker_thread.join()