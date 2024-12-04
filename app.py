import logging
import sys
import csv
import os
from datetime import datetime
from flask import Flask, request, jsonify
import psycopg2
from psycopg2.extras import execute_values, DictCursor

app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),  # Send logs to stdout for Render to capture
        logging.StreamHandler(sys.stderr)   # Optional: Send errors to stderr for Render's error logs
    ]
)


DATABASE_URL = os.getenv('DATABASE_URL')

students_file_name = "Student list - Sheet 1"

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
                    test_group INTEGER NOT NULL
                );
            ''')
            

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS logs (
                    id SERIAL PRIMARY KEY,
                    student_id INTEGER NOT NULL REFERENCES students(student_id),
                    timestamp TIMESTAMP NOT NULL,
                    page_number INTEGER NOT NULL,
                    log_data TEXT NOT NULL
                );
            ''')

            conn.commit()
    except Exception as e:
        logging.error("Error initializing DB - " + str(e))

def distribute_test_groups(students):
    grouped_students = {}
    for student in students:
        grade = student['grade_level']
        if grade not in grouped_students:
            grouped_students[grade] = []
        grouped_students[grade].append(student)

    # Assign test groups
    distributed_students = []
    for grade, students in grouped_students.items():
        for i, student in enumerate(students):
            student['test_group'] = 1 if i % 2 == 0 else 2
            distributed_students.append(student)
    
    return distributed_students

def populate_students_table(csv_file):
    with open(csv_file, newline='') as file:
        reader = csv.DictReader(file)
        students = [{'student_id': int(row['student_id']), 'grade_level': int(row['grade_level'])} for row in reader]

    distributed_students = distribute_test_groups(students)

    # Insert into database
    insert_query = """
        INSERT INTO students (student_id, grade_level, test_group)
        VALUES %s
        ON CONFLICT (student_id) DO NOTHING;
    """

    values = [(s['student_id'], s['grade_level'], s['test_group']) for s in distributed_students]

    try:
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor() as cursor:
                execute_values(cursor, insert_query, values)
                conn.commit()
        print(f"Inserted {len(values)} students into the database.")
    except psycopg2.Error as e:
        print(f"Database error: {e}")

# # Endpoint to add a new student
# @app.route('/students', methods=['POST'])
# def add_student():
#     data = request.json
#     student_id = data.get('StudentID')

#     if not student_id:
#         return jsonify({"error": "StudentID is required"}), 400

#     try:
#         with get_db_connection() as conn:
#             cursor = conn.cursor()
#             cursor.execute("INSERT INTO students (student_id) VALUES (%s)", (student_id,))
#             conn.commit()
#         return jsonify({"message": "Student added successfully"}), 201
#     except psycopg2.Error as e:
#         return jsonify({"error": str(e) }), 400
    
# Endpoint to list all students
@app.route('/students', methods=['GET'])
def list_students():
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM students")
        rows = cursor.fetchall()
        data = [{"id": row[0], "StudentID": row[1], "TestGroup": row[2]} for row in rows]
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
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO logs (student_id, timestamp, page_number, log_data) VALUES (%s, %s, %s, %s)", (student_id, timestamp, page_number, log_data))
            conn.commit()
        return jsonify({"message": "Student added successfully"}), 201
    except psycopg2.Error as e:
        return jsonify({"error": str(e) }), 400

# Endpoint to get logs for a specific student
@app.route('/logs/<string:student_id>', methods=['GET'])
def get_logs_for_student(student_id):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM logs WHERE student_id = %s", (student_id,))
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

init_db()  # Initialize the database
populate_students_table(students_file_name)