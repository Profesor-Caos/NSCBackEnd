import logging
import sys
import os
import database
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

# Endpoint to list all students
@app.route('/students', methods=['GET'])
def list_students():
    return database.list_students()

# Endpoint to get a specific student
@app.route('/students/<int:student_id>', methods=['GET'])
def get_student(student_id):
    return database.get_student(student_id)

# Endpoint to add a log entry
@app.route('/logs', methods=['POST'])
def add_log():
    return database.add_log(request)

# Endpoint to get logs for a specific student
@app.route('/logs/<string:student_id>', methods=['GET'])
def get_logs_for_student(student_id):
    return database.get_logs_for_student(student_id)

# Endpoint to list all logs
@app.route('/logs', methods=['GET'])
def list_logs():
    return database.list_logs()