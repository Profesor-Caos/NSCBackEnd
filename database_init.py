import database
import csv
import psycopg2
from psycopg2.extras import execute_values, DictCursor

STUDENTS_FILE = "Student list - Sheet1.csv"
STUDENTS_FILE_2 = "Student list - Sheet2.csv"
STUDENTS_FILE_3 = "Student list - Sheet3.csv"

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
            student['test_group'] = i % 3 + 1
            distributed_students.append(student)
    
    return distributed_students

def add_test_IDs():

    values = [(111111, 0, 1, 0), (222222, 0, 2, 0), (333333, 0, 3, 0), (444444, 0, 1, 0), (555555, 0, 2, 0), (666666, 0, 3, 0)]
    insert_students(values)

def populate_students_table(csv_file, sheet_id):
    with open(csv_file, newline='') as file:
        reader = csv.DictReader(file)
        students = [{'student_id': int(row['IDs']), 'grade_level': int(row['Grade'])} for row in reader]

    distributed_students = distribute_test_groups(students)
    values = [(s['student_id'], s['grade_level'], s['test_group'], sheet_id) for s in distributed_students]
    insert_students(values)

def insert_students(values):
    insert_query = """
        INSERT INTO students (student_id, grade_level, test_group, sheet_id)
        VALUES %s
        ON CONFLICT (student_id) DO NOTHING;
    """

    try:
        with database.get_db_connection() as conn:
            with conn.cursor() as cursor:
                execute_values(cursor, insert_query, values)
                conn.commit()
        print(f"Inserted {len(values)} students into the database.")
    except psycopg2.Error as e:
        print(f"Database error: {e}")

database.init_db()  # Initialize the database
populate_students_table(STUDENTS_FILE, 1)
populate_students_table(STUDENTS_FILE_2, 2)
populate_students_table(STUDENTS_FILE_3, 3)
add_test_IDs()
# database.reset_logs()