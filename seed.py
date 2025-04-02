import random
import datetime
from faker import Faker
from sqlalchemy.exc import SQLAlchemyError
from conf.db import session
from conf.models import Teacher, Group, Student, Subject, Grade

fake = Faker('uk-UA')

NUMBER_OF_STUDENTS = 30
NUMBER_OF_GROUPS = 3
NUMBER_OF_SUBJECTS = 6
NUMBER_OF_TEACHERS = 3
GRADES_PER_SUBJECT = 3

def insert_groups():
    groups = [Group(name=fake.word()) for _ in range(NUMBER_OF_GROUPS)]
    session.add_all(groups)
    session.commit()
    return session.query(Group).all()

def insert_teachers():
    teachers = [Teacher(fullname=fake.name()) for _ in range(NUMBER_OF_TEACHERS)]
    session.add_all(teachers)
    session.commit()
    return session.query(Teacher).all()

def insert_subjects(teachers):
    subjects = []
    for teacher in teachers:
        for _ in range(2):
            subjects.append(Subject(name=fake.word(), teacher_id=teacher.id))
    session.add_all(subjects)
    session.commit()
    return session.query(Subject).all()

def insert_students(groups):
    students = []
    for group in groups:
        for _ in range(NUMBER_OF_STUDENTS // NUMBER_OF_GROUPS):
            students.append(Student(fullname=fake.name(), group_id=group.id))
    session.add_all(students)
    session.commit()
    return session.query(Student).all()

def insert_grades(students, subjects):
    grades = []
    for student in students:
        for subject in subjects:
            for _ in range(GRADES_PER_SUBJECT):
                grades.append(Grade(
                    student_id=student.id,
                    subjects_id=subject.id,
                    grade=random.randint(0, 100),
                    grade_date=fake.date_between(start_date=datetime.date(2025, 1, 1), end_date=datetime.date.today())
                ))
    session.add_all(grades)
    session.commit()

if __name__ == '__main__':
    try:
        groups = insert_groups()
        teachers = insert_teachers()
        subjects = insert_subjects(teachers)
        students = insert_students(groups)
        insert_grades(students, subjects)
    except SQLAlchemyError as e:
        print(e)
        session.rollback()
    finally:
        session.close()
