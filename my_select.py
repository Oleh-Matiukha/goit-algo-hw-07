from sqlalchemy import func, desc, select, and_

from conf.models import Grade, Teacher, Student, Group, Subject
from conf.db import session


def select_01():
    """
    SELECT
        s.id,
        s.fullname,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM students s
    JOIN grades g ON s.id = g.student_id
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 5;
    """
    result = session.query(
        Student.id,
        Student.fullname,
        func.round(func.avg(Grade.grade), 2).label('average_grade')
    ) \
    .select_from(Student) \
    .join(Grade) \
    .group_by(Student.id) \
    .order_by(desc('average_grade')) \
    .limit(5) \
    .all()
    return result

def select_02(subject_id: int):
    """
    SELECT
        s.id,
        s.fullname,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    JOIN students s ON s.id = g.student_id
    where g.subject_id = :subject_id
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 1;
    """
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).join(Student).filter(Grade.subject_id == subject_id).group_by(Student.id).order_by(
        desc('average_grade')).limit(1).all()
    return result

def select_03(subject_id: int):
    """
    SELECT
        s.group_id,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    JOIN students s ON g.student_id = s.id
    WHERE g.subject_id = :subject_id
    GROUP BY s.group_id;
    """
    result = session.query(Student.group_id, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .join(Grade, Grade.student_id == Student.id) \
        .filter(Grade.subject_id == subject_id) \
        .group_by(Student.group_id) \
        .all()
    return result

def select_04():
    """
    SELECT
        ROUND(AVG(grade), 2) AS average_grade
    FROM grades;
    """
    result = session.query(func.round(func.avg(Grade.grade), 2).label('average_grade')).all()
    return result

def select_05(teacher_id: int):
    """
    SELECT sub.name
    FROM subjects sub
    JOIN teachers t ON sub.teacher_id = t.id
    WHERE t.id = :teacher_id;
    """
    result = session.query(Subject.name) \
        .join(Teacher, Teacher.id == Subject.teacher_id) \
        .filter(Teacher.id == teacher_id) \
        .all()
    return result

def select_06(group_id: int):
    """
    SELECT s.id, s.fullname
    FROM students s
    WHERE s.group_id = :group_id;
    """
    result = session.query(Student.id, Student.fullname) \
        .filter(Student.group_id == group_id) \
        .all()
    return result

def select_07(group_id: int, subject_id: int):
    """
    SELECT s.id, s.fullname, gr.grade
    FROM grades gr
    JOIN students s ON gr.student_id = s.id
    WHERE s.group_id = :group_id
    AND gr.subject_id = :subject_id;
    """
    result = session.query(Student.id, Student.fullname, Grade.grade) \
        .join(Grade, Grade.student_id == Student.id) \
        .filter(Student.group_id == group_id, Grade.subject_id == subject_id) \
        .all()
    return result

def select_08(teacher_id:int):
    """
    SELECT t.id, t.fullname, ROUND(AVG(gr.grade), 2) AS average_grade
    FROM grades gr
    JOIN subjects sub ON gr.subject_id = sub.id
    JOIN teachers t ON sub.teacher_id = t.id
    WHERE t.id = :teacher_id
    GROUP BY t.id;
    """
    result = session.query(Teacher.id, Teacher.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .join(Subject, Subject.id == Grade.subject_id) \
        .join(Teacher, Teacher.id == Subject.teacher_id) \
        .filter(Teacher.id == teacher_id) \
        .group_by(Teacher.id) \
        .all()
    return result

def select_09(student_id:int):
    """
    SELECT DISTINCT s.fullname, sub.id AS subject_id, sub.name AS subject_name
    FROM subjects sub
    JOIN grades gr ON sub.id = gr.subject_id
    JOIN students s ON gr.student_id = s.id
    WHERE s.id = :student_id;
    """
    result = session.query(Student.fullname, Subject.id.label('subject_id'), Subject.name.label('subject_name')) \
        .join(Grade, Grade.student_id == Student.id) \
        .join(Subject, Subject.id == Grade.subject_id) \
        .filter(Student.id == student_id) \
        .distinct() \
        .all()
    return result

def select_10(student_id:int, teacher_id: int):
    """
    SELECT DISTINCT sub.id AS subject_id, sub.name AS subject_name
    FROM subjects sub
    JOIN grades gr ON sub.id = gr.subject_id
    JOIN students s ON gr.student_id = s.id
    WHERE s.id = :student_id
    AND sub.teacher_id = :teacher_id;
    """
    result = session.query(Subject.id.label('subject_id'), Subject.name.label('subject_name')) \
        .join(Grade, Grade.subject_id == Subject.id) \
        .join(Student, Student.id == Grade.student_id) \
        .filter(Student.id == student_id, Subject.teacher_id == teacher_id) \
        .distinct() \
        .all()
    return result

def select_11(student_id: int, teacher_id:int):
    """
    SELECT s.fullname, ROUND(AVG(gr.grade), 2) AS average_grade
    FROM grades gr
    JOIN subjects sub ON gr.subject_id = sub.id
    JOIN students s ON gr.student_id = s.id
    WHERE s.id = :student_id
    AND sub.teacher_id = :teacher_id
    GROUP BY s.id;
    """
    result = session.query(
        Student.fullname,
        func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .join(Grade, Grade.student_id == Student.id) \
        .join(Subject, Subject.id == Grade.subject_id) \
        .filter(Student.id == student_id, Subject.teacher_id == teacher_id) \
        .group_by(Student.id) \
        .all()
    return result

def select_12(group_id: int, subject_id: int):
    """
    WITH LastGrades AS (
        SELECT student_id, subject_id, MAX(grade_date) AS last_grade_date
        FROM grades
        GROUP BY student_id, subject_id
    )
    SELECT s.fullname, gr.grade, gr.grade_date
    FROM grades gr
    JOIN students s ON gr.student_id = s.id
    JOIN subjects sub ON gr.subject_id = sub.id
    JOIN LastGrades lg ON gr.student_id = lg.student_id
                    AND gr.subject_id = lg.subject_id
                    AND gr.grade_date = lg.last_grade_date
    WHERE s.group_id = :group_id
    AND sub.id = :subject_id;
    """
    # Підзапит для отримання останньої дати оцінки для кожного студента та предмета
    last_grades_subquery = select(Grade.student_id, Grade.subject_id,
                                  func.max(Grade.grade_date).label('last_grade_date')
                                  ).group_by(Grade.student_id, Grade.subject_id).subquery()
    # Основний запит
    result = session.query(Student.fullname, Grade.grade, Grade.grade_date
    ).join(Grade, Grade.student_id == Student.id) \
     .join(Subject, Subject.id == Grade.subject_id) \
     .join(last_grades_subquery,
           (Grade.student_id == last_grades_subquery.c.student_id) &
           (Grade.subject_id == last_grades_subquery.c.subject_id) &
           (Grade.grade_date == last_grades_subquery.c.last_grade_date)
    ).filter(
        Student.group_id == group_id,
        Subject.id == subject_id
    ).all()
    return result


if __name__ == '__main__':
    print(select_01())  # 5 студентів із найбільшим середнім балом з усіх предметів.
    print(select_02(1))  # Студент із найвищим середнім балом з предмета з ID = 1
    print(select_03(1))  # Середній бал на потоці по предмету з ID = 1
    print(select_04())  # Середній бал на потоці (по всій таблиці оцінок).
    print(select_05(1))  # Перелік курсів, які читає викладач з ID = 1
    print(select_06(1))  # Перелік студентів групи з ID = 1
    print(select_07(1, 1))  # Оцінки студентів у групі з ID = 1 з предмета з ID = 1
    print(select_08(1))  # Середній бал викладача з ID = 1 зі своїх предметів.
    print(select_09(1))  # Перелік курсів, які відвідує студент з ID = 1
    print(select_10(1, 1))  # Перелік курсів, які викладає викладач з ID = 1 студенту з ID = 1
    print(select_11(1, 1))  # Середній бал для студента з ID = 1 від викладача з ID = 1
    print(select_12(1, 1))  # Оцінки студентів в групі з ID = 1 по предмету з ID = 1 на останньому занятті