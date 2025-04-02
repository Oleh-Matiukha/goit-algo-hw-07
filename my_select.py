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
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Student).join(Grade).group_by(Student.id).order_by(desc('average_grade')).limit(5).all()
    return result


def select_02():
    """
    SELECT
        s.id,
        s.fullname,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    JOIN students s ON s.id = g.student_id
    where g.subject_id = 1
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 1;
    """


    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).join(Student).filter(Grade.subjects_id == 1).group_by(Student.id).order_by(
        desc('average_grade')).limit(1).all()
    return result


def select_03():
    """
    SELECT
        s.group_id,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    JOIN students s ON g.student_id = s.id
    WHERE g.subject_id = 1  -- Замініть 1 на потрібний ID предмета
    GROUP BY s.group_id;
    """
    result = session.query(Student.group_id, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .join(Grade, Grade.student_id == Student.id) \
        .filter(Grade.subjects_id == 1) \
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

def select_05():
    """
    SELECT sub.name
    FROM subjects sub
    JOIN teachers t ON sub.teacher_id = t.id
    WHERE t.id = 1;  -- Замініть 1 на ID викладача
    """
    result = session.query(Subject.name) \
        .join(Teacher, Teacher.id == Subject.teacher_id) \
        .filter(Teacher.id == 1) \
        .all()
    return result


def select_06():
    """
    SELECT s.id, s.fullname
    FROM students s
    WHERE s.group_id = 1; -- Замініть 1 на ID групи
    """
    result = session.query(Student.id, Student.fullname) \
        .filter(Student.group_id == 1) \
        .all()
    return result


def select_07():
    """
    SELECT s.id, s.fullname, gr.grade
    FROM grades gr
    JOIN students s ON gr.student_id = s.id
    WHERE s.group_id = 1  -- Замініть 1 на ID групи
    AND gr.subject_id = 1;  -- Замініть 1 на ID предмета
    """
    result = session.query(Student.id, Student.fullname, Grade.grade) \
        .join(Grade, Grade.student_id == Student.id) \
        .filter(Student.group_id == 1, Grade.subjects_id == 1) \
        .all()
    return result


def select_08():
    """
    SELECT t.id, t.fullname, ROUND(AVG(gr.grade), 2) AS average_grade
    FROM grades gr
    JOIN subjects sub ON gr.subject_id = sub.id
    JOIN teachers t ON sub.teacher_id = t.id
    WHERE t.id = 1  -- Замініть 1 на ID викладача
    GROUP BY t.id;
    """
    result = session.query(Teacher.id, Teacher.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .join(Subject, Subject.id == Grade.subjects_id) \
        .join(Teacher, Teacher.id == Subject.teacher_id) \
        .filter(Teacher.id == 1) \
        .group_by(Teacher.id) \
        .all()
    return result


def select_09():
    """
    SELECT DISTINCT s.fullname, sub.id AS subject_id, sub.name AS subject_name
    FROM subjects sub
    JOIN grades gr ON sub.id = gr.subject_id
    JOIN students s ON gr.student_id = s.id
    WHERE s.id = 1;  -- Замініть 1 на ID студента
    """
    result = session.query(Student.fullname, Subject.id.label('subject_id'), Subject.name.label('subject_name')) \
        .join(Grade, Grade.student_id == Student.id) \
        .join(Subject, Subject.id == Grade.subjects_id) \
        .filter(Student.id == 1) \
        .distinct() \
        .all()
    return result


def select_10():
    """
    SELECT DISTINCT sub.id AS subject_id, sub.name AS subject_name
    FROM subjects sub
    JOIN grades gr ON sub.id = gr.subject_id
    JOIN students s ON gr.student_id = s.id
    WHERE s.id = 1  -- Замініть 1 на ID студента
    AND sub.teacher_id = 1;  -- Замініть 1 на ID викладача
    """
    result = session.query(Subject.id.label('subject_id'), Subject.name.label('subject_name')) \
        .join(Grade, Grade.subjects_id == Subject.id) \
        .join(Student, Student.id == Grade.student_id) \
        .filter(Student.id == 1, Subject.teacher_id == 1) \
        .distinct() \
        .all()
    return result


def select_11():
    """
    SELECT s.fullname, ROUND(AVG(gr.grade), 2) AS average_grade
    FROM grades gr
    JOIN subjects sub ON gr.subject_id = sub.id
    JOIN students s ON gr.student_id = s.id
    WHERE s.id = 1  -- Замініть 1 на ID студента
    AND sub.teacher_id = 1  -- Замініть 1 на ID викладача
    GROUP BY s.id;
    """
    result = session.query(
        Student.fullname,
        func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .join(Grade, Grade.student_id == Student.id) \
        .join(Subject, Subject.id == Grade.subjects_id) \
        .filter(Student.id == 1, Subject.teacher_id == 1) \
        .group_by(Student.id) \
        .all()
    return result


def select_12():
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
    WHERE s.group_id = 1  -- Замініть 1 на ID групи
    AND sub.id = 1;  -- Замініть 1 на ID предмета
    """

    # Підзапит для отримання останньої дати оцінки для кожного студента та предмета
    last_grades_subquery = select(Grade.student_id, Grade.subjects_id,
                                  func.max(Grade.grade_date).label('last_grade_date')
                                  ).group_by(Grade.student_id, Grade.subjects_id).subquery()

    # Основний запит
    result = session.query(Student.fullname, Grade.grade, Grade.grade_date
    ).join(Grade, Grade.student_id == Student.id) \
     .join(Subject, Subject.id == Grade.subjects_id) \
     .join(last_grades_subquery,
           (Grade.student_id == last_grades_subquery.c.student_id) &
           (Grade.subjects_id == last_grades_subquery.c.subjects_id) &
           (Grade.grade_date == last_grades_subquery.c.last_grade_date)
    ).filter(
        Student.group_id == 1,
        Subject.id == 1
    ).all()

    return result

if __name__ == '__main__':
    print(select_12())