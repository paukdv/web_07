from sqlalchemy import func, desc, select, and_
from prettytable import PrettyTable

from database.models import Teacher, Student, Subject, Grade, Group
from database.db import session


def select_1():
    """
    Знайти 5 студентів із найбільшим середнім балом з усіх предметів.
    :return: list[dict]
    """
    result = session.query(Student.student_name, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade).join(Student).group_by(Student.id).order_by(desc('avg_grade')).limit(5).all()
    return result


def select_2(subject_id: int):
    """
    Знайти студента із найвищим середнім балом з певного предмета
    :return: list{dict}
    """
    result = session.query(Subject.subject_name,
                           Student.student_name,
                           func.round(func.avg(Grade.grade), 2).label('avg_grade')
                           ) \
        .select_from(Grade) \
        .join(Student) \
        .join(Subject) \
        .filter(Subject.id == subject_id) \
        .group_by(Student.id, Subject.subject_name) \
        .order_by(desc('avg_grade')) \
        .limit(1).all()
    return result


def select_3(subject_id: int):
    """
    Знайти середній бал у групах з певного предмета
    :return: list{dict}
    """
    result = session.query(Subject.subject_name,
                           Group.group_name,
                           func.round(func.avg(Grade.grade), 2).label('avg_grade')
                           ) \
        .select_from(Student) \
        .join(Grade) \
        .join(Group) \
        .join(Subject) \
        .filter(Subject.id == subject_id) \
        .group_by(Group.group_name, Subject.subject_name) \
        .order_by(desc('avg_grade')).all()
    return result


def select_4():
    """
    Знайти середній бал на потоці (по всій таблиці оцінок).
    :return: list{dict}
    """
    result = session.query(func.round(func.avg(Grade.grade), 2).label('avg_grade')).scalar()

    return result


def select_5(teacher_id: int):
    """
    Знайти, які курси читає певний викладач.
    :return: list{dict}
    """
    result = session.query(Subject.subject_name,
                           Teacher.teacher_name) \
        .select_from(Teacher) \
        .join(Subject) \
        .filter(Teacher.id == teacher_id) \
        .group_by(Teacher.teacher_name, Subject.subject_name).all()
    return result


def select_6(group_id: int):
    """
    Знайти список студентів у певній групі.
    :return: list{dict}
    """
    result = session.query(Group.group_name,
                           Student.student_name) \
        .select_from(Group) \
        .join(Student) \
        .filter(Group.id == group_id) \
        .group_by(Group.group_name, Student.student_name).all()
    return result


def select_7(subject_id: int):
    """
    Знайти оцінки студентів в окремій групі з певного предмета.
    :return: list{dict}
    """
    query = session.query(Group.group_name,
                          Student.student_name,
                          Grade.grade, Grade.date_grade) \
        .select_from(Grade) \
        .join(Student) \
        .join(Group) \
        .join(Subject) \
        .filter(Subject.id == subject_id) \
        .group_by(Group.id, Student.student_name, Grade.grade, Grade.date_grade) \
        .order_by(Group.id)

    result = query.all()

    table = PrettyTable()
    table.field_names = ["Group", "Student", "Grade", "Date"]

    for row in result:
        table.add_row([row.group_name, row.student_name, row.grade, row.date_grade])
    print(table)


def select_8(teacher_id: int):
    """
    Знайти середній бал, який ставить певний викладач зі своїх предметів.
    :return: list{dict}
    """
    query = session.query(Teacher.teacher_name,
                          Subject.subject_name,
                          func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade) \
        .join(Subject) \
        .join(Teacher) \
        .filter(Teacher.id == teacher_id) \
        .group_by(Teacher.teacher_name, Subject.subject_name) \
        .order_by(desc('avg_grade'))

    result = query.all()

    table = PrettyTable()
    table.field_names = ["Teacher", "Subject", "Avg_Grade"]

    for row in result:
        table.add_row([row.teacher_name, row.subject_name, row.avg_grade])
    print(table)


def select_9(student_id: int):
    """
    Знайти список курсів, які відвідує певний студент.
    :return: list{dict}
    """
    query = session.query(Student.student_name,
                          Subject.subject_name) \
        .select_from(Grade) \
        .join(Subject) \
        .join(Student) \
        .filter(Student.id == student_id) \
        .group_by(Student.student_name, Subject.subject_name) \

    result = query.all()

    table = PrettyTable()
    table.field_names = ["Student", "Subject"]

    for row in result:
        table.add_row([row.student_name, row.subject_name])
    print(table)


def select_10(student_id: int, teacher_id: int):
    """
    Список курсів, які певному студенту читає певний викладач.
    :return: list{dict}
    """
    query = session.query(Subject.subject_name,
                          Student.student_name,
                          Teacher.teacher_name) \
        .select_from(Grade) \
        .join(Subject) \
        .join(Student) \
        .filter(and_(Student.id == student_id, Teacher.id == teacher_id)) \
        .group_by(Subject.subject_name, Student.student_name, Teacher.teacher_name) \

    result = query.all()

    table = PrettyTable()
    table.field_names = ["Subject", "Student", "Teacher"]

    for row in result:
        table.add_row([row.subject_name, row.student_name, row.teacher_name])
    print(table)


def select_11(student_id: int, teacher_id: int):
    """
    Середній бал, який певний викладач ставить певному студентові.
    :return: list{dict}
    """
    query = session.query(Teacher.teacher_name,
                          Student.student_name,
                          func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade) \
        .join(Subject) \
        .join(Student) \
        .filter(and_(Student.id == student_id, Teacher.id == teacher_id)) \
        .group_by(Teacher.teacher_name, Student.student_name, Grade.grade) \
        .order_by(desc('avg_grade'))

    result = query.all()

    table = PrettyTable()
    table.field_names = ["Teacher", "Student", "Avg_Grade"]

    for row in result:
        table.add_row([row.teacher_name, row.student_name, row.avg_grade])
    print(table)


def select_12(subject_id: int, group_id: int):
    """
    Оцінки студентів у певній групі з певного предмета на останньому занятті.
    :return: list{dict}
    """
    subquery = (select(Grade.date_grade)
                .join(Student)
                .join(Group)
                .where(and_(Grade.subject_id == subject_id, Group.id == group_id))) \
                .order_by(desc(Grade.date_grade)).limit(1).scalar_subquery()

    query = session.query(Subject.subject_name,
                        Student.student_name,
                        Group.group_name,
                        Grade.grade
                        )\
            .select_from(Grade) \
            .join(Student) \
            .join(Subject) \
            .join(Group)\
            .filter(and_(Subject.id == subject_id, Group.id == group_id, Grade.date_grade == subquery)) \
            .order_by(desc(Grade.date_grade))

    result = query.all()

    table = PrettyTable()
    table.field_names = ["Subject", "Student", "Group", "Grade"]

    for row in result:
        table.add_row([row.subject_name, row.student_name, row.group_name, row.grade])
    print(table)

if __name__ == '__main__':
    select_12(1, 2)
