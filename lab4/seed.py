import os
import csv
import random
import logging
from datetime import datetime, timedelta
import re 
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
RNG = random.Random(42)



INT_LIKE = {
    "academic_year", "semester", "capacity", "hours", "seq_no",
    "day_of_week", "pair_no", "attempt_no", "grade_value",
}
BOOL_LIKE = {"is_active", "passed", "admitted", "is_free"}
FLOAT_LIKE = {"price", "fee", "tuition_price"}

def _normalize(col: str, val):
    if val is None:
        return None
    v = str(val).strip()

    if v == "" or v.lower() in {"null", "none", "nan"}:
        return None

    if col in BOOL_LIKE:
        return v.lower() in {"true", "t", "1", "yes", "y"}

    if col.endswith("_id") or col in INT_LIKE:
        if re.fullmatch(r"-?\d+(\.0+)?", v):
            v = v.split(".")[0]
        return int(v)

    if col in FLOAT_LIKE:
        return float(v)

    return v


def read_csv_rows(csv_path: str, columns: list[str]) -> list[tuple]:
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = []
        for r in reader:
            rows.append(tuple(_normalize(c, r.get(c)) for c in columns))
        return rows

def table_exists(cur, name: str) -> bool:
    cur.execute(
        """
        SELECT EXISTS (
          SELECT 1 FROM information_schema.tables
          WHERE table_schema='public' AND table_name=%s
        );
        """,
        (name,),
    )
    return cur.fetchone()[0]


def reset_db(cur):

    ordered = [
        "certificate",
        "retake",
        "exam_grade",
        "entrance_exam",
        "schedule_ack",
        "schedule_item",
        "schedule_version",
        "teacher_workslot",
        "payment",
        "student",
        "study_group",
        "program_teacher",
        "program_course",
        "program",
        "teacher_course",
        "course_prereq",
        "course",
        "classroom",
        "audit_log",
        "admin_staff",
        "teacher",
        "account",
    ]
    existing = [t for t in ordered if table_exists(cur, t)]
    if not existing:
        logging.warning("No tables found to TRUNCATE.")
        return
    cur.execute(f"TRUNCATE TABLE {', '.join(existing)} RESTART IDENTITY CASCADE;")
    logging.info("DB truncated (RESTART IDENTITY, CASCADE).")


def sync_identity(cur, table: str, id_col: str):

    cur.execute("SELECT pg_get_serial_sequence(%s,%s);", (table, id_col))
    seq = cur.fetchone()[0]
    if not seq:
        return
    cur.execute(f"SELECT COALESCE(MAX({id_col}), 1) FROM {table};")
    mx = cur.fetchone()[0]
    cur.execute("SELECT setval(%s, %s, true);", (seq, mx))
    logging.info("Synced sequence for %s.%s to %s", table, id_col, mx)


def insert_from_csv(cur, table: str, columns: list[str], csv_dir: str, overriding: bool) -> int:

    path = os.path.join(csv_dir, f"{table}.csv")
    if not os.path.exists(path):
        logging.warning("CSV not found: %s (skip %s)", path, table)
        return 0

    rows = read_csv_rows(path, columns)
    if not rows:
        logging.warning("CSV empty: %s", path)
        return 0

    cols = ", ".join(columns)
    override_sql = " OVERRIDING SYSTEM VALUE" if overriding else ""
    sql = f"INSERT INTO {table} ({cols}){override_sql} VALUES %s"

    execute_values(cur, sql, rows, page_size=5000)
    logging.info("Inserted into %s: %d rows", table, len(rows))
    return len(rows)

def insert_program_teacher_with_price(cur, csv_dir: str):
    if not table_exists(cur, "program_teacher"):
        logging.info("program_teacher table not found -> skip.")
        return

    path = os.path.join(csv_dir, "program_teacher.csv")
    if not os.path.exists(path):
        logging.info("program_teacher.csv not found -> skip.")
        return

    rows = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            if "program_id" not in r or "teacher_id" not in r:
                continue
            program_id = int(r["program_id"])
            teacher_id = int(r["teacher_id"])

            # осмысленная "ставка" преподавателя за участие в программе
            price = float(RNG.choice([3000, 4500, 6000, 7500, 9000, 11000]))
            rows.append((program_id, teacher_id, price))

    if not rows:
        logging.info("program_teacher.csv had no usable rows -> skip.")
        return

    execute_values(
        cur,
        """
        INSERT INTO program_teacher (program_id, teacher_id, price)
        VALUES %s
        ON CONFLICT DO NOTHING
        """,
        rows,
        page_size=2000,
    )
    logging.info("Inserted into program_teacher: %d rows", len(rows))



def ensure_classrooms(cur):
    cur.execute("SELECT COUNT(*) FROM classroom;")
    if cur.fetchone()[0] > 0:
        logging.info("Classroom already filled, skip.")
        return

    rooms = []
    for n in range(101, 111):
        rooms.append((str(n), 20))
    for n in range(201, 206):
        rooms.append((str(n), 15))
    for n in range(301, 304):
        rooms.append((str(n), 30))

    execute_values(
        cur,
        "INSERT INTO classroom(room_number, capacity) VALUES %s ON CONFLICT DO NOTHING;",
        rooms,
    )
    logging.info("Inserted into classroom: %d rows", len(rooms))


def create_schedule(cur, year: int, semester: int):
    cur.execute(
        """
        INSERT INTO schedule_version (academic_year, semester, version_no, is_active, created_at)
        VALUES (%s,%s,1,TRUE,NOW())
        ON CONFLICT (academic_year, semester, version_no)
        DO UPDATE SET is_active = EXCLUDED.is_active
        RETURNING schedule_version_id;
        """,
        (year, semester),
    )
    sv_id = cur.fetchone()[0]

    cur.execute("SELECT classroom_id, capacity FROM classroom ORDER BY classroom_id;")
    classrooms = cur.fetchall()

    cur.execute("SELECT group_id, course_id, capacity FROM study_group ORDER BY group_id;")
    groups = cur.fetchall()


    slots = [(d, p) for d in range(1, 8) for p in range(1, 7)]
    RNG.shuffle(slots)

    used_g, used_t, used_r = set(), set(), set()
    schedule_rows = []
    group_main_teacher = {}  

    for group_id, course_id, gcap in groups:
        cur.execute(
            "SELECT teacher_id, tuition_price FROM teacher_course WHERE course_id=%s ORDER BY random() LIMIT 20;",
            (course_id,),
        )
        candidates = cur.fetchall()
        if not candidates:
            raise RuntimeError(f"teacher_course is empty for course_id={course_id}")

        teacher_id, tuition = RNG.choice(candidates)
        group_main_teacher[group_id] = (teacher_id, course_id, float(tuition))

        for _ in range(2):
            placed = False
            for __ in range(5000):
                if not slots:
                    raise RuntimeError("Not enough schedule slots to place all lessons.")

                dow, pair = slots.pop()

                possible_rooms = [cid for (cid, cap) in classrooms if cap >= gcap]
                classroom_id = RNG.choice(possible_rooms)

                key_g = (sv_id, group_id, dow, pair)
                key_t = (sv_id, teacher_id, dow, pair)
                key_r = (sv_id, classroom_id, dow, pair)

                if key_g in used_g or key_t in used_t or key_r in used_r:
                    continue

                used_g.add(key_g)
                used_t.add(key_t)
                used_r.add(key_r)

                schedule_rows.append((sv_id, dow, pair, group_id, teacher_id, course_id, classroom_id))
                placed = True
                break

            if not placed:
                raise RuntimeError(f"Could not place lesson for group {group_id}")

    execute_values(
        cur,
        """
        INSERT INTO schedule_item
          (schedule_version_id, day_of_week, pair_no, group_id, teacher_id, course_id, classroom_id)
        VALUES %s
        ON CONFLICT DO NOTHING;
        """,
        schedule_rows,
        page_size=5000,
    )
    logging.info("Inserted schedule_item: %d rows", len(schedule_rows))
    return sv_id, group_main_teacher


def create_schedule_ack(cur, sv_id: int):
    cur.execute("SELECT DISTINCT teacher_id FROM schedule_item WHERE schedule_version_id=%s;", (sv_id,))
    teachers = [r[0] for r in cur.fetchall()]
    rows = [(sv_id, tid, "APPROVED") for tid in teachers]
    execute_values(
        cur,
        "INSERT INTO schedule_ack(schedule_version_id, teacher_id, status) VALUES %s ON CONFLICT DO NOTHING;",
        rows,
    )
    logging.info("Inserted schedule_ack: %d rows", len(rows))


def create_teacher_workslots(cur, year: int, semester: int, sv_id: int):
    cur.execute(
        """
        SELECT DISTINCT teacher_id, day_of_week, pair_no
        FROM schedule_item
        WHERE schedule_version_id=%s;
        """,
        (sv_id,),
    )
    base = cur.fetchall()

    rows = [(t, year, semester, dow, pair) for (t, dow, pair) in base]

    cur.execute("SELECT user_id FROM teacher ORDER BY user_id;")
    teachers = [r[0] for r in cur.fetchall()]
    all_slots = [(d, p) for d in range(1, 8) for p in range(1, 7)]
    for t in teachers:
        extra = RNG.sample(all_slots, k=2)
        for dow, pair in extra:
            rows.append((t, year, semester, dow, pair))

    execute_values(
        cur,
        """
        INSERT INTO teacher_workslot(teacher_id, academic_year, semester, day_of_week, pair_no)
        VALUES %s
        ON CONFLICT DO NOTHING;
        """,
        rows,
        page_size=10000,
    )
    logging.info("Inserted teacher_workslot: %d rows", len(rows))


def create_payments(cur, group_main_teacher: dict):
    cur.execute("SELECT user_id, group_id FROM student ORDER BY user_id;")
    students = cur.fetchall()

    cur.execute("SELECT user_id FROM admin_staff ORDER BY user_id;")
    staff = [r[0] for r in cur.fetchall()] or [None]

    rows = []
    for user_id, group_id in students:
        teacher_id, course_id, price = group_main_teacher[group_id]
        accepted_by = staff[(user_id + group_id) % len(staff)]
        paid_at = datetime.now() - timedelta(days=(user_id % 30))
        rows.append((user_id, teacher_id, course_id, price, paid_at, "PAID", accepted_by))

    execute_values(
        cur,
        """
        INSERT INTO payment(student_id, teacher_id, course_id, price, paid_at, status, accepted_by_staff_user_id)
        VALUES %s;
        """,
        rows,
        page_size=10000,
    )
    logging.info("Inserted payment: %d rows", len(rows))


def create_entrance_exams(cur, count: int = 40):
    cur.execute("SELECT schedule_item_id FROM schedule_item ORDER BY schedule_item_id LIMIT 1;")
    one_slot = cur.fetchone()
    schedule_item_id = one_slot[0] if one_slot else None

    cur.execute("SELECT user_id, group_id FROM student ORDER BY user_id LIMIT %s;", (count,))
    students = cur.fetchall()

    cur.execute("SELECT group_id, course_id FROM study_group;")
    group_course = {gid: cid for gid, cid in cur.fetchall()}

    cur.execute("SELECT course_id FROM course ORDER BY course_id;")
    all_courses = [r[0] for r in cur.fetchall()]

    rows = []
    for user_id, group_id in students:
        current_course = group_course[group_id]
        other_courses = [c for c in all_courses if c != current_course] or [current_course]
        course_id = other_courses[(user_id + group_id) % len(other_courses)]
        grade = 40 + (user_id % 61)
        passed = grade >= 60
        admitted = passed
        fee = 700.0
        taken_at = datetime.now() - timedelta(days=(user_id % 60))
        rows.append((user_id, course_id, schedule_item_id, taken_at, grade, passed, admitted, fee, "PAID", taken_at))

    execute_values(
        cur,
        """
        INSERT INTO entrance_exam
          (student_user_id, course_id, schedule_item_id, taken_at, grade_value, passed, admitted, fee, pay_status, paid_at)
        VALUES %s
        ON CONFLICT (student_user_id, course_id) DO NOTHING;
        """,
        rows,
        page_size=2000,
    )
    logging.info("Inserted entrance_exam: %d rows", len(rows))


def create_exam_grades_and_retakes(cur, retake_limit: int = 50):
    cur.execute("SELECT user_id, group_id FROM student ORDER BY user_id;")
    students = cur.fetchall()

    cur.execute("SELECT group_id, course_id FROM study_group;")
    group_course = {gid: cid for gid, cid in cur.fetchall()}

    cur.execute("SELECT course_id, teacher_id FROM teacher_course ORDER BY course_id, teacher_id;")
    tc = cur.fetchall()
    teachers_by_course: dict[int, list[int]] = {}
    for course_id, teacher_id in tc:
        teachers_by_course.setdefault(course_id, []).append(teacher_id)

    grade_rows = []
    for user_id, group_id in students:
        course_id = group_course[group_id]
        examiner_list = teachers_by_course.get(course_id)
        if not examiner_list:
            cur.execute("SELECT user_id FROM teacher ORDER BY random() LIMIT 1;")
            examiner = cur.fetchone()[0]
        else:
            examiner = examiner_list[user_id % len(examiner_list)]
        grade = 40 + (user_id % 61)
        passed = grade >= 60
        exam_date = datetime.now() - timedelta(days=(user_id % 20))
        grade_rows.append((user_id, course_id, examiner, exam_date, grade, passed))

    execute_values(
        cur,
        """
        INSERT INTO exam_grade(student_user_id, course_id, examiner_teacher_id, exam_date, grade_value, passed)
        VALUES %s;
        """,
        grade_rows,
        page_size=10000,
    )
    logging.info("Inserted exam_grade: %d rows", len(grade_rows))

    cur.execute(
        """
        SELECT student_user_id, course_id
        FROM exam_grade
        WHERE passed = FALSE
        ORDER BY student_user_id
        LIMIT %s;
        """,
        (retake_limit,),
    )
    failed = cur.fetchall()
    if not failed:
        logging.info("No failed grades -> no retakes.")
        return

    new_grade_rows = []
    for sid, course_id in failed:
        examiner_list = teachers_by_course.get(course_id) or []
        if examiner_list:
            examiner = examiner_list[(sid + course_id) % len(examiner_list)]
        else:
            cur.execute("SELECT user_id FROM teacher ORDER BY random() LIMIT 1;")
            examiner = cur.fetchone()[0]
        new_grade = 70 + (sid % 31)
        new_grade_rows.append((sid, course_id, examiner, datetime.now() - timedelta(days=5), new_grade, True))

    execute_values(
        cur,
        """
        INSERT INTO exam_grade(student_user_id, course_id, examiner_teacher_id, exam_date, grade_value, passed)
        VALUES %s
        RETURNING grade_id, student_user_id, course_id;
        """,
        new_grade_rows,
        page_size=2000,
    )
    new_grades = cur.fetchall()

    retake_rows = []
    for grade_id, sid, course_id in new_grades:
        examiner_list = teachers_by_course.get(course_id) or []
        if examiner_list:
            examiner = examiner_list[(sid + 3) % len(examiner_list)]
        else:
            cur.execute("SELECT user_id FROM teacher ORDER BY random() LIMIT 1;")
            examiner = cur.fetchone()[0]
        retake_rows.append(
            (grade_id, sid, course_id, examiner, datetime.now() - timedelta(days=4),
             1, True, 0.0, "NOT_REQUIRED", None)
        )

    execute_values(
        cur,
        """
        INSERT INTO retake
          (grade_id, student_user_id, course_id, examiner_teacher_id, taken_at,
           attempt_no, is_free, fee, pay_status, paid_at)
        VALUES %s;
        """,
        retake_rows,
        page_size=2000,
    )
    logging.info("Inserted retake: %d rows", len(retake_rows))


def create_certificates(cur, limit: int = 200):
    cur.execute(
        """
        SELECT grade_id, student_user_id, course_id
        FROM exam_grade
        WHERE passed = TRUE
        ORDER BY grade_id
        LIMIT %s;
        """,
        (limit,),
    )
    passed = cur.fetchall()

    rows = []
    for grade_id, sid, course_id in passed:
        cert_no = f"CERT-{grade_id:06d}"  # <= 30 symbols
        rows.append((cert_no, datetime.now(), sid, course_id, None, grade_id))

    execute_values(
        cur,
        """
        INSERT INTO certificate(certificate_number, issued_at, student_user_id, course_id, program_id, grade_id)
        VALUES %s
        ON CONFLICT (certificate_number) DO NOTHING;
        """,
        rows,
        page_size=2000,
    )
    logging.info("Inserted certificate: %d rows", len(rows))


def create_audit_logs(cur):
    cur.execute("SELECT user_id FROM admin_staff ORDER BY user_id LIMIT 1;")
    staff = cur.fetchone()
    staff_id = staff[0] if staff else None

    cur.execute("SELECT user_id FROM teacher ORDER BY user_id LIMIT 1;")
    t = cur.fetchone()
    teacher_id = t[0] if t else None

    cur.execute("SELECT user_id FROM student ORDER BY user_id LIMIT 1;")
    s = cur.fetchone()
    student_id = s[0] if s else None

    rows = []
    if staff_id:
        rows.append((staff_id, "IMPORT_CSV", "Импорт данных из CSV", datetime.now()))
        rows.append((staff_id, "PUBLISH_SCHEDULE", "Опубликована версия расписания", datetime.now()))
    if teacher_id:
        rows.append((teacher_id, "APPROVE_SCHEDULE", "Преподаватель утвердил расписание", datetime.now()))
    if student_id:
        rows.append((student_id, "PAY_TUITION", "Слушатель оплатил обучение", datetime.now()))

    execute_values(cur, "INSERT INTO audit_log(user_id, action, details, created_at) VALUES %s;", rows)
    logging.info("Inserted audit_log: %d rows", len(rows))


def main():
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--csv-dir", required=True, help="Папка с CSV (account.csv, teacher.csv, ...)")
    ap.add_argument("--reset", action="store_true", help="Очистить БД перед загрузкой")
    ap.add_argument("--year", type=int, default=2026)
    ap.add_argument("--semester", type=int, default=1)
    ap.add_argument("--dbname", default=os.getenv("PGDATABASE", "educat_center"))
    ap.add_argument("--user", default=os.getenv("PGUSER", "postgres"))
    ap.add_argument("--password", default=os.getenv("PGPASSWORD", ""))
    ap.add_argument("--host", default=os.getenv("PGHOST", "localhost"))
    ap.add_argument("--port", default=os.getenv("PGPORT", "5432"))
    args = ap.parse_args()

    try:
        conn = psycopg2.connect(
            dbname=args.dbname,
            user=args.user,
            password=args.password,
            host=args.host,
            port=args.port,
        )
        conn.autocommit = False
        cur = conn.cursor()
        logging.info("Подключение к БД установлено")
    except Exception as e:
        logging.error("Ошибка подключения к БД: %s", e)
        raise

    try:
        if args.reset:
            reset_db(cur)
            conn.commit()

        # заполняем таблицы данными из файлов
        insert_from_csv(
            cur,
            "account",
            ["user_id", "login", "password_hash", "role", "is_active", "created_at", "last_login_at"],
            args.csv_dir,
            overriding=True, 
        )
        conn.commit()

        insert_from_csv(
            cur,
            "teacher",
            ["user_id", "fio", "work_start_date", "qualification", "interests", "email", "phone"],
            args.csv_dir,
            overriding=False,
        )
        insert_from_csv(
            cur,
            "admin_staff",
            ["user_id", "fio", "position", "phone", "email"],
            args.csv_dir,
            overriding=False,
        )
        conn.commit()

        insert_from_csv(
            cur,
            "course",
            ["course_id", "name", "direction", "hours"],
            args.csv_dir,
            overriding=True, 
        )
        insert_from_csv(
            cur,
            "course_prereq",
            ["course_id", "prereq_course_id"],
            args.csv_dir,
            overriding=False,
        )
        conn.commit()

        insert_from_csv(
            cur,
            "program",
            ["program_id", "name", "direction", "price"],
            args.csv_dir,
            overriding=True, 
        )
        insert_from_csv(
            cur,
            "program_course",
            ["program_id", "course_id", "seq_no"],
            args.csv_dir,
            overriding=False,
        )

        insert_program_teacher_with_price(cur, args.csv_dir)
        conn.commit()

        insert_from_csv(
            cur,
            "teacher_course",
            ["teacher_id", "course_id", "tuition_price", "note"],
            args.csv_dir,
            overriding=False,
        )
        conn.commit()

        insert_from_csv(
            cur,
            "study_group",
            ["group_id", "name", "academic_year", "semester", "capacity",
             "course_id", "program_id", "curator_staff_id", "curator_teacher_id"],
            args.csv_dir,
            overriding=True,  
        )
        insert_from_csv(
            cur,
            "student",
            ["user_id", "fio", "group_id", "phone", "email"],
            args.csv_dir,
            overriding=False,
        )
        conn.commit()

        # обновляем счетчик 
        sync_identity(cur, "account", "user_id")
        sync_identity(cur, "course", "course_id")
        sync_identity(cur, "program", "program_id")
        sync_identity(cur, "study_group", "group_id")
        conn.commit()

        # заполняем остальные на основе файловых
        ensure_classrooms(cur)
        conn.commit()

        sv_id, group_main_teacher = create_schedule(cur, args.year, args.semester)
        create_schedule_ack(cur, sv_id)
        conn.commit()

        create_teacher_workslots(cur, args.year, args.semester, sv_id)
        conn.commit()

        create_payments(cur, group_main_teacher)
        conn.commit()

        create_entrance_exams(cur, count=40)
        conn.commit()

        create_exam_grades_and_retakes(cur, retake_limit=50)
        conn.commit()

        create_certificates(cur, limit=200)
        conn.commit()

        create_audit_logs(cur)
        conn.commit()

        logging.info("База заполнена.")

    except Exception as e:
        conn.rollback()
        logging.error("Ошибка заполнения: %s", e)
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()