DO $$ BEGIN
  CREATE TYPE account_role AS ENUM ('STUDENT', 'TEACHER', 'ADMIN');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
  CREATE TYPE pay_status AS ENUM ('PAID', 'CANCELED', 'REFUND');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
  CREATE TYPE ack_status AS ENUM ('SEEN', 'APPROVED', 'REJECTED');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
  CREATE TYPE exam_pay_status AS ENUM ('NOT_REQUIRED', 'UNPAID', 'PAID');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

--E14 учетная запись 
CREATE TABLE IF NOT EXISTS account (
    user_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    login            VARCHAR(50)  NOT NULL UNIQUE,
    password_hash    VARCHAR(255) NOT NULL,
    role             account_role NOT NULL,
    is_active        BOOLEAN NOT NULL DEFAULT TRUE,
    created_at       TIMESTAMP NOT NULL DEFAULT NOW(),
    last_login_at    TIMESTAMP
);

--E1 преподаватель
CREATE TABLE IF NOT EXISTS teacher (
    user_id          BIGINT PRIMARY KEY
        REFERENCES account(user_id) ON DELETE RESTRICT,
    fio               VARCHAR(150) NOT NULL,
    work_start_date  DATE NOT NULL CHECK (work_start_date <= CURRENT_DATE),
    qualification     VARCHAR(60) NOT NULL,
    interests        TEXT,
    email            VARCHAR(254) UNIQUE,
    phone            VARCHAR(30)
);


--E16 Сотрудник администрации
CREATE TABLE IF NOT EXISTS admin_staff(
    user_id     BIGINT PRIMARY KEY
        REFERENCES account(user_id) ON DELETE RESTRICT,
    fio          VARCHAR(150) NOT NULL,
    position    VARCHAR(80) NOT NULL,
    phone       VARCHAR(30),
    email       VARCHAR(254) UNIQUE
);

--E13 Журнал действий
CREATE TABLE IF NOT EXISTS audit_log(
    log_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id     BIGINT NOT NULL
        REFERENCES account(user_id) ON DELETE RESTRICT,
    action      VARCHAR(50) NOT NULL,
    details     TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

--E4 Аудитория
CREATE TABLE IF NOT EXISTS classroom(
    classroom_id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    room_number     VARCHAR(20) NOT NULL UNIQUE,
    capacity        SMALLINT NOT NULL CHECK (capacity > 0)
);

--E3 КУРСЫ
CREATE TABLE IF NOT EXISTS course(
    course_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name        VARCHAR(150) NOT NULL UNIQUE,
    direction   VARCHAR(60)  NOT NULL,
    hours       SMALLINT NOT NULL CHECK (hours > 0)
);
--E21 (E3-E3) пререквизиты курса
CREATE TABLE IF NOT EXISTS course_prereq (
    course_id        BIGINT NOT NULL REFERENCES course(course_id) ON DELETE CASCADE,
    prereq_course_id BIGINT NOT NULL REFERENCES course(course_id) ON DELETE CASCADE,
    CONSTRAINT pk_course_prereq PRIMARY KEY (course_id, prereq_course_id),
    CHECK (course_id <> prereq_course_id)
);

--E18 ПРЕПОДАВАЕМЫЙ КУРС
CREATE TABLE IF NOT EXISTS teacher_course (
    teacher_id       BIGINT NOT NULL REFERENCES teacher(user_id) ON DELETE RESTRICT,
    course_id        BIGINT NOT NULL REFERENCES course(course_id) ON DELETE RESTRICT,
    tuition_price    NUMERIC(10,2) NOT NULL CHECK (tuition_price >= 0),
    note             TEXT,
    CONSTRAINT pk_teacher_course PRIMARY KEY (teacher_id, course_id)
);

--E6 Междисцип Курс
CREATE TABLE IF NOT EXISTS program (
    program_id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name                  VARCHAR(120) NOT NULL UNIQUE,
    direction             VARCHAR(60) NOT NULL,
    price                 NUMERIC(10,2) NOT NULL CHECK (price >= 0)
);

--E19 Состав программы
CREATE TABLE IF NOT EXISTS program_course (
    program_id  BIGINT NOT NULL REFERENCES program(program_id) ON DELETE CASCADE,
    course_id   BIGINT NOT NULL REFERENCES course(course_id)  ON DELETE RESTRICT,
    seq_no      SMALLINT NOT NULL CHECK (seq_no > 0),
    CONSTRAINT pk_program_course PRIMARY KEY (program_id, course_id),
    CONSTRAINT uq_program_seq UNIQUE (program_id, seq_no)
);

-- E5 Группа
CREATE TABLE IF NOT EXISTS study_group (
    group_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name          VARCHAR(50) NOT NULL UNIQUE,
    academic_year SMALLINT NOT NULL,
    semester      SMALLINT NOT NULL CHECK (semester IN (1,2)),
    capacity      SMALLINT NOT NULL DEFAULT 10 CHECK (capacity BETWEEN 1 AND 10),

    course_id     BIGINT NOT NULL REFERENCES course(course_id) ON DELETE RESTRICT,
    program_id    BIGINT REFERENCES program(program_id) ON DELETE RESTRICT,

    curator_staff_id    BIGINT REFERENCES admin_staff(user_id) ON DELETE RESTRICT,
    curator_teacher_id BIGINT REFERENCES teacher(user_id) ON DELETE RESTRICT,

    -- куратор либо сотрудник, либо преподаватель
    CONSTRAINT chk_curator_xor CHECK (
      (curator_staff_id IS NULL) <> (curator_teacher_id IS NULL)
    ),

    -- преподаватель может курировать только одну группу
    CONSTRAINT uq_teacher_curator UNIQUE (curator_teacher_id)
);


--E2 СЛУШАТЕЛЬ
CREATE TABLE IF NOT EXISTS student(
    user_id     BIGINT PRIMARY KEY
        REFERENCES account(user_id) ON DELETE RESTRICT,
    fio          VARCHAR(150) NOT NULL,
    group_id    BIGINT NOT NULL REFERENCES study_group(group_id) ON DELETE RESTRICT,
    phone       VARCHAR(30),
    email       VARCHAR(254)
);

-- E7 Оплата (только обучение): E2 + E18
CREATE TABLE IF NOT EXISTS payment (
  payment_id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  student_id        BIGINT NOT NULL REFERENCES student(user_id) ON DELETE RESTRICT,

  teacher_id        BIGINT NOT NULL,
  course_id         BIGINT NOT NULL,
  FOREIGN KEY (teacher_id, course_id)
    REFERENCES teacher_course(teacher_id, course_id)
    ON DELETE RESTRICT,

  price             NUMERIC(10,2) NOT NULL CHECK (price > 0),
  paid_at           TIMESTAMP NOT NULL DEFAULT NOW(),
  status            pay_status NOT NULL DEFAULT 'PAID',

  accepted_by_staff_user_id BIGINT REFERENCES admin_staff(user_id) ON DELETE RESTRICT
);

-- E9 Расписание
CREATE TABLE IF NOT EXISTS schedule_version (
  schedule_version_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  academic_year       SMALLINT NOT NULL,
  semester            SMALLINT NOT NULL CHECK (semester IN (1,2)),
  version_no          SMALLINT NOT NULL CHECK (version_no > 0),
  is_active           BOOLEAN NOT NULL DEFAULT FALSE,
  created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
  UNIQUE (academic_year, semester, version_no)
);

-- проверка на одну активную версию
CREATE UNIQUE INDEX IF NOT EXISTS uq_active_schedule
ON schedule_version(academic_year, semester)
WHERE is_active = TRUE;

CREATE TABLE IF NOT EXISTS schedule_item (
  schedule_item_id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  schedule_version_id  BIGINT NOT NULL REFERENCES schedule_version(schedule_version_id) ON DELETE CASCADE,

  day_of_week          SMALLINT NOT NULL CHECK (day_of_week BETWEEN 1 AND 7),
  pair_no              SMALLINT NOT NULL CHECK (pair_no > 0),

  group_id             BIGINT NOT NULL REFERENCES study_group(group_id) ON DELETE RESTRICT,
  teacher_id      BIGINT NOT NULL REFERENCES teacher(user_id)     ON DELETE RESTRICT,
  course_id            BIGINT NOT NULL REFERENCES course(course_id)    ON DELETE RESTRICT,
  classroom_id         BIGINT NOT NULL REFERENCES classroom(classroom_id) ON DELETE RESTRICT,

  -- ensure teacher can teach this course (consistent with E18)
  FOREIGN KEY (teacher_id, course_id)
    REFERENCES teacher_course(teacher_id, course_id)
    ON DELETE RESTRICT
);

-- ограничения 
CREATE UNIQUE INDEX IF NOT EXISTS uq_schedule_group_time
  ON schedule_item(schedule_version_id, group_id, day_of_week, pair_no);

CREATE UNIQUE INDEX IF NOT EXISTS uq_schedule_teacher_time
  ON schedule_item(schedule_version_id, teacher_id, day_of_week, pair_no);

CREATE UNIQUE INDEX IF NOT EXISTS uq_schedule_room_time
  ON schedule_item(schedule_version_id, classroom_id, day_of_week, pair_no);

-- E10 Рабочие дни преподавателя 
CREATE TABLE IF NOT EXISTS teacher_workslot (
  workslot_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  teacher_id  BIGINT NOT NULL REFERENCES teacher(user_id) ON DELETE RESTRICT,
  academic_year    SMALLINT NOT NULL,
  semester         SMALLINT NOT NULL CHECK (semester IN (1,2)),
  day_of_week      SMALLINT NOT NULL CHECK (day_of_week BETWEEN 1 AND 7),
  pair_no          SMALLINT NOT NULL CHECK (pair_no > 0),
  UNIQUE (teacher_id, academic_year, semester, day_of_week, pair_no)
);

-- E12 Ознакомление с расписанием
CREATE TABLE IF NOT EXISTS schedule_ack (
  schedule_version_id BIGINT NOT NULL REFERENCES schedule_version(schedule_version_id) ON DELETE CASCADE,
  teacher_id     BIGINT NOT NULL REFERENCES teacher(user_id) ON DELETE RESTRICT,
  status              ack_status NOT NULL,
  PRIMARY KEY (schedule_version_id, teacher_id)
);

-- E15 Вступительный экзамен 
CREATE TABLE IF NOT EXISTS entrance_exam (
  entrance_exam_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  student_user_id    BIGINT NOT NULL REFERENCES student(user_id) ON DELETE RESTRICT,
  course_id          BIGINT NOT NULL REFERENCES course(course_id) ON DELETE RESTRICT,

  schedule_item_id   BIGINT REFERENCES schedule_item(schedule_item_id) ON DELETE SET NULL,

  taken_at           TIMESTAMP NOT NULL DEFAULT NOW(),
  grade_value        SMALLINT,
  passed             BOOLEAN NOT NULL DEFAULT FALSE,
  admitted           BOOLEAN NOT NULL DEFAULT FALSE,

  fee                NUMERIC(10,2) NOT NULL CHECK (fee >= 0),
  pay_status         exam_pay_status NOT NULL DEFAULT 'UNPAID',
  paid_at            TIMESTAMP,

  UNIQUE (student_user_id, course_id),
  CHECK ( (NOT admitted) OR passed )
);

-- E8 Оценка за экзамен
CREATE TABLE IF NOT EXISTS exam_grade (
  grade_id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  student_user_id     BIGINT NOT NULL REFERENCES student(user_id) ON DELETE RESTRICT,
  course_id           BIGINT NOT NULL REFERENCES course(course_id) ON DELETE RESTRICT,
  examiner_teacher_id BIGINT NOT NULL REFERENCES teacher(user_id) ON DELETE RESTRICT,
  exam_date           TIMESTAMP NOT NULL DEFAULT NOW(),
  grade_value         SMALLINT NOT NULL,
  passed              BOOLEAN NOT NULL DEFAULT FALSE
);

-- E11 Пересдача
CREATE TABLE IF NOT EXISTS retake (
  retake_id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

  grade_id            BIGINT NOT NULL UNIQUE REFERENCES exam_grade(grade_id) ON DELETE RESTRICT,

  student_user_id     BIGINT NOT NULL REFERENCES student(user_id) ON DELETE RESTRICT,
  course_id           BIGINT NOT NULL REFERENCES course(course_id) ON DELETE RESTRICT,
  examiner_teacher_id BIGINT NOT NULL REFERENCES teacher(user_id) ON DELETE RESTRICT,

  taken_at            TIMESTAMP NOT NULL DEFAULT NOW(),
  attempt_no          SMALLINT NOT NULL CHECK (attempt_no > 0),
  is_free             BOOLEAN NOT NULL DEFAULT TRUE,

  fee                 NUMERIC(10,2) NOT NULL CHECK (fee >= 0),
  pay_status          exam_pay_status NOT NULL DEFAULT 'NOT_REQUIRED',
  paid_at             TIMESTAMP
);

-- E17 Сертификат
CREATE TABLE IF NOT EXISTS certificate (
  certificate_id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  certificate_number   VARCHAR(30) NOT NULL UNIQUE,
  issued_at           TIMESTAMP NOT NULL DEFAULT NOW(),

  student_user_id     BIGINT NOT NULL REFERENCES student(user_id) ON DELETE RESTRICT,

  course_id           BIGINT REFERENCES course(course_id) ON DELETE RESTRICT,
  program_id          BIGINT REFERENCES program(program_id) ON DELETE RESTRICT,

  grade_id            BIGINT REFERENCES exam_grade(grade_id) ON DELETE SET NULL,

  CHECK ( (course_id IS NULL) <> (program_id IS NULL) ) -- XOR
);

-- Е20 преподаватели программы
CREATE TABLE IF NOT EXISTS program_teacher (
    program_id  BIGINT NOT NULL REFERENCES program(program_id) ON DELETE CASCADE,
    teacher_id  BIGINT NOT NULL REFERENCES teacher(user_id)  ON DELETE RESTRICT,
    price       NUMERIC(10,2) NOT NULL CHECK (price >= 0),
    CONSTRAINT pk_program_teacher PRIMARY KEY (program_id, teacher_id)
);