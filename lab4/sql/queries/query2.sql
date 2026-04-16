WITH active_ver AS (
  SELECT schedule_version_id, academic_year, semester
  FROM schedule_version
  WHERE is_active = TRUE
  ORDER BY academic_year DESC, semester DESC
  LIMIT 1
),
t_lessons AS (
  SELECT si.teacher_id, si.course_id, si.group_id, si.day_of_week
  FROM schedule_item si
  JOIN active_ver av ON av.schedule_version_id = si.schedule_version_id
),

t_courses AS (
  SELECT teacher_id, COUNT(DISTINCT course_id) AS courses_now
  FROM t_lessons
  GROUP BY teacher_id
),

t_students AS (
  SELECT tl.teacher_id, COUNT(DISTINCT e.student_id) AS students
  FROM (SELECT DISTINCT teacher_id, group_id FROM t_lessons) tl
  JOIN active_ver av ON true
  JOIN study_group g ON g.group_id = tl.group_id AND g.academic_year = av.academic_year AND g.semester = av.semester
  JOIN student_group_enrollment e ON e.group_id = g.group_id AND e.end_at IS NULL
  GROUP BY tl.teacher_id
),

t_day_load AS (
  SELECT teacher_id, day_of_week, COUNT(*) AS lessons_cnt
  FROM t_lessons
  GROUP BY teacher_id, day_of_week
),

t_busiest AS (
  SELECT DISTINCT ON (teacher_id) teacher_id, day_of_week AS busiest_day, lessons_cnt AS busiest_day_lessons
  FROM t_day_load
  ORDER BY teacher_id, lessons_cnt DESC, day_of_week
),

t_least AS (
  SELECT DISTINCT ON (teacher_id) teacher_id, day_of_week AS least_loaded_day, lessons_cnt AS least_loaded_day_lessons
  FROM t_day_load
  ORDER BY teacher_id, lessons_cnt ASC, day_of_week
),

t_workdays AS (
  SELECT tw.teacher_id, COUNT(DISTINCT tw.day_of_week) AS working_days
  FROM teacher_workslot tw
  JOIN active_ver av ON av.academic_year = tw.academic_year AND av.semester = tw.semester
  GROUP BY tw.teacher_id
),

t_is_curator AS (
  SELECT t.user_id AS teacher_id,
    EXISTS (
      SELECT 1
      FROM study_group g
      JOIN active_ver av ON av.academic_year = g.academic_year AND av.semester = g.semester
      WHERE g.curator_teacher_id = t.user_id
    ) AS is_curator
  FROM teacher t
)

SELECT t.user_id AS "Teacher ID", t.fio AS "ФИО", t.qualification AS "Квалификация",
  COALESCE(tc.courses_now, 0) AS "Сколько курсов сейчас ведёт",
  COALESCE(ts.students, 0)    AS "Количество студентов в этих курсах",
  COALESCE(tw.working_days, 0) AS "Количество рабочих дней",
  COALESCE(tb.busiest_day, -1) AS "Самый загруженный день (1-7)",
  COALESCE(tb.busiest_day_lessons, 0) AS "Занятий в самый загруженный день",
  COALESCE(tl.least_loaded_day, -1) AS "Наименее загруженный день (1-7)",
  COALESCE(tl.least_loaded_day_lessons, 0) AS "Занятий в наименее загруженный день",
  CASE WHEN tic.is_curator THEN 'Да' 
       ELSE 'Нет'
  END AS "Является куратором"

FROM teacher t
LEFT JOIN t_courses tc ON tc.teacher_id = t.user_id
LEFT JOIN t_students ts ON ts.teacher_id = t.user_id
LEFT JOIN t_workdays tw ON tw.teacher_id = t.user_id
LEFT JOIN t_busiest tb ON tb.teacher_id = t.user_id
LEFT JOIN t_least tl ON tl.teacher_id = t.user_id
LEFT JOIN t_is_curator tic ON tic.teacher_id = t.user_id
ORDER BY t.user_id;