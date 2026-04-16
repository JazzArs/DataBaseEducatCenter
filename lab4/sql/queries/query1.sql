WITH active_term AS (
    SELECT academic_year, semester
    FROM schedule_version
    WHERE is_active = TRUE
    LIMIT 1 OFFSET 3
),
grp AS (
    SELECT g.course_id, COUNT(*) AS groups_cnt
    FROM study_group g
    JOIN active_term at ON at.academic_year = g.academic_year AND at.semester = g.semester
    GROUP BY g.course_id
),
enrolled AS (
    SELECT g.course_id, e.student_id
    FROM student_group_enrollment e
    JOIN study_group g ON g.group_id = e.group_id
    JOIN active_term at ON at.academic_year = g.academic_year AND at.semester = g.semester
    WHERE e.end_at IS NULL
),
enrolled_cnt AS (
    SELECT course_id, COUNT(DISTINCT student_id) AS enrolled_students
    FROM enrolled
    GROUP BY course_id
    HAVING COUNT(DISTINCT student_id) > 0
),
teachers_src AS (
    SELECT DISTINCT tc.course_id, tc.teacher_id
    FROM teacher_course tc
),
teacher_info AS (
    SELECT ts.course_id, STRING_AGG( DISTINCT (t.fio || ' (' || t.qualification || ')'), ', ' ORDER BY (t.fio || ' (' || t.qualification || ')')) AS teachers
    FROM teachers_src ts
    JOIN teacher t ON t.user_id = ts.teacher_id
    GROUP BY ts.course_id
),
exam_flags AS (
  SELECT e.course_id, e.student_id,
    EXISTS (
      SELECT 1
      FROM exam_grade eg
      WHERE eg.student_user_id = e.student_id
        AND eg.course_id = e.course_id
    ) AS has_exam,

    EXISTS (
      SELECT 1
      FROM exam_grade eg
      WHERE eg.student_user_id = e.student_id
        AND eg.course_id = e.course_id
        AND eg.passed = TRUE
    ) AS has_passed,
    EXISTS (
      SELECT 1
      FROM retake r
      WHERE r.student_user_id = e.student_id
        AND r.course_id = e.course_id
    ) AS has_retake

  FROM enrolled e
),

exam_agg AS (
  SELECT course_id,
    COUNT(*) FILTER (WHERE has_passed) AS passed_cnt,
    COUNT(*) FILTER (WHERE has_exam AND NOT has_passed) AS failed_after_retakes_cnt,
    COUNT(*) FILTER (WHERE has_retake) AS retake_cnt
  FROM exam_flags
  GROUP BY course_id
)
SELECT c.course_id AS "Номер курса", c.name AS "Название курса",
  COALESCE(ti.teachers, '—') AS "Преподаватели курса",
  COALESCE(ec.enrolled_students, 0) AS "Поступивших студентов",
  COALESCE(g.groups_cnt, 0) AS "Количество групп",
  COALESCE(ROUND(100.0 * COALESCE(ea.passed_cnt, 0) / NULLIF(COALESCE(ec.enrolled_students, 0), 0), 2), 0) AS "% успешно окончивших",
  COALESCE(ROUND(100.0 * COALESCE(ea.failed_after_retakes_cnt, 0) / NULLIF(COALESCE(ec.enrolled_students, 0), 0), 2), 0) AS "% не сдавших (с учётом пересдач)",
  COALESCE(ROUND(100.0 * COALESCE(ea.retake_cnt, 0) / NULLIF(COALESCE(ec.enrolled_students, 0), 0), 2), 0) AS "% попавших на пересдачу"

FROM course c
LEFT JOIN grp g ON g.course_id = c.course_id
RIGHT JOIN enrolled_cnt ec ON ec.course_id = c.course_id
LEFT JOIN teacher_info ti ON ti.course_id = c.course_id
LEFT JOIN exam_agg ea ON ea.course_id = c.course_id
ORDER BY c.course_id;
