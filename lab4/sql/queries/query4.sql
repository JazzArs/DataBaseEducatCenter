WITH lessons AS (
  SELECT e.student_id, sv.academic_year, sv.semester, sv.version_no, si.day_of_week, si.pair_no, si.course_id,
    LAST_VALUE(si.course_id) OVER (
      PARTITION BY e.student_id
      ORDER BY sv.academic_year, sv.semester, sv.version_no, si.day_of_week, si.pair_no
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS last_course_id
  FROM student_group_enrollment e
  JOIN schedule_item si ON si.group_id = e.group_id
  JOIN schedule_version sv ON sv.schedule_version_id = si.schedule_version_id
),
last_course AS (
  SELECT DISTINCT student_id, last_course_id
  FROM lessons
),
pay AS (
  SELECT student_id, course_id, status, price
  FROM payment
),
paid_courses AS (
  SELECT student_id, course_id, SUM(price) AS paid_sum
  FROM pay
  WHERE status = 'PAID'
  GROUP BY student_id, course_id
),
paid_agg AS (
  SELECT student_id,
         COUNT(DISTINCT course_id) AS bought_courses_cnt,
         SUM(paid_sum) AS total_paid_sum
  FROM paid_courses
  GROUP BY student_id
),
all_paid_flag AS (
  SELECT p.student_id,
         NOT EXISTS (
           SELECT 1 FROM pay p2
           WHERE p2.student_id = p.student_id AND p2.status <> 'PAID'
         ) AS all_courses_paid
  FROM pay p
  GROUP BY p.student_id
),
exam_per_course AS (
  SELECT pc.student_id, pc.course_id,
         EXISTS (
           SELECT 1 FROM exam_grade eg
           WHERE eg.student_user_id = pc.student_id AND eg.course_id = pc.course_id
         ) AS has_exam,
         EXISTS (
           SELECT 1 FROM exam_grade eg
           WHERE eg.student_user_id = pc.student_id AND eg.course_id = pc.course_id AND eg.passed = TRUE
         ) AS passed_course
  FROM paid_courses pc
),
exam_agg AS (
  SELECT student_id,
         COUNT(*) FILTER (WHERE passed_course) AS passed_cnt,
         COUNT(*) FILTER (WHERE NOT passed_course) AS not_passed_cnt,
         COUNT(*) AS bought_cnt,
         COUNT(*) FILTER (WHERE NOT has_exam) AS not_started_cnt
  FROM exam_per_course
  GROUP BY student_id
),
avg_grade AS (
  SELECT eg.student_user_id AS student_id,
         ROUND(AVG(eg.grade_value)::numeric, 2) AS avg_grade_value
  FROM exam_grade eg
  GROUP BY eg.student_user_id
)
SELECT s.user_id AS "ID студента", s.fio AS "ФИО", s.email AS "Email", s.phone AS "Телефон", c_last.name AS "Последний посещённый курс",
  COALESCE(pa.bought_courses_cnt, 0) AS "Количество купленных курсов",
  COALESCE(ROUND(100.0 * COALESCE(ea.passed_cnt, 0) / NULLIF(COALESCE(ea.bought_cnt, 0), 0), 2), 0) AS "% успешного завершения курсов",
  COALESCE(pa.total_paid_sum, 0) AS "Сумма покупок",
  CASE WHEN COALESCE(ap.all_courses_paid, TRUE) THEN 'Да' ELSE 'Нет' END AS "Все ли курсы оплачены",
  ag.avg_grade_value AS "Средняя оценка",
  CASE WHEN (COALESCE(ea.not_started_cnt, 0) > 0) OR (COALESCE(ea.not_passed_cnt, 0) > 0) THEN 'Да' ELSE 'Нет' END AS "Есть непройденные курсы"
FROM student s
LEFT JOIN last_course lc ON lc.student_id = s.user_id
LEFT JOIN course c_last ON c_last.course_id = lc.last_course_id
LEFT JOIN paid_agg pa ON pa.student_id = s.user_id
LEFT JOIN all_paid_flag ap ON ap.student_id = s.user_id
LEFT JOIN exam_agg ea ON ea.student_id = s.user_id
LEFT JOIN avg_grade ag ON ag.student_id = s.user_id
ORDER BY s.user_id;