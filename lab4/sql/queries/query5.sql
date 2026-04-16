WITH active_ver AS (
  SELECT schedule_version_id
  FROM schedule_version
  WHERE is_active = TRUE
  ORDER BY academic_year DESC, semester DESC
  LIMIT 1 
),
teacher_courses_in_schedule AS (
  SELECT si.teacher_id, si.course_id
  FROM schedule_item si
  JOIN active_ver av ON av.schedule_version_id = si.schedule_version_id
  GROUP BY si.teacher_id, si.course_id
),
total_per_teacher AS (
  SELECT teacher_id, COUNT(*) AS total_courses_in_schedule
  FROM teacher_courses_in_schedule
  GROUP BY teacher_id
),
ack AS (
  SELECT sa.teacher_id, sa.status
  FROM schedule_ack sa
  JOIN active_ver av ON av.schedule_version_id = sa.schedule_version_id
),
not_ack_courses AS (
  SELECT tcs.teacher_id, tcs.course_id
  FROM teacher_courses_in_schedule tcs
  LEFT JOIN ack a ON a.teacher_id = tcs.teacher_id
  WHERE a.teacher_id IS NULL OR a.status NOT IN ('SEEN', 'APPROVED')
),
not_ack_agg AS (
  SELECT nac.teacher_id, COUNT(*) AS not_ack_cnt, STRING_AGG(DISTINCT c.name, ', ' ORDER BY c.name) AS courses_list
  FROM not_ack_courses nac
  JOIN course c ON c.course_id = nac.course_id
  GROUP BY nac.teacher_id
)

SELECT t.user_id AS "Teacher ID", t.fio AS "ФИО", t.qualification AS "Квалификация",
  na.not_ack_cnt AS "Количество расписаний без ознакомления",
  na.courses_list AS "Курсы без ознакомления",
  ROUND(100.0 * na.not_ack_cnt / NULLIF(tp.total_courses_in_schedule, 0),2) AS "% неознакомленных расписаний"

FROM not_ack_agg na
JOIN total_per_teacher tp ON tp.teacher_id = na.teacher_id
JOIN teacher t ON t.user_id = na.teacher_id
ORDER BY na.not_ack_cnt DESC, t.user_id;