CREATE EXTENSION IF NOT EXISTS tablefunc;

SELECT
  row_course,
  COALESCE(c_1,0)  AS c_1,
  COALESCE(c_2,0)  AS c_2,
  COALESCE(c_3,0)  AS c_3,
  COALESCE(c_4,0)  AS c_4,
  COALESCE(c_10,0) AS c_10,
  COALESCE(c_11,0) AS c_11,
  COALESCE(c_13,0) AS c_13,
  COALESCE(c_14,0) AS c_14,
  COALESCE(c_15,0) AS c_15,
  COALESCE(c_16,0) AS c_16,
  COALESCE(c_17,0) AS c_17,
  COALESCE(c_18,0) AS c_18,
  COALESCE(c_19,0) AS c_19,
  COALESCE(c_20,0) AS c_20,
  COALESCE(c_21,0) AS c_21,
  COALESCE(c_22,0) AS c_22,
  COALESCE(c_24,0) AS c_24
FROM crosstab(
  $$
  WITH pairs AS (
    SELECT
      pc1.course_id AS row_course,
      pc2.course_id AS col_course,
      1::int AS flag
    FROM program_course pc1
    JOIN program_course pc2 ON pc1.program_id = pc2.program_id
    WHERE pc1.course_id <> pc2.course_id
    GROUP BY pc1.course_id, pc2.course_id
  )
  SELECT row_course, col_course, flag
  FROM pairs
  ORDER BY 1,2
  $$,
  $$
  SELECT DISTINCT course_id
  FROM program_course
  ORDER BY course_id
  $$
) AS ct(
  row_course int,
  c_1 int, c_2 int, c_3 int, c_4 int,
  c_10 int, c_11 int, c_13 int, c_14 int, c_15 int, c_16 int, c_17 int,
  c_18 int, c_19 int, c_20 int, c_21 int, c_22 int, c_24 int
)
ORDER BY row_course;
