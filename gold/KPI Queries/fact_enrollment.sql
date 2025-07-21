-- Enrollment Trends over time
SELECT 
    t.term_year,
    t.term_sess,
    COUNT(f.enrollment_key) AS total_enrollments,
    SUM(f.enrollment_hours) AS total_credit_hours,
    COUNT(DISTINCT f.enrollment_stu_key) AS unique_students
FROM DITTEAU_DATA.GOLD.FACT_ENROLLMENT f
JOIN DITTEAU_DATA.GOLD.DIM_TERM t ON f.enrollment_term_key = t.term_key
WHERE f.is_current = 'Y'
GROUP BY t.term_year, t.term_sess
ORDER BY t.term_year, t.term_sess;

-- Add/Drop/Withdraw Trends
SELECT 
    f.enrollment_status,
    t.term_year,
    t.term_sess,
    COUNT(*) AS count
FROM DITTEAU_DATA.GOLD.FACT_ENROLLMENT f
JOIN DITTEAU_DATA.GOLD.DIM_TERM t ON f.enrollment_term_key = t.term_key
GROUP BY f.enrollment_status, t.term_year, t.term_sess
ORDER BY t.term_year, t.term_sess, f.enrollment_status;

--Grade Distribution
SELECT 
    g.grade_code,
    f.enrollment_program_level,
    t.term_year,
    COUNT(*) AS enrollment_count
FROM DITTEAU_DATA.GOLD.FACT_ENROLLMENT f
JOIN DITTEAU_DATA.GOLD.DIM_GRADE g ON f.enrollment_grade_key = g.grade_key
JOIN DITTEAU_DATA.GOLD.DIM_TERM t ON f.enrollment_term_key = t.term_key
GROUP BY g.grade_code, f.enrollment_program_level, t.term_year
ORDER BY t.term_year, f.enrollment_program_level, g.grade_code;

--Student Progression
SELECT 
    f.enrollment_program_level,
    t.term_year,
    t.term_sess,
    COUNT(DISTINCT f.enrollment_stu_key) AS student_count
FROM DITTEAU_DATA.GOLD.FACT_ENROLLMENT f
JOIN DITTEAU_DATA.GOLD.DIM_TERM t ON f.enrollment_term_key = t.term_key
GROUP BY f.enrollment_program_level, t.term_year, t.term_sess
ORDER BY f.enrollment_program_level, t.term_year, t.term_sess;

--Faculty Instructional Load
SELECT 
    fac.faculty_lastname,
    COUNT(DISTINCT f.enrollment_stu_key) AS students_taught,
    SUM(f.enrollment_hours) AS total_hours,
    COUNT(DISTINCT cs.course_section_key) AS courses_taught
FROM DITTEAU_DATA.GOLD.FACT_ENROLLMENT f
JOIN DITTEAU_DATA.GOLD.DIM_FACULTY fac ON f.enrollment_faculty_key = fac.faculty_key
JOIN DITTEAU_DATA.GOLD.DIM_COURSE_SECTION cs ON f.enrollment_course_section_key = cs.course_section_key
GROUP BY fac.faculty_lastname
ORDER BY students_taught DESC;

