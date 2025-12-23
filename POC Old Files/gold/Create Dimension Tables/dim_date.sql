USE DATABASE DITTEAU_DATA;
USE SCHEMA GOLD;


CREATE OR REPLACE TABLE GOLD.DIM_DATE (
    DATE_KEY NUMBER(8,0) PRIMARY KEY, -- YYYYMMDD
    DATE_ACTUAL DATE,
    DAY_OF_WEEK_NUM NUMBER(1,0), -- 1=Sunday, 7=Saturday
    DAY_OF_WEEK_NAME VARCHAR(10),
    DAY_OF_WEEK_ABBR VARCHAR(3),
    DAY_OF_MONTH NUMBER(2,0),
    DAY_OF_YEAR NUMBER(3,0),
    WEEK_OF_YEAR NUMBER(2,0),
    MONTH_NUM NUMBER(2,0),
    MONTH_NAME VARCHAR(10),
    MONTH_ABBR VARCHAR(3),
    QUARTER_NUM NUMBER(1,0),
    QUARTER_NAME VARCHAR(10),
    YEAR_NUM NUMBER(4,0),
    IS_WEEKEND BOOLEAN,
    IS_CURRENT_DATE BOOLEAN,
    -- Higher Education Specific Attributes (General/Primary Term)
    ACADEMIC_YEAR VARCHAR(9), -- e.g., '2024-2025'
    IS_IN_TERM BOOLEAN, -- Flag to indicate if the date is within any academic term
    PRIMARY_TERM_KEY NUMBER(38,0), -- The key of the selected primary term for this date
    PRIMARY_TERM_CODE VARCHAR(16777216),
    PRIMARY_TERM_DESCR VARCHAR(16777216),
    PRIMARY_TERM_ACAD_YR VARCHAR(16777216),
    PRIMARY_TERM_SESS VARCHAR(16777216),
    PRIMARY_TERM_YEAR NUMBER(38,0),
    PRIMARY_TERM_SUBSESS VARCHAR(16777216),
    PRIMARY_TERM_PROGRAM_LEVEL VARCHAR(16777216), -- Retain for clarity on which was picked
    -- Fiscal Year (assuming fiscal year starts July 1st for higher ed)
    FISCAL_YEAR_NUM NUMBER(4,0),
    FISCAL_QUARTER_NUM NUMBER(1,0),
    FISCAL_MONTH_NUM NUMBER(2,0),
    FISCAL_PERIOD_NAME VARCHAR(20) -- e.g., 'FY2025-Q1', 'FY2025-M07'
);

-- Populate DIM_DATE
INSERT INTO GOLD.DIM_DATE
WITH RECURSIVE DATE_SERIES AS (
    SELECT '1980-01-01'::DATE AS DT -- Start date for your dimension
    UNION ALL
    SELECT DATEADD(day, 1, DT) FROM DATE_SERIES WHERE DT < '2035-12-31'::DATE -- End date for your dimension
),
-- CTE to select a single, primary term for each distinct term date range.
-- If multiple program levels share the exact same term dates, we'll pick one.
PRIMARY_TERMS_PER_RANGE AS (
    SELECT
        TERM_KEY,
        TERM_CODE,
        TERM_DESCR,
        TERM_ACAD_YR,
        TERM_SESS,
        TERM_YEAR,
        TERM_SUBSESS,
        TERM_PROGRAM_LEVEL,
        TERM_BEG_DATE,
        TERM_END_DATE,
        ROW_NUMBER() OVER (
            PARTITION BY TERM_BEG_DATE, TERM_END_DATE -- Group by the unique date range
            ORDER BY
                CASE
                    WHEN TERM_PROGRAM_LEVEL = 'UNDG' THEN 1
                    WHEN TERM_PROGRAM_LEVEL = 'GRAD' THEN 2
                    ELSE 3
                END,
                TERM_KEY -- Fallback for consistent selection
        ) as rn_per_range
    FROM GOLD.DIM_TERM
    WHERE TERM_BEG_DATE IS NOT NULL AND TERM_END_DATE IS NOT NULL
),
-- CTE to join dates with all relevant primary terms, then pick one for each date.
DAILY_TERM_ASSIGNMENT AS (
    SELECT
        DS.DT,
        PT.TERM_KEY,
        PT.TERM_CODE,
        PT.TERM_DESCR,
        PT.TERM_ACAD_YR,
        PT.TERM_SESS,
        PT.TERM_YEAR,
        PT.TERM_SUBSESS,
        PT.TERM_PROGRAM_LEVEL,
        ROW_NUMBER() OVER (
            PARTITION BY DS.DT
            ORDER BY
                PT.TERM_PROGRAM_LEVEL, -- Apply business logic here for prioritizing overlapping terms for a *given day*
                PT.TERM_BEG_DATE DESC, -- Prefer terms that started later (more recent term if overlapping)
                PT.TERM_END_DATE ASC,   -- Prefer terms that end earlier (e.g., shorter session within a longer one)
                PT.TERM_KEY             -- Final tie-breaker
        ) as rn_per_day
    FROM DATE_SERIES DS
    LEFT JOIN PRIMARY_TERMS_PER_RANGE PT
        ON DS.DT BETWEEN PT.TERM_BEG_DATE AND PT.TERM_END_DATE
        AND PT.rn_per_range = 1 -- Only consider terms that were deemed primary for their date range
)
SELECT
    TO_NUMBER(TO_CHAR(DSA.DT, 'YYYYMMDD')) AS DATE_KEY,
    DSA.DT AS DATE_ACTUAL,
    DAYOFWEEK(DSA.DT) AS DAY_OF_WEEK_NUM,
    DAYNAME(DSA.DT) AS DAY_OF_WEEK_NAME,
    LEFT(DAYNAME(DSA.DT), 3) AS DAY_OF_WEEK_ABBR,
    DAYOFMONTH(DSA.DT) AS DAY_OF_MONTH,
    DAYOFYEAR(DSA.DT) AS DAY_OF_YEAR,
    WEEKOFYEAR(DSA.DT) AS WEEK_OF_YEAR,
    MONTH(DSA.DT) AS MONTH_NUM,
    MONTHNAME(DSA.DT) AS MONTH_NAME,
    LEFT(MONTHNAME(DSA.DT), 3) AS MONTH_ABBR,
    QUARTER(DSA.DT) AS QUARTER_NUM,
    'Q' || QUARTER(DSA.DT) AS QUARTER_NAME,
    YEAR(DSA.DT) AS YEAR_NUM,
    CASE WHEN DAYOFWEEK(DSA.DT) IN (1, 7) THEN TRUE ELSE FALSE END AS IS_WEEKEND, -- Sunday=1, Saturday=7
    CASE WHEN DSA.DT = CURRENT_DATE() THEN TRUE ELSE FALSE END AS IS_CURRENT_DATE,
    -- Academic Year (assuming academic year starts in Fall of previous calendar year)
    CASE
        WHEN MONTH(DSA.DT) >= 8 THEN -- Academic year starts in August (adjust as per your institution)
            TO_VARCHAR(YEAR(DSA.DT)) || '-' || TO_VARCHAR(YEAR(DSA.DT) + 1)
        ELSE
            TO_VARCHAR(YEAR(DSA.DT) - 1) || '-' || TO_VARCHAR(YEAR(DSA.DT))
    END AS ACADEMIC_YEAR,
    -- Term Information (from the DAILY_TERM_ASSIGNMENT CTE)
    CASE WHEN DSA.TERM_KEY IS NOT NULL THEN TRUE ELSE FALSE END AS IS_IN_TERM,
    DSA.TERM_KEY AS PRIMARY_TERM_KEY,
    DSA.TERM_CODE AS PRIMARY_TERM_CODE,
    DSA.TERM_DESCR AS PRIMARY_TERM_DESCR,
    DSA.TERM_ACAD_YR AS PRIMARY_TERM_ACAD_YR,
    DSA.TERM_SESS AS PRIMARY_TERM_SESS,
    DSA.TERM_YEAR AS PRIMARY_TERM_YEAR,
    DSA.TERM_SUBSESS AS PRIMARY_TERM_SUBSESS,
    DSA.TERM_PROGRAM_LEVEL AS PRIMARY_TERM_PROGRAM_LEVEL,
    CASE
        WHEN MONTH(DSA.DT) >= 7 THEN -- Fiscal year starts in July (adjust as per institution)
            YEAR(DSA.DT) + 1
        ELSE
            YEAR(DSA.DT)
    END AS FISCAL_YEAR_NUM,
    CASE
        WHEN MONTH(DSA.DT) >= 7 AND MONTH(DSA.DT) <= 9 THEN 1
        WHEN MONTH(DSA.DT) >= 10 AND MONTH(DSA.DT) <= 12 THEN 2
        WHEN MONTH(DSA.DT) >= 1 AND MONTH(DSA.DT) <= 3 THEN 3
        WHEN MONTH(DSA.DT) >= 4 AND MONTH(DSA.DT) <= 6 THEN 4
        ELSE NULL
    END AS FISCAL_QUARTER_NUM,
    CASE
        WHEN MONTH(DSA.DT) >= 7 THEN MONTH(DSA.DT) - 6
        ELSE MONTH(DSA.DT) + 6
    END AS FISCAL_MONTH_NUM,
    'FY' || TO_VARCHAR(
        CASE
            WHEN MONTH(DSA.DT) >= 7 THEN YEAR(DSA.DT) + 1
            ELSE YEAR(DSA.DT)
        END
    ) || '-Q' || TO_VARCHAR(
        CASE
            WHEN MONTH(DSA.DT) >= 7 AND MONTH(DSA.DT) <= 9 THEN 1
            WHEN MONTH(DSA.DT) >= 10 AND MONTH(DSA.DT) <= 12 THEN 2
            WHEN MONTH(DSA.DT) >= 1 AND MONTH(DSA.DT) <= 3 THEN 3
            WHEN MONTH(DSA.DT) >= 4 AND MONTH(DSA.DT) <= 6 THEN 4
            ELSE NULL
        END
    ) AS FISCAL_PERIOD_NAME
FROM DAILY_TERM_ASSIGNMENT DSA
WHERE DSA.rn_per_day = 1
ORDER BY DATE_KEY;

-- Verify the DIM_DATE table
SELECT COUNT(*) AS total_rows, COUNT(DISTINCT DATE_KEY) AS distinct_dates FROM GOLD.DIM_DATE;
-- This should now return equal values.

-- Test a date within a potentially overlapping term period
SELECT * FROM GOLD.DIM_DATE WHERE DATE_ACTUAL = '2024-09-15';