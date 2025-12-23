USE ROLE SECURITYADMIN;
USE DATABASE DITTEAU_DATA;
USE SCHEMA GOVERNANCE;

CREATE OR REPLACE TAG PII_EMAIL_PHONE COMMENT = 'Indicates columns containing personally identifiable information (email or phone).';
CREATE OR REPLACE TAG PII_SSN COMMENT = 'Indicates columns containing Social Security Numbers.';
CREATE OR REPLACE TAG PII_ADDRESS COMMENT = 'Indicates columns containing personally identifiable information (address).';

-- Tag for general confidential data
CREATE OR REPLACE TAG CONFIDENTIAL
    COMMENT = 'Indicates columns containing sensitive or confidential information that requires restricted access.';

-- Tag for financial data
CREATE OR REPLACE TAG FINANCIAL_DATA
    COMMENT = 'Indicates columns containing financial transaction data, balances, or other fiscal information.';

-- Tag for highly confidential (e.g., medical records, disciplinary actions - relevant for Higher Ed)
CREATE OR REPLACE TAG HIGHLY_CONFIDENTIAL
    COMMENT = 'Indicates columns containing extremely sensitive information (e.g., medical, legal, disciplinary records) requiring the highest level of protection.';


CREATE OR REPLACE TAG STUDENT_DEMOGRAPHICS
    COMMENT = 'Indicates columns related to student demographic information (e.g., age, gender, ethnicity).';

--Then apply policies to tags:

ALTER TAG GOVERNANCE.CONFIDENTIAL SET MASKING POLICY GOVERNANCE.CONFIDENTIAL_MASKING_POLICY;

--Test 
USE ROLE DITTEAU_DATA_ADMIN;
SELECT STUDENT_ID, MAJOR FROM GOLD.DIM_STUDENT LIMIT 2; -- Should see unmasked MAJOR

USE ROLE DATA_ANALYST;
SELECT STUDENT_ID, MAJOR FROM GOLD.DIM_STUDENT LIMIT 2; -- Should see '*** CONFIDENTIAL ***' for MAJOR