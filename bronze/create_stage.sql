USE DATABASE DITTEAU_DATA;
USE SCHEMA BRONZE;

-- Create an internal stage for raw data files
CREATE OR REPLACE STAGE RAW_POWERFAIDS_STAGE
  FILE_FORMAT = (TYPE = CSV
                   FIELD_DELIMITER = ','
                   SKIP_HEADER = 0
                   FIELD_OPTIONALLY_ENCLOSED_BY = '"' -- for text fields with commas
                   ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE -- lenient for POC data
                   NULL_IF = ('', 'NULL', 'null') -- common null representations
                  )
  COMMENT = 'Internal stage for raw PowerFAIDS data extracts';

  show stages;
  