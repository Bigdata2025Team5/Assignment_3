CREATE OR REPLACE PROCEDURE UPDATE_CO2.CREATE_DAILY_MEASUREMENTS()

    RETURNS STRING

    LANGUAGE SQL

    EXECUTE AS CALLER

AS

$$

BEGIN

    -- Set role to ACCOUNTADMIN

    USE ROLE ACCOUNTADMIN;

    USE SCHEMA RAW_CO2;
 
    -- Create the table using dynamic SQL

    EXECUTE IMMEDIATE '

        CREATE OR REPLACE TABLE RAW_CO2.Daily_Measurements (

            date STRING,

            co2_ppm FLOAT

        );

    ';
 
    -- Grant privileges on the table to ACCOUNTADMIN role

    EXECUTE IMMEDIATE '

        GRANT ALL PRIVILEGES ON TABLE RAW_CO2.Daily_Measurements TO ROLE ACCOUNTADMIN;

    ';
 
    -- Copy data from the stage into the table

    EXECUTE IMMEDIATE '

        COPY INTO RAW_CO2.Daily_Measurements

        FROM @RAW_CO2.CO2_EXTERNAL_STAGE

        FILE_FORMAT = (

            TYPE = ''CSV'' 

            SKIP_HEADER = 1

            FIELD_OPTIONALLY_ENCLOSED_BY = ''"''

        )

        ON_ERROR = ''CONTINUE'';

    ';
 
    -- Create a stream on the table using dynamic SQL

    EXECUTE IMMEDIATE '

        CREATE OR REPLACE STREAM RAW_CO2.DAILY_MEASUREMENTS_STREAM 

        ON TABLE RAW_CO2.Daily_Measurements;

    ';
 
    RETURN 'Procedure executed successfully';

END;

$$;
 
-- Call the procedure

CALL UPDATE_CO2.CREATE_DAILY_MEASUREMENTS();

 