--!jinja

/*-----------------------------------------------------------------------------
Hands-On Lab: Intro to Data Engineering with Notebooks
Script:       deploy_notebooks.sql
Author:       Jeremiah Hansen
Last Updated: 6/11/2024
-----------------------------------------------------------------------------*/

-- See https://docs.snowflake.com/en/LIMITEDACCESS/execute-immediate-from-template

-- Create the Notebooks
--USE SCHEMA {{env}}_SCHEMA;

CREATE OR REPLACE NOTEBOOK IDENTIFIER('"CO2_DB"."{{env}}_SCHEMA"."{{env}}_data_ingestion"')
    FROM '@"CO2_DB"."INTEGRATIONS"."DEMO_GIT_REPO"/branches/"{{branch}}"/notebooks/data_ingestion/'
    QUERY_WAREHOUSE = 'CO2_WH'
    MAIN_FILE = 'data_ingestion.ipynb';

ALTER NOTEBOOK "CO2_DB"."{{env}}_SCHEMA"."{{env}}_data_ingestion" ADD LIVE VERSION FROM LAST;

CREATE OR REPLACE NOTEBOOK IDENTIFIER('"CO2_DB"."{{env}}_SCHEMA"."{{env}}_daily_updates"')
    FROM '@"CO2_DB"."INTEGRATIONS"."DEMO_GIT_REPO"/branches/"{{branch}}"/notebooks/daily_updates/'
    QUERY_WAREHOUSE = 'CO2_WH'
    MAIN_FILE = 'daily_updates.ipynb';

ALTER NOTEBOOK "CO2_DB"."{{env}}_SCHEMA"."{{env}}_daily_updates" ADD LIVE VERSION FROM LAST;