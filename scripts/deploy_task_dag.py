# Replace cell8 with this code

from snowflake.core.task.dagv1 import DAGOperation, DAG, DAGTask

from datetime import timedelta

from snowflake.core.task import Cron  # Import Cron
 
# Create the tasks using the DAG API

warehouse_name = "CO2_WH"

dag_name = "CO2_DAG"
 
schema_name = "DEV_SCHEMA"
 
api_root = Root(session)

schema = api_root.databases[database_name].schemas[schema_name]

dag_op = DAGOperation(schema)
 
# Define the DAG

with DAG(dag_name, schedule=Cron("30 8 * * *", timezone="EST"), warehouse=warehouse_name) as dag: # Use Cron object with timezone

    # Add a task to call the stored procedure

    proc_task = DAGTask(

        "load_raw_co2_data",

        definition="CALL CO2_DB.UPDATE_CO2.CREATE_DAILY_MEASUREMENTS()",  # Ensure correct procedure name and case

        warehouse=warehouse_name

    )
 
    # Your existing tasks

    dag_task2 = DAGTask(

        "daily_updates",

        definition=f'''EXECUTE NOTEBOOK "{database_name}"."{schema_name}"."{env}_daily_updates"()''',

        warehouse=warehouse_name

    )
 
    # Define the dependencies between the tasks

    proc_task >> dag_task2  # First run the stored procedure, then daily updates
 
# Create the DAG in Snowflake

dag_op.deploy(dag, mode="orreplace")

 