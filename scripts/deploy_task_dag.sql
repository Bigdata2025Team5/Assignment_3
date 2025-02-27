

from snowflake.snowpark import Session
from snowflake.core import Root
from snowflake.core.task.dagv1 import DAGOperation, DAG, DAGTask
from datetime import timedelta

# Create the tasks using the DAG API
def main(session: Session, database_name, schema_name) -> str:
    # Set the environment context
    env = 'PROD' if schema_name == 'PROD_SCHEMA' else 'DEV'
    session.use_schema(f"{database_name}.{schema_name}")

    warehouse_name = "CO2_WH"
    dag_name = "CO2_DAG"
    api_root = Root(session)
    schema = api_root.databases[database_name].schemas[schema_name]
    dag_op = DAGOperation(schema)

    # Define the DAG
    with DAG(dag_name, schedule=timedelta(days=1), warehouse=warehouse_name) as dag:
        dag_task1 = DAGTask("data_ingestion", definition=f'''EXECUTE NOTEBOOK "{database_name}"."{schema_name}"."{env}_data_ingestion"()''', warehouse=warehouse_name)
        dag_task2 = DAGTask("daily_updates", definition=f'''EXECUTE NOTEBOOK "{database_name}"."{schema_name}"."{env}_daily_updates"()''', warehouse=warehouse_name)

        # Define the dependencies between the tasks
        dag_task1 >> dag_task2 # dag_task1 is a predecessor of dag_task2

    # Create the DAG in Snowflake
    dag_op.deploy(dag, mode="orreplace")

    #dag_op.run(dag)


# For local debugging
if __name__ == "__main__":
    import sys

    # Create a local Snowpark session
    with Session.builder.getOrCreate() as session:
        if len(sys.argv) > 1:
            print(main(session, *sys.argv[1:]))  # type: ignore
        else:
            print(main(session))  # type: ignore