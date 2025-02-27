from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, date_trunc, avg, sum
 
def create_weekly_metrics_table(session: Session):
    """Creates a weekly performance metrics table for CO2 levels."""
    # Fully qualify the source table with the database and schema
    qualified_source_table = f"CO2_DB.HARMONIZED_CO2.CO2_EMISSIONS_HARMONIZED"
    # Load the data from the source table (fully qualified)
    df = session.table(qualified_source_table)
    # Aggregate the data by week, calculating the average and sum of CO2_PPM
    weekly_metrics = df.groupBy(date_trunc('week', col("DATE")).alias("WEEKLY_DATE")) \
                        .agg(avg("CO2_PPM").alias("AVG_CO2_PPM"), 
                             sum("CO2_PPM").alias("SUM_CO2_PPM"))
    # Fully qualify the target table with the database and schema
    qualified_target_table = f"CO2_DB.ANALYTICS_CO2.weekly_co2_metrics"
    # Write the resulting weekly metrics to the target table
    weekly_metrics.write.mode("overwrite").save_as_table(qualified_target_table)
    # Print a confirmation message
    print(f"Weekly metrics table created: {qualified_target_table}")
 
# Main block to execute the script
if __name__ == "__main__":
    try:
        # Create Snowflake session
        session = Session.builder.appName("CO2_Weekly_Metrics").getOrCreate()
 
        # Call the function to create weekly CO2 metrics table
        create_weekly_metrics_table(session)
 
    except Exception as e:
        # Handle errors during session creation or transformation
        print(f"Error: {e}")
 
    finally:
        # Close the session after execution
        if 'session' in locals():
            session.close()
            print("Snowflake session closed.")