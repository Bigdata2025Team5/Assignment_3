from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, date_trunc, avg, sum
 
def create_daily_metrics_table(session):
    """Creates a daily performance metrics table for CO2 levels."""
    # Fully qualify the source table with the database and schema
    qualified_source_table = "CO2_DB.HARMONIZED_CO2.CO2_EMISSIONS_HARMONIZED"
 
    # Load data from the source table (fully qualified)
    df = session.table(qualified_source_table)
 
    # Aggregate the data by day, calculating the average and sum of CO2_PPM
    daily_metrics = df.groupBy(date_trunc('day', col("DATE")).alias("DAILY_DATE")) \
                      .agg(avg("CO2_PPM").alias("AVG_CO2_PPM"), 
                           sum("CO2_PPM").alias("SUM_CO2_PPM"))
 
    # Fully qualify the target table with the database and schema
    qualified_target_table = "CO2_DB.ANALYTICS_CO2.daily_co2_metrics"
 
    # Write the resulting daily metrics to the target table
    daily_metrics.write.mode("overwrite").save_as_table(qualified_target_table)
 
    # Print a confirmation message
    print(f"Daily CO2 metrics table created: {qualified_target_table}")
 
# Main block to execute the script
if __name__ == "__main__":
    try:
        # Create Snowflake session
        session = Session.builder.appName("CO2_Daily_Metrics").getOrCreate()
 
        # Call the function to create daily CO2 metrics table
        create_daily_metrics_table(session)
 
    except Exception as e:
        # Handle errors during session creation or transformation
        print(f"Error: {e}")
 
    finally:
        # Close the session after execution
        if 'session' in locals():
            session.close()
            print("Snowflake session closed.")