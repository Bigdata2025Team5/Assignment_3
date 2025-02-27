from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lag, when
from snowflake.snowpark.window import Window
 
def calculate_co2_percentage_change(session: Session):
    """
    Calculates the percentage change in CO2_PPM from the previous day.
    """
    try:
        # Access harmonized CO2 data table
        harmonized_df = session.table("CO2_DB.HARMONIZED_CO2.CO2_EMISSIONS_HARMONIZED")
 
        # Define window specification ordered by 'DATE'
        window_spec = Window.orderBy(col("DATE"))
 
        # Calculate previous day's CO2_PPM using 'lag'
        harmonized_df = harmonized_df.with_column(
            "PREVIOUS_CO2", lag(col("CO2_PPM")).over(window_spec)
        )
 
        # Calculate percentage change from previous day
        harmonized_df = harmonized_df.with_column(
            "PERCENTAGE_CHANGE",
            when(col("PREVIOUS_CO2").isNotNull(),
                 ((col("CO2_PPM") - col("PREVIOUS_CO2")) / col("PREVIOUS_CO2")) * 100
            ).otherwise(None)
        )
 
        # Show the result
        harmonized_df.show()
 
        # Save the result to a new table
        harmonized_df.write.mode("overwrite").save_as_table("CO2_DB.HARMONIZED_CO2.CO2_EMISSIONS_HARMONIZED_WITH_PERCENTAGE_CHANGE")
 
        # Return the dataframe
        return harmonized_df
 
    except Exception as e:
        print(f"Error in calculate_co2_percentage_change: {e}")
        return None
 
# Main block to execute the script
if __name__ == "__main__":
    try:
        # Create Snowflake session
        session = Session.builder.appName("CO2_Percentage_Change").getOrCreate()
 
        # Call the function to calculate percentage change
        calculate_co2_percentage_change(session)
 
    except Exception as e:
        # Handle errors during session creation or transformation
        print(f"Error in main execution: {e}")
 