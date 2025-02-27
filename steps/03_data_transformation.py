

from snowflake.snowpark import Session

from snowflake.snowpark.functions import col, to_date, avg

from snowflake.snowpark.window import Window
 
# Function to transform CO2 data

def transform_co2_data(session):

    try:

        # Step 1: Read data from the raw table

        # We're accessing the DAILY_MEASUREMENTS table from the CO2_DB.RAW_CO2 schema

        raw_df = session.table("CO2_DB.RAW_CO2.DAILY_MEASUREMENTS")
 
        # Step 2: Apply transformations to the raw data

        # Convert the 'DATE' column to a proper DATE type and cast 'CO2_PPM' to float

        harmonized_df = raw_df.select(

            to_date(col("DATE")).alias("DATE"),  # Convert the 'DATE' column to a DATE type

            col("CO2_PPM").cast("float").alias("CO2_PPM")  # Cast 'CO2_PPM' to float for numerical analysis

        )
 
        # Step 3: Remove any rows with null values

        # This is important to ensure that no invalid (null) data is included in the transformations

        harmonized_df = harmonized_df.na.drop()
 
        # Step 4: Calculate a 7-day rolling average of the CO2_PPM values

        # This uses a window function that looks at the previous 6 days (including the current day)

        window = Window.orderBy(col("DATE")).rowsBetween(-6, 0)

        harmonized_df = harmonized_df.with_column(

            "ROLLING_7DAY_AVG",  # New column for the rolling average

            avg(col("CO2_PPM")).over(window)  # Apply the average over the defined window of 7 days

        )
 
        # Step 5: Write the transformed data to the harmonized table

        # The transformed data will be saved into the CO2_EMISSIONS_HARMONIZED table in the HARMONIZED_CO2 schema

        harmonized_df.write.mode("overwrite").save_as_table("CO2_DB.HARMONIZED_CO2.CO2_EMISSIONS_HARMONIZED")
 
        # Print a success message to indicate that the transformation is complete

        print("Data transformation completed successfully.")

    except Exception as e:

        # If an error occurs, print the error message and re-raise the exception for debugging

        print(f"Transformation failed: {e}")

        raise  # Re-raise the exception for the notebook to display
 
# Main block to execute the script

if __name__ == "__main__":

    try:

        # Step 1: Create a Snowflake session

        # Snowflake sessions are automatically authenticated, so no need to provide credentials explicitly.

        session = Session.builder.appName("CO2_Data_Transformation").getOrCreate()

        print("Snowflake session established.")
 
        # Step 2: Run the CO2 data transformation function

        transform_co2_data(session)

        print("Transformation completed.")
 
    except Exception as e:

        # If there is an issue with session creation or transformation, print the error message

        print(f"Error during session creation or transformation: {e}")

        raise  # Re-raise the exception for the notebook to display
 
    finally:

        # Step 3: Close the Snowflake session after the operation completes

        # It ensures that the session is closed properly to release resources.

        if 'session' in locals():  # Check if session was created before attempting to close it

            session.close()

            print("Snowflake session closed.")
 
