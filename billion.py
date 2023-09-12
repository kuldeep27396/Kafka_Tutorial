from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time


def main():
    # Create a Spark session
    spark = SparkSession.builder.appName("OptimizedSparkExample").getOrCreate()

    try:
        # Generate a large DataFrame with 10 billion records
        num_records = 1000000  # 10 billion
        df = generate_large_dataframe(spark, num_records)

        # Start measuring time for optimization steps
        start_time = time.time()

        # Apply optimizations: Caching, Sorting, Filtering
        cached_df = df.cache()  # Cache the DataFrame in memory
        sorted_df = cached_df.sort("value")  # Sort the DataFrame by "value" column
        filtered_df = sorted_df.filter(col("value") > 5000)  # Filter values greater than 5 billion

        # Trigger actions on the DataFrame to actually perform the operations
        filtered_df.count()

        # Calculate time taken for optimizations
        optimization_time = time.time() - start_time

        # Show the result
        print("Optimization Steps:")
        print(f"  Caching, Sorting, and Filtering took {optimization_time:.2f} seconds")

        # Show the first few rows of the filtered DataFrame
        print("Filtered DataFrame:")
        filtered_df.show()

    finally:
        # Stop the Spark session
        spark.stop()


def generate_large_dataframe(spark, num_records):
    # Generate random data and create a DataFrame
    random_data = [(i,) for i in range(num_records)]
    columns = ["value"]
    df = spark.createDataFrame(random_data, columns)
    return df


if __name__ == "__main__":
    main()
