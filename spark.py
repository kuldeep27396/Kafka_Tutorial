def sparkCall():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col

    # Create a Spark session
    spark = SparkSession.builder.appName("LocalSparkExample").getOrCreate()

    # Sample data
    data = [("Alice", 28), ("Bob", 22), ("Charlie", 35), ("David", 40)]
    columns = ["Name", "Age"]

    # Create a DataFrame from the sample data
    df = spark.createDataFrame(data, columns)

    # Show the original DataFrame
    print("Original DataFrame:")
    df.show()

    # Perform a simple transformation (filter and select)
    filtered_df = df.filter(col("Age") >= 30).select("Name", "Age")

    # Show the transformed DataFrame
    print("Transformed DataFrame:")
    filtered_df.show()

    # Stop the Spark session
    spark.stop()


def main():
    sparkCall()


if __name__ == '__main__':
    main()
