import logging
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import avg, count, col, rank, udf, when, coalesce
from pyspark.sql.types import StringType
import os

# Provides timestamped logs, which are better for tracking job progress and errors.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Categorizes salary into bands. UDFs are useful for complex business rules.
def categorize_salary_band(salary):
    """Categorizes a salary value into 'Low', 'Medium', 'High', or 'Very High' bands."""
    if salary is None:
        return 'Unknown'
    if salary < 50000:
        return 'Low'
    elif salary < 90000:
        return 'Medium'
    elif salary < 130000:
        return 'High'
    else:
        return 'Very High'

# Register UDF with Spark
salary_band_udf = udf(categorize_salary_band, StringType())


def main():
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("Advanced HR Analytics") \
        .master("local[*]") \
        .getOrCreate()
    logging.info("SparkSession created successfully!")

    # Define paths for input and output
    input_path = "employees.csv"
    output_dir = "artifacts"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logging.info(f"Created output directory: {output_dir}")

    try:
        df = spark.read.csv(input_path, header=True, inferSchema=True)
        logging.info("Employee dataset loaded successfully.")
        df.printSchema()
    except Exception as e:
        logging.error(f"Error reading from {input_path}: {e}")
        spark.stop()
        return

    # Fill any missing salaries with the average salary of their respective department.
    logging.info("Handling missing salary data...")
    # Calculate average salary for each department
    avg_salary_per_dept = df.groupBy("department").agg(avg("salary").alias("dept_avg_salary"))
    
    # Join original DataFrame with the average salary DataFrame
    df_with_avg = df.join(avg_salary_per_dept, on="department", how="left")
    
    # Use coalesce() to fill null salaries with the calculated department average
    # If 'salary' is not null, it's used; otherwise, 'dept_avg_salary' is used.
    df_cleaned = df_with_avg.withColumn("salary", coalesce(col("salary"), col("dept_avg_salary"))) \
                            .drop("dept_avg_salary")
    logging.info("Missing salaries filled with department average.")


    logging.info("Running Analysis 1: Average Salary by Department")
    avg_salary_df = df_cleaned.groupBy("department").agg(avg("salary").alias("avg_salary"))
    avg_salary_df.show()
    # Save output to a high-performance format
    avg_salary_df.write.mode("overwrite").parquet(f"{output_dir}/avg_salary_by_department")
    logging.info(f"Analysis 1 results saved to {output_dir}/avg_salary_by_department")

    logging.info("Running Analysis 2: Employee Count by City")
    city_count_df = df_cleaned.groupBy("city").agg(count("*").alias("employee_count"))
    city_count_df.orderBy("employee_count", ascending=False).show()
    city_count_df.write.mode("overwrite").parquet(f"{output_dir}/employee_count_by_city")
    logging.info(f"Analysis 2 results saved to {output_dir}/employee_count_by_city")
    
    # Rank employees by salary within each department to find top earners per department.
    logging.info("Running Analysis 3: Rank Employees by Salary within each Department")
    windowSpec = Window.partitionBy("department").orderBy(col("salary").desc())
    ranked_employees_df = df_cleaned.withColumn("rank_in_department", rank().over(windowSpec))
    
    # Show top 3 employees from each department
    ranked_employees_df.filter(col("rank_in_department") <= 3).show(30, truncate=False)
    ranked_employees_df.write.mode("overwrite").parquet(f"{output_dir}/ranked_employees")
    logging.info(f"Analysis 3 results saved to {output_dir}/ranked_employees")

    # Apply the UDF to create a new 'salary_band' column.
    logging.info("Running Analysis 4: Categorizing Employees into Salary Bands")
    banded_df = df_cleaned.withColumn("salary_band", salary_band_udf(col("salary")))
    banded_df.select("employee_name", "department", "salary", "salary_band").show()
    
    # Also show a summary of the bands
    logging.info("Summary of employee counts per salary band:")
    banded_df.groupBy("salary_band").count().orderBy("count", ascending=False).show()
    banded_df.write.mode("overwrite").parquet(f"{output_dir}/employees_with_salary_bands")
    logging.info(f"Analysis 4 results saved to {output_dir}/employees_with_salary_bands")

    # Stop the SparkSession
    spark.stop()
    logging.info("Analysis complete. SparkSession stopped.")

if __name__ == '__main__':
    main()