from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count

def main():
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("HR Analytics") \
        .master("local[*]") \
        .getOrCreate()
    print("SparkSession created successfully!")

    df = spark.read.csv("employees.csv", header=True, inferSchema=True)
    
    print("Starting analysis on the Employee Dataset!")
    print("Schema of our data:")
    df.printSchema()

    print("\n--- Analysis 1: Average Salary by Department ---")
    
    # Group by 'department', then calculate the average of the 'salary' column for each group.
    # Result is given a new column name 'avg_salary'.
    avg_salary_df = df.groupBy("department").agg(avg("salary").alias("avg_salary"))
    
    avg_salary_df.show()

    print("\n--- Analysis 2: Employee Count by City ---")

    # Group by 'city' and then count the rows in each group.
    city_count_df = df.groupBy("city").agg(count("*").alias("employee_count"))

    # Sort the results to see which cities have the most employees
    city_count_df.orderBy("employee_count", ascending=False).show()

    print("\n--- Analysis 3: Top 5 Earners in the IT Department ---")

    # Use the .filter() or .where() method to select only rows where 'department' is 'IT'.
    # Sort by salary in descending order and take the top 5.
    top_it_earners_df = df.filter(df.department == "IT") \
                          .orderBy(df.salary.desc()) \
                          .limit(5)

    top_it_earners_df.show()

    # Stop the SparkSession
    spark.stop()
    print("\n Analysis complete. SparkSession stopped.")

if __name__ == '__main__':
    main()