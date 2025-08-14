# Manthan: Spark based Analytics

A PySpark project designed to analyze a sample employee dataset. It demonstrates fundamental Spark operations like reading data, performing transformations, and running aggregations to derive business insights.

## ✨ Core Analyses

This script performs the following analyses on the employee data:
* Calculates the average salary for each department.
* Counts the number of employees located in each city.
* Filters and displays the top 5 highest-paid employees in the IT department.

## 🛠️ Tech Stack

* **Apache Spark**: The core data processing engine.
* **Python**: The language used for scripting.
* **PySpark**: The Python library for Spark.

## 📋 Prerequisites

Before you begin, ensure you have the following installed and configured on your system:

### Required Software
* **Java Development Kit (JDK 8 or 11)**: Apache Spark requires Java to run
* **Python 3.x**: Python 3.6 or higher recommended
* **Apache Spark**: Version 3.0 or higher
* **winutils.exe**: For Windows users only, must be placed in the `%SPARK_HOME%\hadoop\bin` directory

### Python Dependencies
```bash
pip install pyspark
```

## 🚀 Setup & Execution

### Step 1: Environment Variables
Make sure the following environment variables are set correctly:

**Windows:**
```cmd
set JAVA_HOME=C:\Program Files\Java\jdk-11.0.x
set SPARK_HOME=C:\spark\spark-3.x.x-bin-hadoop3.2
set PATH=%PATH%;%SPARK_HOME%\bin
```

**macOS/Linux:**
```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin
```

### Step 2: Verify Installation
Test your Spark installation:
```bash
spark-submit --version
```

### Step 3: Run the Application
Navigate to the project directory and execute:
```bash
spark-submit analyze_employees.py
```

## 📂 Project Structure

```
Project-Manthan/
├── artifacts/             # (Optional) For storing output files
├── employees.csv          # Input dataset
├── analyze_employees.py   # Main analysis script
└── README.md             # This file
```

## 📊 Dataset Schema

The `employees.csv` file contains the following columns:
* **employee_id**: Unique identifier for each employee
* **name**: Employee name
* **department**: Department (e.g., IT, HR, Finance, Sales)
* **salary**: Annual salary in INR
* **city**: Location of the employee
* **join_date**: Date when the employee joined the company

## 📈 Expected Output

After running the script, you will see several analysis results printed to the console:

### Analysis 1: Average Salary by Department
```
--- 💰 Analysis 1: Average Salary by Department ---
+----------+------------------+
|department|    average_salary|
+----------+------------------+
|        IT|           75000.0|
|        HR|           55000.0|
|   Finance|           70000.0|
|     Sales|           60000.0|
+----------+------------------+
```

### Analysis 2: Employee Count by City
```
--- 🏙️ Analysis 2: Employee Count by City ---
+-----------+----------------+
|       city|employee_count|
+-----------+----------------+
|  Bengaluru|               5|
|     Mumbai|               5|
|      Delhi|               4|
|  Hyderabad|               4|
|       Pune|               3|
|    Chennai|               3|
|  Ahmedabad|               2|
|    Kolkata|               2|
+-----------+----------------+
```

### Analysis 3: Top 5 Highest Paid IT Employees
```
--- 🏆 Analysis 3: Top 5 Highest Paid IT Employees ---
+-----------+----------+----------+------+-----------+----------+
|employee_id|      name|department|salary|       city| join_date|
+-----------+----------+----------+------+-----------+----------+
|        E01| Rajesh IT|        IT| 90000|  Bengaluru|2020-01-15|
|        E05|   Amit IT|        IT| 85000|     Mumbai|2019-07-22|
|        E12|  Neha IT |        IT| 80000|      Delhi|2021-03-10|
|        E18| Kiran IT |        IT| 75000|  Hyderabad|2020-09-05|
|        E23|Priya IT  |        IT| 70000|       Pune|2021-11-18|
+-----------+----------+----------+------+-----------+----------+
```
