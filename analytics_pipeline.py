import pandas as pd
import numpy as np
from dotenv import load_dotenv
import mysql.connector
import os

load_dotenv()

host = os.getenv("AJ_HOST")
user = os.getenv("AJ_USER")
password = os.getenv("AJ_PASSWORD")
database = os.getenv("AJ_NAME")

def describe_df(df):
    return df.describe().round(2)

def group_describe(df):
    return df.groupby("dep_name")["salary"].agg(["mean", "min", "max"]).round(2)

def dep_emp_count(df):
    return df.groupby("dep_name").size()
def global_rnk (df, n=1):
    df = df.copy()
    df["rnk"] = df["salary"].rank(method="dense", ascending=False)
    top_n = df[df["rnk"] <= n]
    return top_n[["emp_id", "name", "salary", "rnk"]].sort_values(["rnk", "salary"], ascending=[True, False])

def group_rnk(df, n=1):
    df = df.copy()
    df["group_rnk"] =  df.groupby("dep_name")["salary"].rank(method="dense", ascending=False)
    top_n = df[df["group_rnk"] <= n]
    return top_n[["name", "salary", "dep_name", "group_rnk"]].sort_values(["dep_name", "group_rnk"])

def above_company_avg(df):
    return df[df["salary"] > df["salary"].mean()][["name", "salary"]]

def dep_above_avg(df):
    df = df.copy()
    df["dep_avg"] = df.groupby("dep_name")["salary"].transform("mean")
    df["diff"] = df["salary"] - df["dep_avg"]
    return df[["name", "dep_name", "salary", "dep_avg", "diff"]].round(2)

def dep_group_sum(df):
    return df.groupby("dep_name")["salary"].sum().sort_values(ascending=False)

def main():
    conn = None
    try:
        conn = mysql.connector.connect(host = host,
                               user = user,
                               password = password,
                               database = database)
        if conn.is_connected():
            print("Successfully connected to the database!")
            df_emp = pd.read_sql("SELECT * FROM employees", conn)
            df_dep = pd.read_sql("SELECT * FROM departments", conn)
            exp_view = pd.read_sql("SELECT * FROM employees_exp_rnk", conn)
            df = pd.merge(df_emp, df_dep, on="dep_id", how="inner")
            df = df.merge(exp_view, on=['emp_id', 'name'], how='left')

            print("""
            Welcome to Data Analytics System

            You can explore the following analyses:

            1. Salary Summary
            2. Department Salary Stats
            3. Employee Count per Department
            4. Top Earners in Company
            5. Top Earners per Department
            6. Salary vs Company Average
            7. Salary vs Department Average
            8. Total Salary per Department
            9. exit
            """)

            print("What do you want to look at:\n")
            print("Note: you can choose number or keyword!")
            while True:
                choice = input("\nEnter here: ").lower().strip()
                if choice in ["1", "salary", "summary", "salary summary"]:
                    print(describe_df(df))
                elif choice in ["2", "department salary stats"]:
                    print(group_describe(df))
                elif choice in ["3", "employee count per department"]:
                    print(dep_emp_count(df))
                elif choice in ["4", "top earners in company"]:
                    print("Do you want to select a custom range? (yes/no)")
                    a = input().lower().strip()
                    if a.startswith("y"):
                        try:
                            b = int(input("Enter your range: "))
                        except ValueError:
                            print("Invalid number, using default (10)")
                            b = 10
                        print(global_rnk(df, b))
                    else:
                        print("Using default (10)")
                        print(global_rnk(df, 10))
                elif choice in ["5", "top earners per department"]:
                    print("Do you want to select a custom range? (yes/no)")
                    a = input().lower().strip()
                    if a.startswith("y"):
                        try:
                            b = int(input("Enter your range: "))
                        except ValueError:
                            print("Invalid number, using default (3)")
                            b = 3
                        print(group_rnk(df, b))
                    else:
                        print("Using default (3)")
                        print(group_rnk(df, 3))
                elif choice in ["6","company average", "salary vs company average"]:
                    print(above_company_avg(df))
                elif choice in ["7", "salary vs department average", "department average"]:
                    print(dep_above_avg(df))
                elif choice in ["8", "total salary per department", "total salary"]:
                    print(dep_group_sum(df))
                elif choice in ["9", "exit"]:
                    print("Goodbye")
                    break
                else:
                    print("Option not recognized. Please try again.")
    except mysql.connector.Error as e:
        print("Database connection failed ")
        print("Error:", e)
    
    finally:
        if conn and conn.is_connected():
            conn.close()


main()