# work is under progress

from word2number import w2n
import re
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

conn = mysql.connector.connect(host = host,
                               user = user,
                               password = password,
                               database = database)

customers = pd.read_sql("SELECT * FROM customers", conn)
products = pd.read_sql("SELECT * FROM products", conn)
orders = pd.read_sql("SELECT * FROM orders", conn)

df = pd.merge(orders, customers, on="customer_id", how="left")
df = df.merge(products, on="product_id", how="left")


def lower_strip(df):
    df.columns = df.columns.str.strip().str.lower()
    object_col = df.select_dtypes(include=["string", "object"]).columns
    for col in object_col:
        df[col] = df[col].apply(lambda x: str(x).strip().lower() if pd.notnull(x) else x)
        df[col] = df[col].replace(["none", "nan", "", "null"], np.nan)
    return df

def convert_datetime(df):

    date_col = df.columns[df.columns.str.contains("date", case=False)]
    for col in date_col:
        before_fill = df[col].isna().sum()
        df[col] = pd.to_datetime(df[col], format="mixed", errors="coerce")

    return df

def clean(x):

    if pd.isna(x):
        return x
    
    x = str(x).lower().strip()
    is_k = bool(re.search(r"\d+(\.\d+)?\s*k\b", x))
    is_m = bool(re.search(r"\d+(\.\d+)?\s*m\b", x))
    x_clean_words = re.sub(r"₹|rs\.?", "", x)
    try:
        word =  w2n.word_to_num(x_clean_words)
        return word
    except:
        pass
    x = re.sub(r"[^\d.]", "", x)
    val = pd.to_numeric(x, errors="coerce")
    if pd.notna(val):
        if is_k:
            val *= 1000
        elif is_m:
            val *= 1000000
    return val


def fun_call(df):
    new_df = df.copy()
    if len(new_df) == 0:
        return new_df, pd.DataFrame()
    new_df.dropna(how="all", inplace= True)
    new_df.dropna(axis=1, how="all", inplace= True)
    new_df = lower_strip(new_df)
    new_df = convert_datetime(new_df)
    id_col = new_df.columns[new_df.columns.str.contains("id", case=False)]
    if len(id_col) == 0:
        return new_df, pd.DataFrame()
    for col in id_col:
        new_df[col] = pd.to_numeric(new_df[col], errors="coerce").astype("Int64")
    incomplete_records = new_df[new_df[id_col].isna().any(axis=1)].copy()
    new_df.dropna(subset=id_col, how="any", inplace=True)

    num_cols = ["price", "stock", "quantity"]
    for col in num_cols:
        if col in new_df.columns:
            new_df[col] = new_df[col].apply(clean)
    
    summary = {
    "total_rows": df.shape[0],
    "total_columns": df.shape[1],
    "clean_rows" : new_df.shape[0],
    "clean_columns" : new_df.shape[1],
    "incomplete_rows": len(incomplete_records)}

    return new_df, incomplete_records, summary


def fill_value(df):
    df = df.copy()
    fill_log = []
    date_col = df.columns[df.columns.str.contains("date", case=False)]
    for col in date_col:
        before_fill = df[col].isna().sum()
        df = df.sort_values(by=col)
        df[col] = df[col].fillna(method="ffill")
        after_fill = df[col].isna().sum()
        if before_fill > after_fill:
            fill_log.append(
                {"column": col,
            "method": "forward fill",
            "filled_values": before_fill - after_fill}
            )
    
    num_cols = ["price", "stock", "quantity"]
    for col in num_cols:
        if col in df.columns:
            before_fill = df[col].isna().sum()
            mid_val = df[col].median()
            if pd.notna(mid_val):
                df[col] = df[col].fillna(mid_val)
            after_fill = df[col].isna().sum()
            if before_fill > after_fill:
                fill_log.append(
                    {"column": col,
                     "method": "median",
                     "filled_values": before_fill - after_fill}
                )

    id_col = df.columns[df.columns.str.contains("id", case=False)]
    object_col = df.select_dtypes(include=["string", "object"]).columns
    for col in object_col:
        if col in id_col:
            continue
        before_null = df[col].isna().sum()
        if df[col].nunique() < 20:
            df[col] = df[col].fillna("missing")
            method = "missing"
        else:
            mode_val = df[col].mode()
            if not mode_val.empty:
                df[col] = df[col].fillna(mode_val[0])
                method = "mode"
            else:
                method = None
        after_null = df[col].isna().sum()
        if before_null > after_null:
            fill_log.append(
                {"column": col,
                     "method": method,
                     "filled_values": before_null - after_null}
            )
    
    fill_log_df = pd.DataFrame(fill_log)

    return df, fill_log_df

def main():
    print("\n=== DATA CLEANING PIPELINE ===\n")

    clean_df, incomplete_records, summary = fun_call(df)

    print("=== Summery ===\n")
    for k, v in summary.items():
        print(f"{k}:{v}")

    missing_report = clean_df.isna().sum().reset_index()
    missing_report.columns = ["columns", "missing_count"]
    missing_report["missing_%"] = (missing_report["missing_count"] / len(clean_df))* 100
    missing_report = missing_report.sort_values(by="missing_%", ascending=False)
    print("\n==== Missing_Report ====")
    print(missing_report)
    choice = input("\nFill missing values? (yes/no): ").strip().lower()
    if choice == "yes":
        final_df, fill_log = fill_value(clean_df)
        print(f"\n Fill_log:\n {fill_log}")

    else:
        final_df = clean_df
        fill_log = pd.DataFrame()
        print("Skipped filling missing values. ")

    print(f"\n Final Cleaned Data Preview:\n {final_df.head()}")
    print(f"\n Incompelete_records Preview:\n {incomplete_records.head()}")
    save = input("Save all records and Clean data? (yes/no): ").strip().lower()
    if save == "yes":
        with pd.ExcelWriter("report.xlsx") as writer:
            final_df.to_excel(writer, sheet_name="clean_data", index=False)
            incomplete_records.to_excel(writer, sheet_name="incomplete", index=False)
            summary_df = pd.DataFrame(list(summary.items()), columns=["metric", "value"])
            summary_df.to_excel(writer, sheet_name="summary", index=False)
            missing_report.to_excel(writer, sheet_name="missing_report", index=False)
            fill_log.to_excel(writer, sheet_name="fill_log", index=False)
    
    else:
        print("Thankyou for testing this script")


main()