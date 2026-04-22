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

temp = pd.merge(customers, orders, on="customer_id", how="left")
df = temp.merge(products, on="product_id", how="left")

def lower_strip(df):
    df.columns = df.columns.str.strip().str.lower()
    object_col = df.select_dtypes(include=["string", "object"]).columns
    for col in object_col:
        df[col] = df[col].apply(lambda x: str(x).strip().lower() if pd.notnull(x) else x)
    return df

def convert_datetime(df):

    date_col = ["signup_date", "order_date", "added_date"]
    df[date_col] = df[date_col].apply(lambda col: pd.to_datetime(col, format="mixed", errors="coerce"))
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

def data_filling(new_df):

    new_df["city"] = new_df["city"].fillna(new_df["city"].mode()[0] if not new_df["city"].mode().empty else "unknown")
    new_df["signup_date"] = new_df.groupby("city")["signup_date"].transform(lambda x: x.fillna(x.median()))
    new_df["product_name"] = new_df["product_name"].fillna(new_df["product_name"].mode()[0] if not new_df["product_name"].mode().empty else "unknown")
    new_df["product_id"] = new_df.groupby("product_name")["product_id"].transform(lambda x: x.fillna(x.median()))
    new_df["price"] = new_df.groupby("product_name")["price"].transform(lambda x: x.fillna(x.median()))
    new_df["added_date"] = new_df.groupby("product_name")["added_date"].transform(lambda x: x.fillna(x.median() if not x.isnull().all() else x))
    new_df["stock"] = new_df.groupby("product_name")["stock"].transform(lambda x: x.fillna(x.median()))
    new_df["category"] = new_df.groupby("product_name")["category"].transform(lambda x: x.fillna(x.mode()[0]if not x.mode().empty else "unknown"))
    return new_df

def fun_call(df):
    new_df = df.copy()
    new_df.dropna(how="all", inplace= True)
    new_df.dropna(subset="order_id", how="any", inplace=True)
    new_df = lower_strip(new_df)
    new_df = convert_datetime(new_df)

    num_cols = ["price", "stock", "quantity"]
    for col in num_cols:
        new_df[col] = new_df[col].apply(clean)
    new_df = data_filling(new_df)

    return new_df

def validate(df):
    shape = df.shape
    nullsum = df.isnull().sum()
    duplicate = df.duplicated(subset=["customer_id", "order_id"]).sum()

    print(f"shape of the datafream ({shape})")
    print(f"missing valus:\n {nullsum}")
    print(f"duplicates valus: ({duplicate})")

df = fun_call(df)
print(df.isnull().sum())
print(df)


