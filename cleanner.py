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
        df[col] = pd.to_datetime(df[col], format="mixed", errors="coerce")
        df[col] = df[col].fillna(df[col].median())
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
            mid_val = new_df[col].median()
            if pd.notna(mid_val):
                new_df[col] = new_df[col].fillna(new_df[col].median())
    
    object_col = new_df.select_dtypes(include=["string", "object"]).columns
    for col in object_col:
        if new_df[col].nunique() < 20:
            new_df[col] = new_df[col].fillna("missing")

    return new_df, incomplete_records

a, b = fun_call(df)
print(a.info())
df.to_excel("messy_data.xlsx", index=False)
a.to_excel("clean_data.xlsx", index=False)
b.to_excel("incomplete_records.xlsx", index=False)