import pandas as pd
import numpy as np

df = pd.read_csv("orders_dummy_50000.csv")

# convert all columns to object so they can hold strings or nulls
df = df.astype(object)

fault_rate = 0.05
rows = len(df)

for col in df.columns:
    idx = np.random.choice(df.index, int(rows * fault_rate), replace=False)

    half = len(idx) // 2
    df.loc[idx[:half], col] = None   # null values
    df.loc[idx[half:], col] = ""     # empty values

df.to_csv("orders_faulty_50000.csv", index=False)

print("Faulty dataset created successfully")