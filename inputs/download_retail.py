import pandas as pd
import urllib.request
import os

print("Converting to CSV...")
df = pd.read_excel("OnlineRetail.xlsx")
# Ensure InvoiceDate is formatted correctly as a string to avoid Hive parsing issues
df['InvoiceDate'] = df['InvoiceDate'].dt.strftime('%m/%d/%y %H:%M')
df.to_csv("OnlineRetail.csv", index=False)

os.remove("OnlineRetail.xlsx")
print("Successfully generated OnlineRetail.csv!")
