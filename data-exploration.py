import pandas as pd

orders = pd.read_csv("data/raw/olist_orders_dataset.csv")
print(orders.shape)
print(orders.columns.tolist())
print(orders.head(3))

customers = pd.read_csv("data/raw/olist_customers_dataset.csv")
print(customers.shape)
print(customers.columns.tolist())
print(customers["customer_id"].nunique())
print(customers["customer_unique_id"].nunique())

items = pd.read_csv("data/raw/olist_order_items_dataset.csv")
print(items.shape)
print(items.columns.tolist())
print(items.head(3))

payments = pd.read_csv("data/raw/olist_order_payments_dataset.csv")
print(payments.shape)
print(payments.columns.tolist())
print(payments.head(3))

print(payments.groupby("order_id")["payment_sequential"].max().value_counts())