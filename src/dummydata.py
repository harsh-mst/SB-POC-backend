import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()
rows = []

product_lines = ['Motorcycles', 'Classic Cars', 'Trains', 'Ships', 'Planes']
statuses = ['Shipped', 'Cancelled', 'On Hold', 'Resolved']
territories = ['NA', 'EMEA', 'APAC']
deal_sizes = ['Small', 'Medium', 'Large']

for i in range(50000):
    order_number = 10000 + i
    quantity = random.randint(10, 100)
    price_each = round(random.uniform(50, 120), 2)
    order_line_number = random.randint(1, 15)
    sales = round(quantity * price_each * random.uniform(0.95, 1.05), 2)

    year = random.choice([2003, 2004, 2005])
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    order_date = datetime(year, month, day)
    qtr = (month - 1) // 3 + 1

    product_line = random.choice(product_lines)
    msrp = random.randint(70, 120)
    product_code = f"S{random.randint(10,99)}_{random.randint(1000,9999)}"

    customer = fake.company()
    phone = fake.phone_number()
    addr1 = fake.street_address()
    addr2 = "" if random.random() < 0.7 else fake.secondary_address()
    city = fake.city()
    state = fake.state_abbr()
    postal = fake.postcode()
    country = fake.country()
    territory = random.choice(territories)
    contact_last = fake.last_name()
    contact_first = fake.first_name()
    deal_size = random.choice(deal_sizes)

    rows.append([
        order_number, quantity, price_each, order_line_number, sales,
        order_date.strftime("%m/%d/%Y 0:00"), random.choice(statuses),
        qtr, month, year, product_line, msrp, product_code, customer,
        phone, addr1, addr2, city, state, postal, country, territory,
        contact_last, contact_first, deal_size
    ])

columns = [
    "ORDERNUMBER","QUANTITYORDERED","PRICEEACH","ORDERLINENUMBER","SALES",
    "ORDERDATE","STATUS","QTR_ID","MONTH_ID","YEAR_ID","PRODUCTLINE","MSRP",
    "PRODUCTCODE","CUSTOMERNAME","PHONE","ADDRESSLINE1","ADDRESSLINE2",
    "CITY","STATE","POSTALCODE","COUNTRY","TERRITORY",
    "CONTACTLASTNAME","CONTACTFIRSTNAME","DEALSIZE"
]

df = pd.DataFrame(rows, columns=columns)
df.to_csv("orders_dummy_50000.csv", index=False)
print("✅ Generated orders_dummy_50000.csv with 50,000 rows.")
