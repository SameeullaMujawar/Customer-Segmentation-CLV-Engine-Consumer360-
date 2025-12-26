import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from mlxtend.frequent_patterns import apriori, association_rules

# --------------------------------
# 1) MySQL connections
# --------------------------------
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",      # keep empty if no password
    database="consumer360"
)

# SQLAlchemy engine (use this for read_sql and to_sql)
engine = create_engine("mysql+pymysql://root:@localhost/consumer360")

# --------------------------------
# 2) Load cleaned fact + dimension data
# --------------------------------
query = """
SELECT
    f.customer_id,
    c.customer_name,
    f.order_id,
    f.order_date,
    f.total_amount
FROM fact_sales f
JOIN dim_customer c
  ON f.customer_id = c.customer_id
"""

df = pd.read_sql(query, engine)

df['order_date'] = pd.to_datetime(df['order_date'])

# --------------------------------
# 3) RFM metrics
# --------------------------------
analysis_date = df['order_date'].max() + pd.Timedelta(days=1)

rfm = df.groupby(['customer_id', 'customer_name']).agg({
    'order_date': lambda x: (analysis_date - x.max()).days,   # Recency
    'order_id': 'nunique',                                    # Frequency
    'total_amount': 'sum'                                     # Monetary
}).reset_index()

rfm.columns = ['customer_id', 'customer_name', 'recency', 'frequency', 'monetary']

# --------------------------------
# 4) Dynamic RFM scoring (no bin errors)
# --------------------------------
# Recency → lower is better
r_bins = pd.qcut(rfm['recency'], q=5, duplicates='drop')
rfm['R_score'] = pd.factorize(r_bins, sort=True)[0] + 1
rfm['R_score'] = 6 - rfm['R_score']   # invert

# Frequency
f_bins = pd.qcut(rfm['frequency'].rank(method="first"), q=5, duplicates='drop')
rfm['F_score'] = pd.factorize(f_bins, sort=True)[0] + 1

# Monetary
m_bins = pd.qcut(rfm['monetary'], q=5, duplicates='drop')
rfm['M_score'] = pd.factorize(m_bins, sort=True)[0] + 1

rfm['RFM_score'] = (
    rfm['R_score'].astype(str) +
    rfm['F_score'].astype(str) +
    rfm['M_score'].astype(str)
)

# --------------------------------
# 5) Segmentation logic
# --------------------------------
def segment_customer(row):
    if row['R_score'] >= 4 and row['F_score'] >= 4 and row['M_score'] >= 4:
        return 'Champions'
    elif row['F_score'] >= 4:
        return 'Loyal Customers'
    elif row['R_score'] <= 2 and row['F_score'] >= 3:
        return 'At Risk'
    else:
        return 'Hibernating'

rfm['segment'] = rfm.apply(segment_customer, axis=1)

print("\nSample RFM segments:")
print(rfm[['customer_name', 'R_score', 'F_score', 'M_score', 'segment']].head())

# --------------------------------
# 6) Validation: champions spend most
# --------------------------------
print("\nAverage Monetary by Segment (Validation):")
print(rfm.groupby('segment')['monetary'].mean().sort_values(ascending=False))

# --------------------------------
# 7) Market Basket Analysis
# --------------------------------
basket_query = """
SELECT
    f.order_id,
    p.product_name
FROM fact_sales f
JOIN dim_product p
  ON f.product_id = p.product_id
"""

basket_df = pd.read_sql(basket_query, engine)

basket = (
    basket_df
    .groupby(['order_id', 'product_name'])['product_name']
    .count()
    .unstack()
    .fillna(0)
)

# convert to boolean matrix (no warnings, faster)
basket = (basket > 0)

frequent_itemsets = apriori(
    basket,
    min_support=0.03,
    use_colnames=True
)

rules = association_rules(
    frequent_itemsets,
    metric="lift",
    min_threshold=1.2
)

print("\nTop Association Rules:")
print(rules[['antecedents', 'consequents', 'support', 'confidence', 'lift']].head())

# --------------------------------
# 8) Save RFM back to MySQL
# --------------------------------
rfm.to_sql(
    name='rfm_result',
    con=engine,
    if_exists='replace',
    index=False
)

print("\nRFM results saved to MySQL table: rfm_result")
