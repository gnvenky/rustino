import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

df = pd.DataFrame({
    'order_id': [1,2,3,4,5,6],
    'customer_id': [101,102,103,104,105,106],
    'amount': [100,200,300,400,500,600],
    'order_date': [
        datetime(2023,1,1),
        datetime(2023,1,2),
        datetime(2023,1,3),
        datetime(2023,1,4),
        datetime(2023,1,5),
        datetime(2023,1,6),
    ]
})

table = pa.Table.from_pandas(df)
pq.write_table(table, 'data/sales.parquet')
