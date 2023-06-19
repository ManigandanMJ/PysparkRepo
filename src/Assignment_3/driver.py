from src.Assignment_3.utils import *

spark = spark_session()
product_df = create_df(spark)
product_df.show()
counrty_amt = pivot_amount(product_df)
counrty_amt.show()
unpivoted_df = unpivot_country(product_df)
unpivoted_df.show()