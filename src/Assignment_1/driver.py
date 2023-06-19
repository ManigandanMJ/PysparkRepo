from src.Assignment_1.utils import *

# Product dataframe
spark = session_object()
product_df = create_df(spark)
product_df.show()
date_updated_df = date_time(product_df)
date_updated_df.show()
date_type = date_type(date_updated_df)
date_type.show()
space_df = space_remove(product_df)
null_emp = null_empty(product_df)
null_emp.show()

# transaction dataframe
transact_df = transaction_table(spark)
cs_df = camel_snake(transact_df)
cs_df.show()
ms_df = start_time_ms(transact_df)
ms_df.show()
col1 = "Product number"
col2 = "Product Number"
join_type = "inner"
join_df = combine_df(transact_df,product_df, col1, col2, join_type)
join_df.show()
country_df = country_en(join_df)
country_df.show()
