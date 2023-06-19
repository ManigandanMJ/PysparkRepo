from src.Assignment_4.utils import *

spark = spark_session()
employee_df = create_df(spark)
employee_df.show()
dept_df = dept_grp(employee_df)
dept_df.show()
new_df = row_data(spark)
new_df.show()
highest_sal = highest_salary(employee_df)
highest_sal.show()
multi_df = multi_action(employee_df)
multi_df.show()
