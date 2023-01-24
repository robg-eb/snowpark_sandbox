from utils import get_snowpark_session
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import udf, sum, col, array_construct, month, year, call_udf, lit
from snowflake.snowpark.types import Variant


def create_order_agg_table(session: Session) -> Variant:
    # Create a table with orders by year
    print("Running table creation..")
    (
        session.table("eb.orders")
        .group_by(year("CREATED"))
        .agg(sum("GROSS_USD"))
        .with_column_renamed('"YEAR(CREATED)"', "ORDER_YEAR")
        .with_column_renamed('"SUM(GROSS_USD)"', "TOTAL_SALES")
        .write.mode("overwrite")
        .save_as_table("eb.orders_by_year")
    )
    print("Wrote to table eb.orders_by_year")

    session.table("eb.orders_by_year").sort("ORDER_YEAR").show(30)

    return {"status": "success"}


if __name__ == "__main__":
    session = get_snowpark_session()
    create_order_agg_table(session)

    # session.sproc.register(
    #     func=create_order_agg_table,
    #     name="eb.create_order_agg_table",
    #     is_permanent=True,
    #     replace=True,
    #     packages=["snowflake-snowpark-python"],
    #     stage_location="@DEV_UTIL.STAGES.S3_EXPORT_DATA",
    # )
    # print(session.call("eb.create_order_agg_table"))
