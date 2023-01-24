from utils import get_snowpark_session
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import udf, sum, col, array_construct, month, year, call_udf, lit
from snowflake.snowpark.types import Variant


def create_order_agg_table(session: Session) -> Variant:
    # Run a SQL query that aggregates table by Year
    orders = session.table("eb.orders")
    grouped = (
        orders.group_by(year("CREATED"))
        .agg(sum("GROSS_USD"))
        .with_column_renamed('"YEAR(CREATED)"', "YEAR")
        .with_column_renamed('"SUM(GROSS_USD)"', "TOTAL_SALES")
        .sort("YEAR")
    )
    print("First 20 rows of output are:")
    grouped.show(20)
    print(f"Query is {grouped.queries}")

    # Save results of SQL query into another Snowflake table
    grouped.write.mode("overwrite").save_as_table("eb.orders_by_year")
    print("Wrote to table eb.orders_by_year")

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
