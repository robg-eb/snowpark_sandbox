from utils import get_snowpark_session, get_snowflake_connection
from snowflake.snowpark.functions import call_udf, count, col

# This will become a Snowflake UDF when we register it!
def get_email_domain(email_address: str) -> str:
    if not email_address:
        return ""
    elif "@" not in email_address:
        return ""
    else:
        return email_address.split("@")[1]


def invoke_udf_with_sql():
    print("Calling UDF via SQL to get Top 10 email domains")
    cursor = get_snowflake_connection().cursor()
    result = cursor.execute(
        """
        select
          eb.get_email_domain(email) as email_domain,
          count(*) as domain_count
        from
          eb.orders
        where
          year(created) = 2022
        group by 1
        order by 2 desc
        limit 10
    """
    ).fetchall()
    print(result)


def invoke_udf_with_snowpark_client():
    print("Calling UDF via Snowpark Dataframe API to get Top 10 email domains")
    result = (
        session.table("eb.orders")
        .with_column("email_domain", call_udf("eb.get_email_domain", col("email")))
        .where("year(created) = 2022")
        .group_by("email_domain")
        .agg(count("*").as_("domain_count"))
        .sort(col("domain_count").desc())
        .limit(10)
    )
    result.show()


if __name__ == "__main__":
    session = get_snowpark_session()

    # Register the UDF
    print("Registering UDF, eb.get_email_domain")
    session.udf.register(
        func=get_email_domain,
        name="eb.get_email_domain",
        session=session,
        replace=True,
        is_permanent=True,
        stage_location="@DEV_UTIL.STAGES.S3_EXPORT_DATA",
    )
    print("Registered succesfully!")

    invoke_udf_with_sql()
    invoke_udf_with_snowpark_client()
