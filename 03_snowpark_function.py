from utils import get_snowpark_session, get_snowflake_connection
from snowflake.snowpark.functions import sum, year, udf

# This will become a Snowflake UDF when we register it!
def get_email_domain(email_address: str) -> str:
    if "@" not in email_address:
        return ""
    else:
        email_address.split("@")[1]


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

    # Call it via SQL
    print("Calling UDF via SQL to get Top 10 email domains")
    cursor = get_snowflake_connection().cursor()
    result = cursor.execute(
        """
        select 
          eb.get_email_domain(email) as email_domain, 
          count(*) as domain_count 
        from 
          eb.orders 
        group by 1 
        order by 2 desc 
        limit 10
    """
    ).fetchall()
    print(result)

    # Call it via Dataframe API
