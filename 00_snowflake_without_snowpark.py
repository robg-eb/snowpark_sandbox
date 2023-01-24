from utils import get_snowflake_connection

if __name__ == "__main__":
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    # Create a table with orders by year
    print("Running table creation..")
    result = cursor.execute(
        """
        create or replace table eb.orders_by_year as 
          select 
            year(created) as order_year,
            sum(gross_usd) as total_sales
          from
            eb.orders
          group by 
            year(created)
    """
    )
    rows = result.fetchall()
    print(rows)

    # Now show the rows from the table
    result = cursor.execute("select * from eb.orders_by_year order by order_year")
    rows = result.fetchall()
    for row in rows:
        print(f"Year: {row[0]}, Total_Sales: ${row[1]}")
