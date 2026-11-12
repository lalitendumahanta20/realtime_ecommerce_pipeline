
import pyodbc

def get_connection():
    # Detect an installed SQL Server ODBC driver
    available_drivers = pyodbc.drivers()

    preferred_drivers = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server"
    ]

    driver = next(
        (d for d in preferred_drivers if d in available_drivers),
        None
    )

    if driver is None:
        raise RuntimeError(
            "SQL Server ODBC Driver 17 or 18 not found. "
            f"Available drivers: {available_drivers}"
        )

    connection_string = (
        f"DRIVER={{{driver}}};"
        "SERVER=localhost;"
        "DATABASE=RealtimeEcommerceDB;"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )

    connection = pyodbc.connect(connection_string, timeout=10)
    return connection


if __name__ == "__main__":
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT DB_NAME()")
        database_name = cursor.fetchone()[0]

        print("SQL Server connection successful!")
        print("Connected database:", database_name)

        cursor.close()
        conn.close()

    except Exception as error:
        print("Database connection failed:", error)
