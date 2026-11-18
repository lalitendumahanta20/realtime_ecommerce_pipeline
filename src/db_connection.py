"""SQL Server connection factory configured through environment variables."""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import (DB_SERVER, DB_NAME, DB_USERNAME, DB_PASSWORD,
                    DB_DRIVER, DB_TRUSTED_CONNECTION)


def get_connection():
    try:
        import pyodbc
    except ImportError as exc:
        raise RuntimeError(
            "pyodbc is not installed. Run: pip install -r requirements.txt"
        ) from exc
    available = pyodbc.drivers()
    driver = DB_DRIVER or next(
        (name for name in ('ODBC Driver 18 for SQL Server', 'ODBC Driver 17 for SQL Server')
         if name in available), None
    )
    if not driver or driver not in available:
        raise RuntimeError(
            'SQL Server ODBC Driver 17/18 not found. '
            f'Available drivers: {available}. Set DB_DRIVER in .env.'
        )

    parts = [f'DRIVER={{{driver}}}', f'SERVER={DB_SERVER}', f'DATABASE={DB_NAME}',
             'TrustServerCertificate=yes']
    if DB_TRUSTED_CONNECTION:
        parts.append('Trusted_Connection=yes')
    else:
        if not DB_USERNAME or not DB_PASSWORD:
            raise RuntimeError('Set DB_USERNAME and DB_PASSWORD for SQL authentication.')
        parts.extend([f'UID={DB_USERNAME}', f'PWD={DB_PASSWORD}'])
    return pyodbc.connect(';'.join(parts) + ';', timeout=10)


if __name__ == '__main__':
    connection = get_connection()
    try:
        print('SQL Server connection successful:', connection.cursor().execute('SELECT DB_NAME()').fetchone()[0])
    finally:
        connection.close()
