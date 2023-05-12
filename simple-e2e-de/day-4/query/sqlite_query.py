from sqlalchemy import create_engine, text

def query_from_db(sql_long_string, engine):
    with engine.begin() as conn:
        conn.execute(text(sql_long_string))

engine = create_engine("sqlite:///../dags/data/db/invoice.db")

q = """
DELETE FROM invoice_product WHERE rowid>5
"""

try:
    query_from_db(q, engine)
    print("Success")
except Exception as err:
    print(err)