import duckdb

con = duckdb.connect("data/warehouse.duckdb")

con.execute("CREATE TABLE IF NOT EXISTS _test (id INTEGER, name VARCHAR)")
con.execute("INSERT INTO _test VALUES (1, 'hello')")
result = con.execute("SELECT * FROM _test").fetchall()
print(result)  # expect: [(1, 'hello')]

con.execute("DROP TABLE _test")
con.close()
print("DuckDB OK")