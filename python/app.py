import mysql.connector

conn = mysql.connector.connect(
    host="localhost",
    user="your_username",
    password="your_password",
    database="your_database"
)

cursor = conn.cursor()

cursor.execute("SELECT * FROM your_table")
results = cursor.fetchall()

for row in results:
    print(row)

cursor.close()
conn.close()