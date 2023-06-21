import pymysql

# Connect to DB1 for read the data
db1_host = '10.253.9.14'
db1_user = 'dbupdate'
db1_password = 'Rucasioadmin@1'
db1_database = 'picrm'

db1_connection = pymysql.connect(
    host=db1_host,
    user=db1_user,
    password=db1_password,
    database=db1_database
)

# Connect to DB2 to insert the data
db2_host = '10.253.20.100'
db2_user = 'dash'
db2_password = 'Rucasioadmin@1'
db2_database = 'picrm'

db2_connection = pymysql.connect(
    host=db2_host,
    user=db2_user,
    password=db2_password,
    database=db2_database
)
# Retrieve data from user_main table in DB1
db1_cursor = db1_connection.cursor()
db1_cursor.execute('SELECT * FROM table_source')
user_main_data = db1_cursor.fetchall()

# Iterate over user_main records
for record in user_main_data:
    # Check if the record already exists in Source table_1 in DB1
    db2_cursor = db2_connection.cursor()
    db2_cursor.execute('SELECT * FROM table_source WHERE pro_id = %s', record[0])
    existing_record = db2_cursor.fetchone()


    # If the record doesn't exist, insert it into Destination table_1 in DB2
    if existing_record is None:
        db2_cursor.execute('INSERT INTO table_destination (pro_id, user_id) VALUES (%s, %s)', record)
        db2_connection.commit()

# Close the database connections
db1_cursor.close()
db1_connection.close()

db2_cursor.close()
db2_connection.close()

# Print "task done" message
print("Task done")
