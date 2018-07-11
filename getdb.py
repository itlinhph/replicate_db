import mysql.connector

NUM_ROWS_INSERT = 1000

# Connect to DB report
CONFIG_DB_LOCAL = {
    'user': 'linhph',
    'password': '',
    'host': 'localhost',
    'database': 'ghtk',
}

CONFIG_DB_REPORT = {
    'user': '***',
    'password': '***',
    'host': '***',
    'port': '***',
    'database': '***',
}


def getdb(cnx, startrow, numrows):
    query = "select * from ghtk.addresses limit " + str(startrow) + "," + str(numrows)
    cnx = mysql.connector.connect(**CONFIG_DB_REPORT)
    cursor = cnx.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    return result

def insertdb(cnx, data):
    query = "INSERT INTO ghtk.log_test VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    cnx = mysql.connector.connect(**CONFIG_DB_LOCAL)
    cursor = cnx.cursor()
    cursor.executemany(query, data)
    cnx.commit()

def countdb(cnx):
    query = "select count(*) from ghtk.addresses"
    cnx = mysql.connector.connect(**CONFIG_DB_REPORT)
    cursor = cnx.cursor()
    cursor.execute(query)
    (num_rows,) = cursor.fetchone()

    return num_rows

def insert_db(cnx, startrow, numrows):
    query = "INSERT INTO ghtk.log_test SELECT * from ghtk.addresses limit " + str(startrow) + "," + str(numrows)
    cnx = mysql.connector.connect(**CONFIG_DB_LOCAL)
    cursor = cnx.cursor()
    cursor.execute(query)
    cnx.commit()


def main():

    cnx =  mysql.connector.connect(**CONFIG_DB_LOCAL)
    allrows = countdb(cnx)
    row_processing = 0
    while (row_processing < allrows):
        insert_db(cnx, row_processing, NUM_ROWS_INSERT)
        row_processing += NUM_ROWS_INSERT

    cnx.close()

main()
