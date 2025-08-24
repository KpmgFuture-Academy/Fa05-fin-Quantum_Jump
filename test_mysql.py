import mysql.connector

try:
    conn = mysql.connector.connect(
        host='localhost',
        port=3308,
        user='orda_user',
        password='1234',
        database='orda_news'
    )
    print('MySQL Connected Successfully')
    
    cursor = conn.cursor()
    cursor.execute('SHOW TABLES')
    tables = cursor.fetchall()
    print(f'Current tables: {tables}')
    
    conn.close()
    print('Connection closed')
    
except Exception as e:
    print(f'Connection failed: {e}')