import psycopg2
from config import host, database, user, password


def conectDB():
    conn = None
    try:

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password)
		
        # create a cursor
        cur = conn.cursor()
        
	# execute a statement
        print('Conected to')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)
       
	# close the communication with the PostgreSQL
        return cur
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return None


def insert(tablename:str, values):
    cur = None

    try:
        cur = conectDB()

        if cur is None:
            return
        
        sql = None

        if tablename == "rtpi_store_id":
            sql = "INSERT INTO rtpi_store_id (store_id, store_name) \
            VALUES (%s,%s) \
            ON CONFLICT (store_id) DO UPDATE \
            SET store_name = excluded.store_name ;"
        if tablename == "rtpi_price_page":
            sql = "INSERT INTO rtpi_price_page (web_price_id, price_name, price_url, date_add, date_last_crawl, date_last_in_stock, rosstat_id, contributor_id, store_id) \
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) \
            ON CONFLICT (web_price_id) DO UPDATE \
            SET price_name = excluded.price_name, \
            price_url = excluded.price_url,\
            date_add = excluded.date_add,\
            date_last_in_stock = excluded.date_last_in_stock,\
            rosstat_id = excluded.rosstat_id,\
            contributor_id = excluded.contributor_id,\
            store_id = excluded.store_id;"
        if tablename == "rtpi_product_name":
            sql = "INSERT INTO rtpi_product_name (web_price_id, product_name, contributor_id, moment) \
            VALUES (%s,%s,%s,%s) \
            ON CONFLICT (web_price_id) DO UPDATE \
            SET product_name = excluded.product_name, \
            contributor_id = excluded.contributor_id,\
            moment = excluded.moment;"
        else :
            print ("Unknow table!!!")

        cur.executemany(sql,values)

        cur.connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
    finally:
       if cur is not None:
           cur.close()

