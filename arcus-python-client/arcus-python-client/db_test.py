from flask import Flask, request

from arcus import *
from arcus_mc_node import *

app = Flask(__name__)
app.secret_key = 'W0asf38r9sdsdjoq!@$89WX/,?RT'

mySQL_config = {"user": "root",
                "password": "1234",
                "database": "test_db",
                "host": "172.17.0.6"
                }

from mysql.connector.pooling import MySQLConnectionPool

mysql_pool = MySQLConnectionPool(pool_name=None, pool_size=4, pool_reset_session=True, **mySQL_config)

last_insert_key = None;

client = Arcus(ArcusLocator(ArcusMCNodeAllocator(ArcusTranscoder())))
client.connect("172.17.0.3:2181", "ruo91-cloud") 


@app.route('/mysql_select')
def mysql_select():
    connection = mysql_pool.get_connection()

    try:
        result = None;
        cursor = connection.cursor()
        key = request.args['key']

        query = "SELECT * FROM `test_table` WHERE `key`='%s'" % (key)
        queryResult = cursor.execute(query)
        result_data = cursor.fetchone()

        if result_data is not None and cursor.rowcount == 1:
            result = "Found value : " + result_data[1]
        else:
            result = "Not Found"
    except Exception as e:
        result = e.msg
    finally:
        if connection is not None:
            connection.close()

    return result


@app.route('/mysql_insert')
def mysql_insert():
    result = None;

    try:
        connection = mysql_pool.get_connection()
        cursor = connection.cursor()
        key = request.args['key']
        value = request.args['value']

        query = "INSERT INTO  `test_table` (`key`, `value`) VALUES ('%s', '%s')" % (key, value)
        queryResult = cursor.execute(query)
        connection.commit()
        result_data = cursor.fetchone()

        result = key + " inserted"
    except Exception as e:
        result = e.msg
    finally:
        if connection is not None:
            connection.close()

    return result


@app.route('/arcus_set')
def arcus_select():
    result = None;

    try:
        key = request.args['key']
        value = request.args['value']

        ret = client.set(key, value, 20)

        result = key + " set " + str(ret)
    except Exception as e:
        result = str(e)
    finally:
        pass

    return result


@app.route('/arcus_get')
def arcus_insert():
    result = None;

    try:
        key = request.args['key']
        ret = client.get(key)
        print(str(ret))
        result = ret.get_result()
    except Exception as e:
        result = str(e)
    finally:
        pass

    print("#### " + str(result));

    if result == None:
        result = "Not Found"

    return result


    return "asdf"


'''
@app.route('/nbase_arc_select')
def asdf():
    return "asdf"


@app.route('/nbase_arc_insert')
def asdf():ㅡ
    return "asdf"

'''

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True, threaded=True)
