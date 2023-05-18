from multiprocessing import connection
from re import S
from sqlite3 import Cursor
from flask import Flask, request
from flask_cors import CORS
# import pyodbc
import psycopg2
import pymysql
import time
# import jsonify


def create_redshift_conn(dbname, host, port, user, password):
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            host=host,
            port=port,
            user=user,
            password=password)
        return conn
    except Exception as e:
        print(e)
        return None


def create_mysql_conn(host, port, user, password, dbname):
    try:
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            db=dbname,
        )
        return conn
    except Exception as e:
        print(e)
        return None


def execute_select_query(connection, query):
    try:
        curr = connection.cursor()
        res = curr.execute(query)
        headers = [i[0] for i in curr.description]
        details = curr.fetchall()
        connection.commit()
        return res, details, headers, None
    except Exception as e:
        return None, None, None, e


def execute_other_query(connection, query):
    try:
        curr = connection.cursor()
        res = curr.execute(query)
        connection.commit()
        return res, None, None, None
    except Exception as e:
        return None, None, None, e


app = Flask(__name__)
CORS(app)


@app.route('/executeQuery', methods=['POST'])
def executeQuery():
    data = request.get_json()
    query = data['query']
    db = data['checked']
    dataset = data['dbselect']
    query = query.replace('’', '\'')
    query = query.replace('‘', '\'')
    query = query.replace('"', '\'')
    print(query)
    if db == 'Redshift':
        dbname = 'dev'
        host = 'redshift-cluster-1.ceopdeohmgfe.us-east-1.redshift.amazonaws.com'
        port = 5439
        user = 'admin'
        password = '08021994Feb'
        if dataset == 'Instacart':
            query1 = 'SET search_path to PUBLIC;'
        else:
            query1 = 'SET search_path to ABC;'
        connection = create_redshift_conn(dbname, host, port, user, password)
    else:
        if dataset == 'Instacart':
            dbname = 'db1'
        else:
            dbname = 'dbAbc'
        host = 'db-1.cxi9nqaaoa0r.us-east-1.rds.amazonaws.com'
        port = 3306
        user = 'admin'
        password = '08021994feb'
        connection = create_mysql_conn(host, port, user, password, dbname)

    print(connection)
    st = time.time()
    if db == 'Redshift':
        res, details, headers, error = execute_other_query(connection, query1)
    if query.split()[0].lower() == 'show' or query.split()[0].lower() == 'select':
        res, details, headers, error = execute_select_query(connection, query)
    else:
        res, details, headers, error = execute_other_query(connection, query)

    et = time.time()

    rowData = []
    print(details)
    if details != None:
        for i in range(len(details)):
            tup = details[i]
            row = {}
            for j in range(len(headers)):
                if type(tup[j]) == type('str'):
                    text = tup[j]
                    row[headers[j]] = text
                else:
                    row[headers[j]] = tup[j]
            rowData.append(row)

    print(rowData)
    print(headers)

    total_time = et-st
    error = str(error)
    return {"res": res, "time": total_time, "details": rowData, "headers": headers, "error": error}
