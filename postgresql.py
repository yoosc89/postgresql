import pandas as pd
import os
import sys
import psycopg2
from datetime import datetime
import json


class PostgerSQL:
    def __init__(self, host: str, user: str, password: str, dbname: str, port: int) -> None:
        self.conn = psycopg2.connect(
            host=host, user=user, password=password, dbname=dbname, port=port)
        self.cur = self.conn.cursor()

    def ReadDB(self, table: str, columns: str = '', where: str = '', order_by: str = '', limit: int = 100) -> list:
        columns = columns if columns.strip() else '*'
        where = 'where '+where if where.strip() else ''
        order_by = f'order by {order_by}' if order_by.strip() else ''
        limit = f'limit {limit}'
        query = f'select {columns} from {table} {where} {order_by} {limit};'
        self.cur.execute(query)
        return self.cur.fetchall()

    def CreateDB(self, table: str, data: list = [], columns: list = []) -> list:
        values = ['%s']*len(columns)
        columns = '('+','.join(columns)+')' if len(columns) else ""
        values = ','.join(values)
        query = f'insert into {table}{columns} values ({values});'
        self.cur.execute(query, data)
        feedback = (query, data)
        return feedback

    def UpdateDB(self, table: str, pk: dict, columns: list, data: list = []) -> list:
        pk_key = None

        for key, value in pk.items():
            pk_key = key
            data.append(value)

        columns = ','.join([f'{col}=%s' for col in columns])

        query = f'update {table} set {columns} where {pk_key}=%s'
        self.cur.execute(query, data)
        feedback = (query, data)
        return feedback

    def commit(self) -> None:
        self.conn.commit()


if __name__ == '__main__':
    account = None
    with open('postgres_host_info.json', 'r') as json_data:
        account = json.load(json_data)

    sql = PostgerSQL(host=account['host'], user=account['user'],
                     password=account['password'], dbname=account['dbname'], port=account['port'])
    # select = sql.ReadDB(table='test',
    #                    order_by='id asc', limit=200,)
    columns = ['text', 'create_data', 'name', 'user_id', 'text1']  # 테스트용
    data = ['d1212fdf', datetime.now(), 'nam1112e', 15, 'dddddddd']  # 테스트용

    #sql.CreateDB(table='test', columns=columns, data=data)

    pk = {'id': 11196}

    sql.UpdateDB(table='test', pk=pk,
                 columns=columns, data=data)

    sql.commit()
