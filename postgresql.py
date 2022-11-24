import psycopg2


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

    def DeleteDB(self, table: str, pk: dict) -> list:
        pk_key, pk_value = None, None
        for key, value in pk:
            pk_key = key
            pk_value = value
        query = f'delete {table} where {pk_key} = %s'
        self.cur.execute(query, [pk_value])
        return [query, pk_value]

    def Columns_Search(self, table: str) -> tuple:
        query = f'SELECT column_name from information_schema.columns WHERE table_name = %s'
        self.cur.execute(query, [table])
        return self.cur.fetchall()

    def commit(self) -> None:
        self.conn.commit()
