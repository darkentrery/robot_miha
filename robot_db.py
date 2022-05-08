import decimal

import pymysql
import time
import json
import datetime



# родительский класс, дает всем остальным объект курсора и коннектора
class Connector:
    def __init__(self, launch):
        self.user = launch['db']['user']
        self.password = launch['db']['password']
        self.host = launch['db']['host']
        self.database = launch['db']['database']
        self.symbol = launch['db']['pair']
        self.config_table = launch['config_table']
        self.price_table = launch['price_table_name']
        self.summary_table = launch['summary_table']

        self.cursor = self.get_db_connection()

    def get_db_connection(self):
        while True:
            try:
                self.cnx = pymysql.connect(user=self.user, password=self.password, host=self.host,
                                              database=self.database, connect_timeout=2)
                self.cnx.autocommit = True
                self.cursor = self.cnx.cursor()
                return self.cursor

            except Exception as e:
                time.sleep(2)
                print(e)


# класс для работы с таблицей цен
class Price(Connector):

    def set_pnl(self, set_query, total, candles):
        print("set_pnl")
        query = f"UPDATE {self.price_table} SET {set_query} rpl_total={total['rpl_total']}, rpl_total_percent={total['rpl_total_percent']} WHERE id={candles[0]['id']}"
        self.cursor.execute(query)
        self.cnx.commit()

    def get_candles(self, launch):
        print("get_candles")
        if not launch.get('last_id'):
            last_id = 0
        else:
            last_id = launch['last_id']

        try:
            query = f"SELECT id FROM {self.price_table} WHERE id > {last_id} order by id asc LIMIT 1"
            self.cursor.execute(query)
            row = self.cursor.fetchone()
            if row is None:
                return False

            launch['cur_id'] = row[0]
            print(launch['cur_id'])
            query = f"SELECT id, time, close FROM {self.price_table} WHERE id <= {launch['cur_id']} order by id desc LIMIT 20"
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            candles = [{'id': r[0], 'time': r[1], 'price': float(r[2])} for r in rows]
            return candles

        except Exception as e:
            print(e)
            self.cursor = self.get_db_connection()
            return self.get_candles(launch)

    def delete_pnl_from_price(self, launch):
        print("delete_pnl_from_price")
        if launch['mode'] != 'tester':
            return
        set_query = [f"pnl_{stream['id']} = NUll" for stream in launch['streams']]
        set_query = ", ".join(set_query)

        query = f"UPDATE {self.price_table} SET {set_query}, rpl_total = NULL, rpl_total_percent = NULL"
        self.cursor.execute(query)
        self.cnx.commit()

    def get_for_compare(self, launch):
        query = f"SHOW COLUMNS FROM {self.database}.{self.price_table}"
        self.cursor.execute(query)
        row = self.cursor.fetchall()
        columns = [r[0] for r in row]
        query = f"SELECT * FROM {self.price_table} WHERE id = {launch['cur_id']} order by id desc LIMIT 1"
        self.cursor.execute(query)
        row = self.cursor.fetchone()
        fields = None
        if row is not None:
            fields = dict(zip(columns, row))
            for field in fields:
                if type(fields[field]) is decimal.Decimal:
                    fields[field] = float(fields[field])

        return fields

    def get_for_state(self, launch):
        query = f"SHOW COLUMNS FROM {self.database}.{self.price_table}"
        self.cursor.execute(query)
        row = self.cursor.fetchall()
        columns = [r[0] for r in row]
        query = f"SELECT * FROM {self.price_table} WHERE id = {launch['last_id']} order by id desc LIMIT 1"
        self.cursor.execute(query)
        row = self.cursor.fetchone()
        fields = None
        if row is not None:
            fields = dict(zip(columns, row))
            for field in fields:
                if type(fields[field]) is decimal.Decimal:
                    fields[field] = float(fields[field])

        return fields


# класс для работы с таблицей конфигурации
class Config(Connector):

    def load_with_datetime(self, pairs, format='%Y-%m-%dT%H:%M:%S'):
        """Load with dates"""
        d = {}
        for k, v in pairs:
            ok = False
            try:
                d[k] = datetime.datetime.strptime(v, format).date()
                ok = True
            except:
                d[k] = v
            if ok == False:
                try:
                    d[k] = datetime.datetime.strptime(v, '%Y-%m-%dT%H:%M:%S.%f').date()
                except:
                    d[k] = v
        return d

    def json_serial(self, obj):

        """JSON serializer for objects not serializable by default json code"""

        if isinstance(obj, datetime.datetime):
            return obj.isoformat()


    def get_streams(self, launch):
        query = f"SHOW COLUMNS FROM {self.database}.{self.config_table}"
        self.cursor.execute(query)
        row = self.cursor.fetchall()
        columns = [r[0] for r in row if 'stream' in r[0][:7]]
        streams = ', '.join(columns)
        query = f"SELECT {streams} FROM {self.config_table} WHERE symbol = '{self.symbol}'"
        self.cursor.execute(query)
        rows = self.cursor.fetchone()
        algorithms = dict(zip(columns, rows))
        print(f"{streams=}")
        launch['streams'] = [{'algorithm': algorithms[a], 'id': str(a.split('_')[1])} for a in algorithms]

        print(f"{launch=}")

    def get_trading_status(self):
        try:
            query = "SELECT trading_status FROM {self.config_table} WHERE symbol = '{self.symbol}'"
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            for (trading_status) in result:
                return trading_status
            return 'on'
        except Exception as e:
            print(e)
            self.cursor = self.get_db_connection()
            return self.get_trading_status()

    def db_clear_state(self):
        try:
            query = f"UPDATE {self.config_table} SET trading_state = None WHERE symbol = '{self.symbol}'"
            self.cursor.execute(query)
            print("Контекст очищен")
        except Exception as e:
            print(e)

    def db_get_state(self):
        print("db_get_state")
        try:
            query = f"SELECT trading_state FROM {self.config_table} WHERE symbol = '{self.symbol}'"
            self.cursor.execute(query)
            result = self.cursor.fetchone()[0]
            if result is not None:
                result = json.loads(result)
            return result

        except Exception as e:
            print(e)
            return False

    def save_state(self, state):
        state_data = json.dumps(state, default=self.json_serial, ensure_ascii=False)
        try:
            query = f"UPDATE {self.config_table} SET trading_state = '{state_data}' WHERE symbol = '{self.symbol}'"
            self.cursor.execute(query)
            self.cnx.commit()

        except Exception as e:
            print(e)



# класс для работы с таблицами алгоритма
class Algo(Connector):

    def db_get_algorithm(self, stream):
        print("db_get_algorithm")
        try:
            query = f"SELECT * FROM {stream['algorithm']}"
            self.cursor.execute(query)
            rows = self.cursor.fetchall()

        except Exception as e:
            print('Ошибка получения таблицы с настройками, причина: ')
            print(e)

        return rows


# класс для работы с таблицей позиций
class Positions(Connector):

    def create_table_positions(self, stream):
        try:
            query = f"SELECT * FROM information_schema.tables WHERE table_name = '{self.symbol}_pos_{stream['id']}'"
            self.cursor.execute(query)
            if not self.cursor.fetchone():
                query = f"CREATE TABLE IF NOT EXISTS {self.symbol}_pos_{stream['id']} (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, block_id VARCHAR (20) NOT NULL," \
                        f" side ENUM('long', 'short') NOT NULL, balance DECIMAL (30, 8) NOT NULL, leverage DECIMAL (30, 2) NOT NULL, order_time DATETIME NOT NULL," \
                        f"order_size DECIMAL (30, 8) NOT NULL, order_price DECIMAL (30, 4) NOT NULL, position_size DECIMAL (30, 8) NOT NULL," \
                        f" position_price DECIMAL (30, 4) NOT NULL, rpl DECIMAL (30, 8) NOT NULL, order_type ENUM('limit', 'market', 'limit_refresh') NOT NULL);"
                self.cursor.execute(query)
                self.cnx.commit()

        except Exception as e:
            print(f"create_position {e}")

    def clear_table_positions(self, stream):
        try:
            query = f"TRUNCATE TABLE {self.symbol}_pos_{stream['id']}"
            self.cursor.execute(query)
            print("trunc")
        except Exception as e:
            print(e)

    def get_last_order(self, stream):
        query = f"SHOW COLUMNS FROM {self.database}.{self.symbol}_pos_{stream['id']}"
        self.cursor.execute(query)
        row = self.cursor.fetchall()
        columns = [r[0] for r in row]
        query = f"SELECT * FROM {self.symbol}_pos_{stream['id']} order by id desc LIMIT 1"
        self.cursor.execute(query)
        row = self.cursor.fetchone()
        parametrs = None
        if row is not None:
            parametrs = dict(zip(columns, row))
            for parametr in parametrs:
                if type(parametrs[parametr]) is decimal.Decimal:
                    parametrs[parametr] = float(parametrs[parametr])
        return parametrs

    def db_insert_position(self, stream, candle, parametrs):

        try:
            data = (
                parametrs['block_id'],
                parametrs['direction'],
                parametrs['balance'],
                parametrs['leverage'],
                candle['time'].strftime('%y/%m/%d %H:%M:%S'),
                parametrs['order_size'],
                parametrs['order_price'],
                parametrs['position_size'],
                parametrs['position_price'],
                parametrs['rpl'],
                parametrs['order_type']
            )

            query = f"INSERT INTO {self.symbol}_pos_{stream['id']} (block_id, side, balance, leverage, order_time, order_size, order_price, position_size, position_price, rpl, order_type) VALUES {data}"
            self.cursor.execute(query)
            self.cnx.commit()

        except Exception as e:
            print(e)
            self.cursor = self.get_db_connection()
            self.db_insert_position(stream, candle, parametrs)


# класс для работы с таблицей 0_summary
class Summary(Connector):

    def get_summary(self):
        print("get_summary")
        try:
            query = f"SELECT * FROM {self.summary_table}"
            self.cursor.execute(query)

        except Exception as e:
            print('Ошибка получения таблицы с настройками, причина: ')
            print(e)
        rows = self.cursor.fetchone()
        summary = []
        for i, row in enumerate(rows):
            if type(row) is decimal.Decimal:
                summary.append(float(row))
            else:
                summary.append(row)
        return summary

















