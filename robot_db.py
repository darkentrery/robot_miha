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
        data = (total['pnl_total'], total['rpl_total'], total['rpl_total_percent'])
        query = f"UPDATE {self.price_table} SET {set_query} pnl_total={data[0]}, rpl_total={data[1]}, rpl_total_percent={data[2]} WHERE id={candles[0]['id']}"
        self.cursor.execute(query)
        self.cnx.commit()

    def get_candles(self, launch):
        print("get_candles")
        if not launch.get('last_id'):
            last_id = 0
        else:
            last_id = launch['last_id']
        print(last_id + 1)
        try:
            query = f"SELECT id, time, close FROM {self.price_table} WHERE id <= {last_id + 1} order by id desc LIMIT 20"
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            candles = [{'id': r[0], 'time': r[1], 'price': r[2]} for r in rows]
            launch['last_id'] = last_id + 1
            return candles

        except Exception as e:
            print(e)
            self.cursor = self.get_db_connection()
            return self.get_candles(launch)

    def delete_pnl_from_price(self, launch):
        print("delete_equity_from_price")
        if launch['mode'] != 'tester':
            return
        set_query = ""
        for stream in launch['streams']:
            set_query = set_query + f"pnl_{stream['id']} = NULL,"

        query = f"UPDATE {self.price_table} SET {set_query} pnl_total = NULL, rpl_total = NULL, rpl_total_percent = NULL"
        self.cursor.execute(query)
        self.cnx.commit()


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
    pass

    def get_0_config(self, launch, data):
        query = f"SELECT tick_status, percent_level, last_tick_time, algo_1, algo_2 FROM {self.config_table} WHERE symbol = '{self.symbol}'"
        self.cursor.execute(query)
        rows = self.cursor.fetchone()
        launch['percent_level'] = str(rows[1])
        algorithm = [str(a) for a in rows[3:5] if int(a)]
        launch['streams'] = [{'algorithm_num': a, 'id': str(id + 1)} for id, a in enumerate(algorithm) if a]

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

    def db_get_state(self, launch):
        print("db_get_state")
        if launch['mode'] != 'robot':
            return False

        #launch_data = json.dumps(launch, default=json_serial)
        stat_data = json.dumps(stat, default=self.json_serial)

        try:
            query = f"SELECT trading_state FROM {self.config_table} WHERE symbol = '{self.symbol}'"
            self.cursor.execute(query)
            for stat_data in self.cursor:

                if stat_data == "null" or stat_data == None:

                    return False

            #launch_data = json.loads(launch_data, object_pairs_hook=load_with_datetime)
            stat_data = json.loads(stat_data, object_pairs_hook=self.load_with_datetime)
            #launch.update(launch_data)
            stat.update(stat_data)

            return True

        except Exception as e:
            print(e)
            return False

    def db_save_state(self, launch, stat):
        #launch_data = json.dumps(launch, default=json_serial)
        stat_data = json.dumps(stat, default=self.json_serial)

        if launch['mode'] != 'robot':
            return False

        stat_data = json.dumps(stat, default=self.json_serial)

        try:
            #update_query = (f"UPDATE {self.config_table} SET launch = %s, stat = %s where symbol = '{self.symbol}'")
            query = f"UPDATE {self.config_table} SET trading_state = %s WHERE symbol = '{self.symbol}'"
            #data = (launch_data, stat_data)
            self.cursor.execute(query, stat_data)

        except Exception as e:
            print(e)

# класс для работы с таблицами алгоритма
class Algo(Connector):

    def db_get_algorithm(self, stream):
        print("db_get_algorithm")
        try:
            query = f"SELECT * FROM {self.symbol}_{stream['algorithm']}"
            self.cursor.execute(query)

        except Exception as e:
            print('Ошибка получения таблицы с настройками, причина: ')
            print(e)
        rows = self.cursor.fetchall()
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
                        f"order_size DECIMAL (30, 8) NOT NULL, order_price DECIMAL (30, 2) NOT NULL, position_size DECIMAL (30, 8) NOT NULL," \
                        f" position_price DECIMAL (30, 2) NOT NULL, rpl DECIMAL (30, 8) NOT NULL, order_type ENUM('limit', 'market') NOT NULL);"
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
        parametrs = {}
        query = f"SELECT balance, leverage, order_price, order_size, position_price, position_size FROM {self.symbol}_pos_{stream['id']} order by id desc LIMIT 1"
        self.cursor.execute(query)
        for (parametrs['balance'], parametrs['leverage'], parametrs['price_order'], parametrs['size_order'],
             parametrs['price_position'],
             parametrs['size_position']) in self.cursor:
             return parametrs

        return parametrs

    def db_insert_position(self, stream, candle, parametrs):

        try:
            data = (
                parametrs['direction'],
                parametrs['balance'],
                candle['time'].strftime('%y/%m/%d %H:%M:%S'),
                parametrs['price_order'],
                parametrs['leverage'],
                parametrs['size_order'],
                parametrs['price_position'],
                parametrs['size_position'],
                parametrs['order_type'],
                parametrs['block_id'],
                parametrs['rpl']
            )

            query = f"INSERT INTO {self.symbol}_pos_{stream['id']} (side, balance, order_time, order_price, leverage, order_size, position_price, position_size, order_type, block_id, rpl) VALUES {data}"
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
        return rows

















