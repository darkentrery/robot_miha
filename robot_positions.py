import ast
import time
import requests


class Position():
    def __init__(self):
        self.start = False

    def first_start(self, balance, leverage, size_order, price_order, size_position, price_position, close):
        self.balance = float(balance)
        self.leverage = float(leverage)
        self.size_order = float(size_order)
        self.price_order = float(price_order)
        self.size_position = float(size_position)
        self.price_position = float(price_position)
        self.close = float(close)
        self.pnl = (self.close - self.price_position) * self.size_position
        self.rpl = 0
        self.start = True

    def update(self, leverage, close):
        leverage_0 = self.leverage
        order_leverage = leverage - self.leverage
        self.leverage = leverage
        self.price_order = self.close
        if order_leverage > 0:
            self.size_order = order_leverage * (self.balance + self.pnl) / self.price_order
            self.price_position += (self.price_order - self.price_position) * (
                        self.size_order / (self.size_position + self.size_order))
        elif order_leverage <= 0 and leverage_0 != 0:
            self.size_order = order_leverage * self.size_position / leverage_0
        else:
            self.size_order = 0

        self.balance += self.rpl
        if self.size_order < 0 and self.size_position != 0:
            self.rpl = self.pnl * (-self.size_order / self.size_position)
        else:
            self.rpl = 0
        self.size_position += self.size_order
        self.close = close

        if not leverage:
            self.start = False



    def update_long(self):
        self.pnl = (self.close - self.price_position) * self.size_position

        return self.balance, self.leverage, self.size_order, self.price_order, self.size_position, self.price_position,\
               self.close, self.pnl, self.rpl

    def update_short(self):
        self.pnl = (self.price_position - self.close) * self.size_position

        return self.balance, self.leverage, self.size_order, self.price_order, self.size_position, self.price_position,\
               self.close, self.pnl, self.rpl



def update_position(stream, block, candles, position, pos):
    candle = candles[0]
    stream['order'] = get_params(stream, block, candles, position, pos)
    pos.db_insert_position(stream, candle, stream['order'])


def get_leverage(block, parametrs):
    if 'leverage_up' in block['actions'][0]:
        leverage = parametrs['leverage'] + block['actions'][0]['leverage_up']
    elif 'leverage_down' in block['actions'][0]:
        leverage = 0
    elif 'leverage' in block['actions'][0]:
        leverage = block['actions'][0]['leverage']
    else:
        leverage = parametrs['leverage']
    return leverage


def get_params(stream, block, candles, position, pos):
    candle = candles[0]
    print(f"{candles=}")
    parametrs = pos.get_last_order(stream)
    #direction = stream['activation_blocks'][0]['actions'][0]['direction']
    direction = block['actions'][0]['direction']
    print(f"{direction=}")
    print(f"{parametrs=}")

    if not position[int(stream['id']) - 1].start:
        parametrs['balance'] = stream['order']['balance']
        parametrs['leverage'] = float(0)
        parametrs['price_order'] = float(0)
        parametrs['size_order'] = float(0)
        parametrs['price_position'] = float(0)
        parametrs['size_position'] = float(0)
        parametrs['last'] = False
        position[int(stream['id']) - 1].first_start(parametrs['balance'], parametrs['leverage'], parametrs['price_order'],
                                                    parametrs['size_order'], parametrs['price_position'], parametrs['size_position'], candles[1]['price'])

    leverage = get_leverage(block, parametrs)

    print(f"{leverage=}")

    if direction == 'long':
        position[int(stream['id']) - 1].update(float(leverage), float(candle['price']))
        params = position[int(stream['id']) - 1].update_long()
    elif direction == 'short':
        position[int(stream['id']) - 1].update(float(leverage), float(candle['price']))
        params = position[int(stream['id']) - 1].update_short()

    params_name = ('balance', 'leverage', 'size_order', 'price_order', 'size_position', 'price_position', 'price',
                   'pnl', 'rpl')
    params_dict = dict(zip(params_name, params))

    for k in params_dict:
        parametrs[k] = params_dict[k]
    parametrs['last'] = True
    parametrs['direction'] = direction
    parametrs['order_type'] = block['actions'][0]['order_type']
    parametrs['block_id'] = block['id']
    print(f"{stream['id']} {parametrs=}")

    return parametrs








#--------------old-----------------


def get_equity_many_robot(stream):

    try:
        
        http_data ='{"equity": "BTC"}'
        #http_data = '{"equity": "USDT"}'
        r = requests.post(stream['url'], data=http_data, timeout=7)
        if r.status_code != 200:
            raise Exception("Status code = " + str(r.status_code))
        equity = r.text.rstrip()
        print("equity = " + equity + ", time = " + str(datetime.datetime.utcnow()))
        return float(ast.literal_eval(equity))
    except Exception as e:
        time.sleep(2)
        print(e)
        print("equity exception" + ", time = " + str(datetime.datetime.utcnow()))
        return None

def send_leverage_many_robot(launch, stream):
    print("send_leverage_many_robot")
    print(f"{stream=}")

    if launch['mode'] != 'robot':
        return

    try:

        order = stream['order']
        price = equity[int(stream['id']) - 1][0].price_order
        quantity = equity[int(stream['id']) - 1][0].size_order
        #http_data='{"symbol":"' + stream['symbol'] + '","side":"' +  order['direction'] + '","leverage":"' + str(order['leverage']) + '","order_type":"' +  order['order_type'] + '"}'
        http_data = '{"symbol":"' + stream['balancing_symbol'] + '","side":"' + order['direction'] + '","leverage":"' + str(order['leverage']) + '","price":"' + str(price) + '","quantity":"' + str(quantity) + '","order_type":"' + order['order_type'] + '"}'
        #http_data = f"symbol:{stream['balancing_symbol']}, side:{order['direction']}, leverage:{order['leverage']}, price:0.9286, order_type:{order['order_type']}"
        #print(f"{http_data=}")
        #http_data = json.dumps(http_data, default=json_serial)
        print(f"{http_data=}")

        r = requests.post(stream['url'], data=http_data, timeout=7)
        if r.status_code != 200:
            raise Exception("Leverage Status code = " + str(r.status_code))
    except Exception as e:
        time.sleep(2)
        print(e)
        print("leverage send exception" + ", time = " + str(datetime.datetime.utcnow()))

def send_balancing_robot(launch, stream):
    print("send_balancing_robot")

    if launch['mode'] != 'robot':
       return

    try:
        http_data='{"equity_balancing":"' + stream['balancing_symbol'] + '"}'
        r = requests.post(stream['url'], data=http_data, timeout=7)
        if r.status_code != 200:
            raise Exception("Leverage Status code = " + str(r.status_code))

    except Exception as e:
        time.sleep(2)
        print(e)
        print("leverage send exception" + ", time = " + str(datetime.datetime.utcnow()))


def get_total_equity(launch, prev_candle, cursor):

    if launch['traiding_mode'] != 'many':
        return None

    if launch['mode'] == 'tester' and prev_candle != {}:
        query = "select id, total_equity from {0} where id={1}".format(launch['price_table_name'], prev_candle['id'])
        cursor.execute(query)
        for (id, total_equity) in cursor:
            return total_equity

    return None
