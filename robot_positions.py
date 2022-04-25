import ast
import time
import requests
import decimal


class Position():
    def __init__(self):
        self.start = False

    def first_start(self, balance, leverage, order_size, order_price, position_size, position_price, close):

        if 'balance' in self.__dir__():
            #self.balance += self.rpl
            pass
        else:
            self.balance = float(balance)

        self.leverage = float(leverage)
        self.order_size = float(order_size)
        self.order_price = float(order_price)
        self.position_size = float(position_size)
        self.position_price = float(position_price)
        self.close = float(close)
        self.pnl = 0 #(self.close - self.position_price) * self.position_size
        self.rpl = 0
        self.start = True

    def update_pnl(self, close_0, direction):
        if self.start:
            self.close = close_0
            if direction == 'long':
                self.pnl = (self.close - self.position_price) * self.position_size
            elif direction == 'short':
                self.pnl = (self.position_price - self.close) * self.position_size



    def update(self, leverage, close_0, balance):
        self.balance = balance

        leverage_0 = self.leverage
        order_leverage = leverage - self.leverage
        self.leverage = leverage
        self.order_price = close_0

        if order_leverage > 0:
            #self.order_size = order_leverage * (self.balance + self.pnl) / self.order_price
            self.order_size = order_leverage * self.balance / self.order_price
            self.position_price += (self.order_price - self.position_price) * (
                    self.order_size / (self.position_size + self.order_size))

        elif order_leverage <= 0 and leverage_0 != 0:
            self.order_size = order_leverage * self.position_size / leverage_0
        else:
            self.order_size = 0

        #self.balance += self.rpl

        if self.order_size < 0 and self.position_size != 0:
            self.rpl = self.pnl * (-self.order_size / self.position_size)
            self.balance_0 = self.balance
        else:
            self.rpl = 0

        self.position_size += self.order_size


        return self.balance, self.leverage, self.order_size, self.order_price, self.position_size, self.position_price, \
               self.close, self.pnl, self.rpl


def update_position(launch, stream, block, candles, position):
    candle = candles[0]

    for action in block['actions']:
        local_stream = stream
        if 'stream' in action:
            for s in range(len(launch['streams'])):
                if launch['streams'][s]['id'] == str(action['stream']):
                    local_stream = launch['streams'][s]

        local_stream['order'] = get_params(launch, local_stream, block, action, candles, position)
        stream['order']['block_id'] = local_stream['order']['block_id']
        launch['pos'].db_insert_position(local_stream, candle, local_stream['order'])


def get_leverage(action, parametrs):
    leverage_0 = decimal.Decimal(parametrs['leverage'])
    if 'leverage_up' in action:
        leverage = leverage_0 + action['leverage_up']
    elif 'leverage_down' in action:
        down = decimal.Decimal(action['leverage_down'].strip('%'))
        leverage = leverage_0 * (1 - down / 100)
    else:
        leverage = leverage_0

    if 'leverage_max' in action:
        if action['leverage_max'] < leverage:
            leverage = action['leverage_max']
    return leverage

def get_balance(launch, action, parametrs):
    balance = parametrs['balance']
    if 'balance' in action:
        up = decimal.Decimal(action['balance'].strip('%'))
        sum = launch['summary'].get_summary()
        balance = sum[1] / sum[2]
        balance = decimal.Decimal(balance) * up / 100

    return balance


def get_params(launch, stream, block, action, candles, position):
    candle = candles[0]
    print(f"{candles=}")
    parametrs = launch['pos'].get_last_order(stream)
    #direction = stream['activation_blocks'][0]['actions'][0]['direction']
    direction = action['direction']
    #print(f"{direction=}")
    #print(f"{parametrs=}")

    if not position[stream['id']].start:
        sum = launch['summary'].get_summary()
        #parametrs['balance'] = stream['order']['balance']
        parametrs['balance'] = sum[1] / sum[2]
        parametrs['leverage'] = float(0)
        parametrs['order_price'] = float(0)
        parametrs['order_size'] = float(0)
        parametrs['position_price'] = float(0)
        parametrs['position_size'] = float(0)
        #parametrs['last'] = False
        position[stream['id']].first_start(parametrs['balance'], parametrs['leverage'], parametrs['order_price'],
                                           parametrs['order_size'], parametrs['position_price'],
                                           parametrs['position_size'], candles[1]['price'])

    leverage = get_leverage(action, parametrs)
    if not leverage:
        position[stream['id']].start = False

    #print(f"{leverage=}")
    balance = get_balance(launch, action, parametrs)

    params = position[stream['id']].update(float(leverage), float(candles[1]['price']), float(balance))
    position[stream['id']].update_pnl(float(candles[0]['price']), direction)

    params_name = ('balance', 'leverage', 'order_size', 'order_price', 'position_size', 'position_price', 'price',
                   'pnl', 'rpl')
    params_dict = dict(zip(params_name, params))

    for k in params_dict:
        parametrs[k] = params_dict[k]
    parametrs['last'] = True
    parametrs['direction'] = direction
    parametrs['order_type'] = action['order_type']
    parametrs['block_id'] = block['id']
    print(f"{stream['id']} {parametrs=}")

    return parametrs









