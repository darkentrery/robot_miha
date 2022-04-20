import ast
import time
import requests
import decimal


class Position():
    def __init__(self):
        self.start = False

    def first_start(self, balance, leverage, size_order, price_order, size_position, price_position, close):

        if 'balance' in self.__dir__():
            self.balance += self.rpl
        else:
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

    def update(self, leverage, close, close_0):
        leverage_0 = self.leverage
        order_leverage = leverage - self.leverage
        self.leverage = leverage
        #self.price_order = self.close
        self.price_order = close_0
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



def update_position(launch, stream, block, candles, position, pos):
    candle = candles[0]

    for action in block['actions']:
        local_stream = stream
        if 'stream' in action:
            for s in range(len(launch['streams'])):
                if launch['streams'][s]['id'] == str(action['stream']):
                    local_stream = launch['streams'][s]

        #stream['order'] = get_params(local_stream, block, action, candles, position, pos)
        local_stream['order'] = get_params(local_stream, block, action, candles, position, pos)
        stream['order']['block_id'] = local_stream['order']['block_id']
        local_stream['execute'] = True
        local_stream['execute_id'] = launch['last_id']
        pos.db_insert_position(local_stream, candle, local_stream['order'])

    #stream['order'] = get_params(stream, block, candles, position, pos)
    #pos.db_insert_position(stream, candle, stream['order'])


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


def get_params(stream, block, action, candles, position, pos):
    candle = candles[0]
    print(f"{candles=}")
    parametrs = pos.get_last_order(stream)
    #direction = stream['activation_blocks'][0]['actions'][0]['direction']
    direction = action['direction']
    print(f"{direction=}")
    print(f"{parametrs=}")

    if not position[stream['id']].start:
        parametrs['balance'] = stream['order']['balance']
        parametrs['leverage'] = float(0)
        parametrs['price_order'] = float(0)
        parametrs['size_order'] = float(0)
        parametrs['price_position'] = float(0)
        parametrs['size_position'] = float(0)
        parametrs['last'] = False
        position[stream['id']].first_start(parametrs['balance'], parametrs['leverage'], parametrs['price_order'],
                                                    parametrs['size_order'], parametrs['price_position'], parametrs['size_position'], candles[1]['price'])

    leverage = get_leverage(action, parametrs)

    print(f"{leverage=}")

    if direction == 'long':
        position[stream['id']].update(float(leverage), float(candle['price']), float(candles[1]['price']))
        params = position[stream['id']].update_long()
    elif direction == 'short':
        position[stream['id']].update(float(leverage), float(candle['price']), float(candles[1]['price']))
        params = position[stream['id']].update_short()

    params_name = ('balance', 'leverage', 'size_order', 'price_order', 'size_position', 'price_position', 'price',
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









