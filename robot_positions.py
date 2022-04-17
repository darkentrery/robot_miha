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









