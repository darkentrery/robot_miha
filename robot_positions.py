
class Position():
    def __init__(self):
        self.start = False

    def first_start(self, balance: float, leverage: float, order_size: float, order_price: float, position_size: float,
                    position_price: float, close: float):

        if 'balance' in self.__dir__():
            #self.balance += self.rpl
            pass
        else:
            self.balance = balance

        self.leverage = leverage
        self.order_size = order_size
        self.order_price = order_price
        self.position_size = position_size
        self.position_price = position_price
        self.close = close
        self.pnl = 0 #(self.close - self.position_price) * self.position_size
        self.rpl = 0
        self.start = True

    def update_pnl(self, close: float, direction):
        if self.start:
            self.close = close
            if direction == 'long':
                self.pnl = (self.close - self.position_price) * self.position_size
            elif direction == 'short':
                self.pnl = (self.position_price - self.close) * self.position_size
        else:
            self.pnl = 0
        print(f"{self.pnl=}")

    def update(self, leverage: float, close_0: float, balance):
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
               self.close, self.rpl


def update_position(launch, stream, block, candles, position):
    candle = candles[0]

    for action in block['actions']:
        local_stream = stream
        if 'stream' in action:
            for s in range(len(launch['streams'])):
                if launch['streams'][s]['id'] == str(action['stream']):
                    local_stream = launch['streams'][s]

        local_stream['order'] = get_params(launch, local_stream, block, action, candles, position, stream['id'])
        stream['order']['block_id'] = local_stream['order']['block_id']
        launch['pos'].db_insert_position(local_stream, candle, local_stream['order'])


def get_leverage(action, parametrs) -> float:
    leverage_0 = parametrs['leverage']
    if 'leverage_up' in action:
        leverage = leverage_0 + action['leverage_up']
    elif 'leverage_down' in action:
        down = float(action['leverage_down'].strip('%'))
        leverage = leverage_0 * (1 - down / 100)
    else:
        leverage = leverage_0

    if 'leverage_max' in action:
        if action['leverage_max'] < leverage:
            leverage = action['leverage_max']
    return leverage

def get_balance(launch, action, parametrs) -> float:
    balance = parametrs['balance']
    if 'balance' in action:
        up = float(action['balance'].strip('%'))
        sum = launch['summary'].get_summary()
        balance = sum[1] / sum[2]
        balance = balance * up / 100

    return balance


def get_params(launch, stream, block, action, candles, position, id):
    can = 0
    print(f"{candles=}")
    #parametrs = launch['pos'].get_last_order(stream)
    direction = action['direction']
    parametrs = {}
    parametrs['balance'] = stream['order']['balance']
    parametrs['leverage'] = stream['order']['leverage']

    if not position[stream['id']].start:
        parametrs['balance'] = stream['order']['balance']
        parametrs['leverage'] = stream['order']['leverage']
        parametrs['order_price'] = float(0)
        parametrs['order_size'] = float(0)
        parametrs['position_price'] = float(0)
        parametrs['position_size'] = float(0)
        position[stream['id']].first_start(parametrs['balance'], parametrs['leverage'], parametrs['order_price'],
                                           parametrs['order_size'], parametrs['position_price'], parametrs['position_size'],
                                           candles[can]['price'])

    leverage = get_leverage(action, parametrs)

    balance = get_balance(launch, action, parametrs)

    position[stream['id']].update_pnl(candles[can]['price'], direction)

    params = position[stream['id']].update(leverage, candles[can]['price'], balance)

    if not leverage:
        position[stream['id']].start = False

    params_name = ('balance', 'leverage', 'order_size', 'order_price', 'position_size', 'position_price', 'price', 'rpl')
    params_dict = dict(zip(params_name, params))

    for k in params_dict:
        parametrs[k] = params_dict[k]
    parametrs['pnl'] = position[stream['id']].pnl
    parametrs['direction'] = direction
    parametrs['order_type'] = action['order_type']
    parametrs['block_id'] = f"{id}_{block['id']}"
    parametrs['candle_id'] = candles[0]['id']
    print(f"{stream['id']} {parametrs=}")

    return parametrs









