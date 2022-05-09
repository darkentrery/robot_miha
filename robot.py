import ast
import time

import robot_positions as positions
from robot_conditions import check_condition

from sys import argv
from robot_db import Config, Positions, Algo, Price, Summary
from loguru import logger
from robot_constants import launch, data, robot_is_stoped
from robot_trading import get_robot_status


logger.add("debug.log", format="{time} {level} {message}", level="DEBUG")


print('=============================================================================')

class Bot():
    def __init__(self, launch):
        self.launch = launch

        self.config = Config(self.launch)
        self.pos = Positions(self.launch)
        self.price = Price(self.launch)
        self.algo = Algo(self.launch)
        self.summary = Summary(self.launch)

        self.launch['config'] = self.config
        self.launch['pos'] = self.pos
        self.launch['price'] = self.price
        self.launch['algo'] = self.algo
        self.launch['summary'] = self.summary

        self.position = {}

    # наполнение массива launch
    def init_launch(self):
        self.config.get_streams(self.launch)
        self.prepare_tables()
        self.launch['last_id'] = 0

        for i, stream in enumerate(self.launch['streams']):
            self.position[stream['id']] = positions.Position()
            host = 'http://' + data[f"host_exchange_{stream['id']}"]
            stream['url'] = stream.setdefault('url', host)

            if stream['algorithm']:
                algorithm_data = self.algo.db_get_algorithm(stream)
                stream['blocks'] = self.convert_algorithm_data(algorithm_data)

                stream['trailing_id'] = None
                stream['max_price'] = None
                stream['first_1'] = None
                stream['second_1'] = None

                if not ('action_block' in stream):
                    stream['action_block'] = None
                    stream['activation_blocks'] = self.get_activation_blocks(stream['action_block'], stream['blocks'])

            stream['was_close'] = False
            stream['was_open'] = False

        self.get_state()
        for i, stream in enumerate(self.launch['streams']):
            null_order = self.get_null_order(stream)
            stream['order'] = null_order
            print(f"{stream['order']=}")

        self.launch['position'] = self.position

    def get_state(self):
        self.launch['rpl_total_percent'] = 0
        self.launch['rpl_total'] = 0

    # преобразование полученных данных из таблиц алгоритмов в форму словаря
    def convert_algorithm_data(self, algorithm_data):
        blocks = []
        for i, a in enumerate(algorithm_data):
            h = ast.literal_eval(a[2])
            activations = a[3].split(',')
            blocks.append({'id': str(a[0]), 'name': a[1], 'conditions': h['conditions'], 'actions': h['actions'],
                           'activations': activations})

        # задаем по умолчанию 'order_type': 'limit_refresh'
        for block in blocks:
            for action in block['actions']:
                if not 'order_type' in action:
                    action['order_type'] = 'limit_refresh'

        return blocks

    # получение списка активных блоков
    def get_activation_blocks(self, action_block, blocks):
        if not action_block:
            # activation_blocks = blocks
            activation_blocks = []
            for block in blocks:
                if '0' in block['activations']:
                    activation_blocks.append(block)
        else:
            activation_blocks = []
            for block in blocks:
                if block['id'] == action_block:
                    if not block['activations'][0]:
                        for b in blocks:
                            if '0' in b['activations']:
                                activation_blocks.append(b)
                    else:
                        for act in block['activations']:
                            for b in blocks:
                                if b['id'] == act:
                                    activation_blocks.append(b)

        return activation_blocks

    def get_candles_from_table(self):
        self.candles = self.price.get_candles(self.launch)

    def check_conditions_in_streams(self):
        for stream in self.launch['streams']:
            if 'direction' in stream['order']:
                self.position[stream['id']].update_pnl(self.candles[0]['price'], stream['order']['direction'])

        for stream in self.launch['streams']:
            if len(self.candles) > 1 and stream['algorithm']:
                self.check_block(stream)

    def set_default_numbers(self, stream):
        activation_blocks = stream['activation_blocks']
        for block in activation_blocks:
            # создание/обновление нулевого блока с наберами
            if not ('numbers' in block):
                block['numbers'] = {}

            for condition in block['conditions']:
                if 'number' in condition:
                    if str(condition['number']) in block['numbers'] and block['numbers'][
                        str(condition['number'])] is not None:
                        block['numbers'][str(condition['number'])] = 0
                    elif not (str(condition['number']) in block['numbers']):
                        block['numbers'][str(condition['number'])] = 0

                elif not ('number' in condition):
                    if '1' in block['numbers'] and block['numbers']['1'] is not None:
                        block['numbers']['1'] = 0
                    elif not ('1' in block['numbers']):
                        block['numbers']['1'] = 0

            # записываем количество каждого набера в блоке
            for condition in block['conditions']:
                if 'number' in condition and block['numbers'][str(condition['number'])] is not None:
                    block['numbers'][str(condition['number'])] += 1
                elif not ('number' in condition) and block['numbers']['1'] is not None:
                    block['numbers']['1'] += 1

            block['numbers'] = dict(sorted(block['numbers'].items(), key=lambda x: x[0]))


    # проверка условий в блоке
    def check_block(self, stream):
        print("check_block")
        bool = False
        activation_blocks = stream['activation_blocks']
        self.set_default_numbers(stream)

        # проверка срабатывания всех условий для намберов по порядку
        for block in activation_blocks:
            for number in block['numbers']:
                if block['numbers'][number]:
                    for condition in block['conditions']:
                        if 'number' in condition and str(condition['number']) == number:
                            if check_condition(self.launch, condition, self.candles, stream):
                                block['numbers'][number] -= 1

                        elif not ('number' in condition) and '1' == number:
                            if check_condition(self.launch, condition, self.candles, stream):
                                block['numbers']['1'] -= 1

            # проверка сработали ли все наберы в блоке, сработавший намбер равен 0, после чего присваивается None для следующих итераций
            for number in block['numbers']:
                if block['numbers'][number] == 0 and block['numbers'][number] is not None:
                    block['numbers'][number] = None
                    break
                elif block['numbers'][number] is None:
                    continue
                else:
                    break

            # если все намберы None то значит сработали
            for number in block['numbers']:
                if block['numbers'][number] is None:
                    bool = True
                else:
                    bool = False
                    break

            if bool:
                self.execute_action(stream, block)
                block['numbers'] = {}
                return

    # выполнение действия
    def execute_action(self, stream, block):
        print("execute_action")
        positions.update_position(self.launch, stream, block, self.candles, self.position)
        stream['action_block'] = block['id']
        stream['activation_blocks'] = self.get_activation_blocks(stream['action_block'], stream['blocks'])
        print(f"{stream['activation_blocks']=}")

    # запись параметров в таблицу прайс
    def set_parametrs(self):
        print("set_parametrs")
        if len(self.candles) < 1:
            return

        set_query = ""
        total = {'rpl_total': 0, 'rpl_total_percent': 0}
        balance = 100  # !!!!!
        for stream in self.launch['streams']:
            #if 'direction' in stream['order']:
            #    self.position[stream['id']].update_pnl(self.candles[0]['price'], stream['order']['direction'])

            if 'pnl' in self.position[stream['id']].__dir__():
                set_query += f"pnl_{stream['id']}={str(self.position[stream['id']].pnl)}, "
            else:
                set_query += f"pnl_{stream['id']}={str(0)}, "

        for stream in self.launch['streams']:
            if 'rpl' in self.position[stream['id']].__dir__():
                self.launch['rpl_total'] += self.position[stream['id']].rpl
                self.launch['rpl_total_percent'] += 100 * self.position[stream['id']].rpl / balance
                self.position[stream['id']].rpl = 0

        total['rpl_total'] += self.launch['rpl_total']
        total['rpl_total_percent'] += self.launch['rpl_total_percent']

        self.price.set_pnl(set_query, total, self.candles)

        self.launch['last_id'] = self.launch['cur_id']

        print(f"{self.launch=}")

    # запись состояния в таблицу 0_config
    def save_trading_state(self):
        pass

    # создание параметров для записи в прайс до момента срабатывания позиций
    def get_null_order(self, order):
        pass

    def prepare_tables(self):
        pass

    def check_exist_candles(self):
        pass

    def get_robot_status(self, robot_is_stoped):
        return False





class Robot(Bot):

    # создание параметров для записи в прайс до момента срабатывания позиций
    def get_null_order(self, stream):
        print("get_null_order")
        order = {}
        order['pnl'] = 0

        sum = self.summary.get_summary()
        parametrs = self.pos.get_last_order(stream)

        fields = self.price.get_for_state(self.launch)
        if fields is not None:
            order['pnl'] = fields[f"pnl_{stream['id']}"]

        if parametrs is not None:
            order['balance'] = parametrs['balance']
            order['leverage'] = parametrs['leverage']
            order['order_price'] = parametrs['order_price']
            order['order_size'] = parametrs['order_size']
            order['position_price'] = parametrs['position_price']
            order['position_size'] = parametrs['position_size']
            order['rpl'] = parametrs['rpl']
            if parametrs['position_size']:
                self.position[stream['id']].start = True
                self.position[stream['id']].balance = parametrs['balance']
                self.position[stream['id']].leverage = parametrs['leverage']
                self.position[stream['id']].order_price = parametrs['order_price']
                self.position[stream['id']].order_size = parametrs['order_size']
                self.position[stream['id']].position_price = parametrs['position_price']
                self.position[stream['id']].position_size = parametrs['position_size']
                self.position[stream['id']].rpl = parametrs['rpl']
                self.position[stream['id']].pnl = order['pnl']
                self.position[stream['id']].close = order['order_price']

        else:
            order['balance'] = sum[1] / sum[2]
            order['leverage'] = 0
            order['order_price'] = 0
            order['order_size'] = 0
            order['position_price'] = 0
            order['position_size'] = 0
            order['rpl'] = 0

        return order

    def prepare_tables(self):
        print("prepare_tables")
        if self.config.db_get_state() is None:
            # удаление информации из таблицы прайс
            self.price.delete_pnl_from_price(self.launch)

            # очистка таблицы позиций
            for stream in self.launch['streams']:
                # проверка наличия и создание таблицы позиций
                self.pos.create_table_positions(stream)
                self.pos.clear_table_positions(stream)

    def save_trading_state(self):
        total_balance = 0
        total_leverage = 0
        state = {}

        for stream in self.launch['streams']:
            total_balance += float(stream['order']['balance'])
            total_leverage += stream['order']['leverage']
            stream_id = f"stream_{stream['id']}"
            state[stream_id] = {}
            #state[stream_id]['activation_blocks'] = []
            if 'action_block' in stream:
                state[stream_id]['action_block'] = stream['action_block']
            else:
                state[stream_id]['action_block'] = None

            if 'activation_blocks' in stream:
                state[stream_id]['activation_blocks'] = [{'id': s['id']} for s in stream['activation_blocks']]
                for i, act in enumerate(stream['activation_blocks']):
                    if 'numbers' in act and len(act['numbers']):
                        for num in stream['activation_blocks'][i]['numbers']:
                            if stream['activation_blocks'][i]['numbers'][num] is None:
                                state[stream_id]['activation_blocks'][i]['conditions_number'] = num
                            else:
                                state[stream_id]['activation_blocks'][i]['conditions_number'] = None
                    else:
                        state[stream_id]['activation_blocks'][i]['conditions_number'] = None

                state[stream_id]['trailing_id'] = stream['trailing_id']
                state[stream_id]['max_price'] = stream['max_price']

        state['total_balance'] = total_balance
        state['total_leverage'] = total_leverage
        state['last_id'] = self.launch['last_id']
        print(f"{state=}")
        self.config.save_state(state)

    def get_state(self):
        self.launch['rpl_total_percent'] = 0
        self.launch['rpl_total'] = 0

        self.state = self.config.db_get_state()
        if self.state:
            self.launch['last_id'] = self.state['last_id']
            fields = self.price.get_for_state(self.launch)
            if fields is not None:
                self.launch['rpl_total_percent'] = fields['rpl_total_percent']
                self.launch['rpl_total'] = fields['rpl_total']

            for stream in self.launch['streams']:
                id =f"stream_{stream['id']}"
                stream['action_block'] = self.state[id]['action_block']
                if stream['algorithm']:
                    stream['activation_blocks'] = self.get_activation_blocks(stream['action_block'], stream['blocks'])
                    self.set_default_numbers(stream)

                if 'max_price' in self.state[id]:
                    stream['max_price'] = self.state[id]['max_price']
                if 'trailing_id' in self.state[id]:
                    stream['trailing_id'] = self.state[id]['trailing_id']

                if 'activation_blocks' in self.state[id]:
                    for block in stream['activation_blocks']:
                        for i in self.state[id]['activation_blocks']:
                            if block['id'] == i['id'] and i['conditions_number'] is not None:
                                for num in block['numbers']:
                                    if int(num) <= int(i['conditions_number']):
                                        block['numbers'][num] = None

        print(f"load_{self.state=}")


    def get_robot_status(self, robot_is_stoped):
        return False

    def check_exist_candles(self):
        if not self.candles:
            print(f"В таблице price нет новых строк, последняя строка {self.launch['last_id']}")
            return False
        return True


class Tester(Bot):

    # создание параметров для записи в прайс до момента срабатывания позиций
    def get_null_order(self, stream):
        print("get_null_order")
        sum = self.summary.get_summary()

        order = {}
        order['balance'] = sum[1] / sum[2]
        order['pnl'] = 0
        order['rpl'] = 0
        order['leverage'] = 0
        return order

    def prepare_tables(self):
        print("prepare_tables")
        # удаление информации из таблицы прайс
        self.price.delete_pnl_from_price(self.launch)

        # очистка таблицы позиций
        for stream in self.launch['streams']:
            # проверка наличия и создание таблицы позиций
            self.pos.create_table_positions(stream)
            self.pos.clear_table_positions(stream)

    def check_exist_candles(self):
        if not self.candles:
            print(f"В таблице price нет новых строк, последняя строка {self.launch['last_id']}")
            return False
        return True

@logger.catch
def trade_loop(launch, robot_is_stoped):
    if launch['mode'] == 'tester':
        mode = Tester(launch)
    elif launch['mode'] == 'robot':
        mode = Robot(launch)

    mode.init_launch()

    # цикл по прайс
    while True:
        if mode.get_robot_status(robot_is_stoped):
            continue

        mode.get_candles_from_table()

        if not mode.check_exist_candles():
            break

        # проверка условий и исполнение действий для каждого потока
        mode.check_conditions_in_streams()

        # запись значений в таблицу прайс
        mode.set_parametrs()

        mode.save_trading_state()



if __name__ == '__main__':
    trade_loop(launch, robot_is_stoped)







