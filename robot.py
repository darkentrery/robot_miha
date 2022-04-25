import ast
import decimal
import time

import robot_positions as positions
from robot_conditions import check_condition

from sys import argv
from robot_db import Config, Positions, Algo, Price, Summary
from loguru import logger
from robot_constants import launch, data, SYMBOL, robot_is_stoped
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

        self.position = {'1': positions.Position(), '2': positions.Position()}

    # наполнение массива launch
    def init_launch(self):
        algorithm_prefix = 'algo_'
        self.config.get_0_config(self.launch)

        self.launch['pnl_total'] = 0
        self.launch['rpl_total_percent'] = 0
        self.launch['rpl_total'] = 0

        for stream in self.launch['streams']:
            stream['algorithm'] = algorithm_prefix + str(stream['algorithm_num'])
            stream['order'] = self.get_null_order(None)

            host = 'http://' + data[f"host_exchange_{stream['id']}"]
            stream['url'] = stream.setdefault('url', host)

            algorithm_data = self.algo.db_get_algorithm(stream)
            blocks = self.convert_algorithm_data(algorithm_data)
            stream['blocks'] = blocks

            stream['was_close'] = False
            stream['was_open'] = False
            stream['action_block'] = None
            stream['activation_blocks'] = self.get_activation_blocks(stream['action_block'], stream['blocks'])
            print(f"{stream['activation_blocks']=}")
            if len(stream['activation_blocks']) == 0:
                raise Exception('There is no first block in startegy')

            # проверка наличия и создание таблицы позиций
            self.pos.create_table_positions(stream)

    # преобразование полученных данных из таблиц алгоритмов в форму словаря
    def convert_algorithm_data(self, algorithm_data):
        blocks = []
        for i, a in enumerate(algorithm_data):
            print(f"{a[2]=}")
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
            if len(self.candles) > 2:
                self.check_block(stream)
                if 'direction' in stream['order']:
                    self.position[stream['id']].update_pnl(float(self.candles[0]['price']), stream['order']['direction'])

    # проверка условий в блоке
    def check_block(self, stream):
        print("check_block")
        numbers = 3
        activation_blocks = stream['activation_blocks']
        bool_numbers = [False for _ in range(numbers)]

        # вначале проверяем условия по намберам
        for block in activation_blocks:
            for num in range(numbers):
                for condition in block['conditions']:
                    if 'number' in condition:
                        if check_condition(self.launch, condition, self.candles):
                            bool_numbers[num] = True
                        else:
                            bool_numbers[num] = False
                            break

                if bool_numbers[num]:
                    self.execute_action(stream, block)
                    return

        # если намберы не сработали проверяем остльные условия
        number = 1  # если намбер явно не объявлен, то принимается равным 1
        if not (True in bool_numbers):
            for block in activation_blocks:
                for condition in block['conditions']:
                    if not ('number' in condition):
                        if check_condition(self.launch, condition, self.candles):
                            bool_numbers[number] = True
                        else:
                            bool_numbers[number] = False
                            break
                if bool_numbers[number]:
                    self.execute_action(stream, block)
                    return

    # выполнение действия
    def execute_action(self, stream, block):
        print("execute_action")
        positions.update_position(self.launch, stream, block, self.candles, self.position)
        stream['activation_blocks'] = self.get_activation_blocks(block['id'], stream['blocks'])
        print(f"{stream['activation_blocks']=}")

    # запись параметров в таблицу прайс
    def set_parametrs(self):
        print("set_parametrs")
        if len(self.candles) < 1:
            return

        set_query = ""
        total = {'pnl_total': 0, 'rpl_total': 0, 'rpl_total_percent': 0}
        balance = 100  # !!!!!
        self.launch['pnl_total'] = 0
        for stream in self.launch['streams']:
            if self.position[stream['id']].start:
                launch['pnl_total'] += self.position[stream['id']].pnl
                total['pnl_total'] += self.position[stream['id']].pnl
                set_query += f"pnl_{stream['id']}={str(self.position[stream['id']].pnl)}, "
            else:
                set_query += f"pnl_{stream['id']}={str(0)}, "

        for stream in self.launch['streams']:
            if 'pnl' in self.position[stream['id']].__dir__() and 'rpl' in self.position[stream['id']].__dir__():
                self.launch['rpl_total'] += self.position[stream['id']].rpl
                self.launch['rpl_total_percent'] += 100 * self.position[stream['id']].rpl / balance
                self.position[stream['id']].rpl = 0

        total['rpl_total'] += self.launch['rpl_total']
        total['rpl_total_percent'] += self.launch['rpl_total_percent']

        self.price.set_pnl(set_query, total, self.candles)

        self.launch['last_id'] = self.launch['cur_id']

        print(f"{self.launch=}")


    # создание параметров для записи в прайс до момента срабатывания позиций
    def get_null_order(self, order):
        pass

    def check_exist_candles(self):
        pass

    def get_robot_status(self, robot_is_stoped):
        return False





class Robot(Bot):
    # создание параметров для записи в прайс до момента срабатывания позиций
    def get_null_order(self, order):
        pass
        return order

    def prepare_tables(self):
        print("prepare_tables")

    def get_robot_status(self, robot_is_stoped):
        return False

    def check_exist_candles(self):
        if not self.candles:
            time.sleep(1)
            log_condition(get_cur_time(), "wait tick")
        return True


class Tester(Bot):
    #def __init__(self, launch):
    #    Bot.__init__(launch)

    # создание параметров для записи в прайс до момента срабатывания позиций
    def get_null_order(self, order):
        sum = self.summary.get_summary()
        if not order:
            order = {}
            order['balance'] = sum[1] / sum[2]
            order['pnl'] = 0
            order['rpl'] = 0
        return order

    def prepare_tables(self):
        print("prepare_tables")
        # удаление информации из таблицы прайс
        self.price.delete_pnl_from_price(self.launch)

        # очистка таблицы позиций
        for stream in range(1, 3):
            self.pos.clear_table_positions(stream)

    def check_exist_candles(self):
        if not self.candles:
            print(f"В таблице price нет новых строк, последняя строка {self.launch['last_id']}")
            return False
        return True


def trade_loop(launch, robot_is_stoped):
    tester = Tester(launch)
    tester.init_launch()
    tester.prepare_tables()
    # цикл по прайс
    while True:
        if tester.get_robot_status(robot_is_stoped):
            continue

        tester.get_candles_from_table()

        if not tester.check_exist_candles():
            break

        # проверка условий и исполнение действий для каждого потока
        tester.check_conditions_in_streams()

        # запись значений в таблицу прайс
        tester.set_parametrs()


trade_loop(launch, robot_is_stoped)







