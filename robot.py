import ast
import decimal
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

        self.launch['rpl_total_percent'] = 0
        self.launch['rpl_total'] = 0
        null_order = self.get_null_order(None)

        for stream in self.launch['streams']:
            stream['order'] = null_order

            self.position[stream['id']] = positions.Position()

            host = 'http://' + data[f"host_exchange_{stream['id']}"]
            stream['url'] = stream.setdefault('url', host)

            if stream['algorithm']:
                algorithm_data = self.algo.db_get_algorithm(stream)
                blocks = self.convert_algorithm_data(algorithm_data)
                stream['blocks'] = blocks
                stream['action_block'] = None
                stream['activation_blocks'] = self.get_activation_blocks(stream['action_block'], stream['blocks'])

            stream['was_close'] = False
            stream['was_open'] = False

            # проверка наличия и создание таблицы позиций
            self.pos.create_table_positions(stream)

        self.launch['position'] = self.position

        result = self.config.db_get_state()
        print(f"{result=}")


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
            if len(self.candles) > 1 and stream['algorithm']:
                self.check_block(stream)

    # проверка условий в блоке
    def check_block(self, stream):
        print("check_block")
        bool = False
        activation_blocks = stream['activation_blocks']
        for block in activation_blocks:
            # создание/обновление нулевого блока с наберами
            if not ('numbers' in block):
                block['numbers'] = {}

            for condition in block['conditions']:
                if 'number' in condition:
                    if condition['number'] in block['numbers'] and block['numbers'][condition['number']] is not None:
                        block['numbers'][condition['number']] = 0
                    elif not (condition['number'] in block['numbers']):
                        block['numbers'][condition['number']] = 0

                elif not ('number' in condition):
                    if 1 in block['numbers'] and block['numbers'][1] is not None:
                        block['numbers'][1] = 0
                    elif not (1 in block['numbers']):
                        block['numbers'][1] = 0

            # записываем количество каждого набера в блоке
            for condition in block['conditions']:
                if 'number' in condition and block['numbers'][condition['number']] is not None:
                    block['numbers'][condition['number']] += 1
                elif not ('number' in condition) and block['numbers'][1] is not None:
                    block['numbers'][1] += 1


            block['numbers'] = dict(sorted(block['numbers'].items(), key=lambda x: x[0]))

        # проверка срабатывания всех условий для намберов по порядку
        for block in activation_blocks:
            for number in block['numbers']:
                if block['numbers'][number]:
                    for condition in block['conditions']:
                        if 'number' in condition and condition['number'] == number:
                            if check_condition(self.launch, condition, self.candles, stream):
                                block['numbers'][number] -= 1

                        elif not ('number' in condition) and number == 1:
                            if check_condition(self.launch, condition, self.candles, stream):
                                block['numbers'][1] -= 1

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
        stream['activation_blocks'] = self.get_activation_blocks(block['id'], stream['blocks'])
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
            if 'direction' in stream['order']:
                self.position[stream['id']].update_pnl(float(self.candles[0]['price']), stream['order']['direction'])

            if self.position[stream['id']].start:
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

    # запись состояния в таблицу 0_config
    def save_trading_state(self):
        total_balance = 0
        total_leverage = 0
        state = {}

        for stream in self.launch['streams']:
            total_balance += float(stream['order']['balance'])
            total_leverage += stream['order']['leverage']

        state['streams'] = self.launch['streams']
        state['total_balance'] = total_balance
        state['total_leverage'] = total_leverage
        state['last_id'] = self.launch['last_id']
        print(f"{state=}")
        #state = {'actions': [{'direction': 'long', 'leverage_up': 1}], 'conditions': [{'type': 'compare', 'fields': 'ai_1-h > ai_2-h'}]}
        self.config.save_state(state)


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
        print("get_null_order")
        sum = self.summary.get_summary()
        if not order:
            order = {}
            order['balance'] = float(sum[1] / sum[2])
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
    mode.prepare_tables()
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







