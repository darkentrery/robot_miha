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
    pass

class Robot(Bot):
    pass

class Tester(Bot):
    pass

#---------------for first launch------------------------------

# создание параметров для записис в прайс до момента срабатывания позиций
def get_null_order(order, launch, summary):
    sum = summary.get_summary()

    if launch['mode'] == 'robot':
        pass

    elif launch['mode'] == 'tester':
        if not order:
            order = {}
            order['balance'] = sum[1] / sum[2]
            order['pnl'] = 0
            order['rpl'] = 0
    return order

# наполнение массива launch
def init_launch(launch, config, summary):
    algorithm_prefix = 'algo_'
    config.get_0_config(launch, data)

    launch['pnl_total'] = 0
    launch['rpl_total_percent'] = 0
    launch['rpl_total'] = 0


    for stream in launch['streams']:
        stream['algorithm'] = algorithm_prefix + str(stream['algorithm_num'])
        stream['order'] = get_null_order(None, launch, summary)
        stream['execute'] = False # переменная для проверки срабатывания условий, требуется чтобы корректно писать rpl_total_percent
        host = 'http://' + data[f"host_exchange_{stream['id']}"]
        stream['url'] = stream.setdefault('url', host)
        stream['balancing_symbol'] = stream.setdefault('balancing_symbol', SYMBOL)



# преобразование полученных данных из таблиц алгоритмов в форму словаря
def convert_algorithm_data(algorithm_data):
    blocks = []
    for i, a in enumerate(algorithm_data):
        h = ast.literal_eval(a[2])
        activations = a[3].split(',')
        blocks.append({'id': str(a[0]), 'name': a[1], 'conditions': h['conditions'], 'actions': h['actions'], 'activations': activations})

    # задаем по умолчанию 'order_type': 'limit_refresh'
    for block in blocks:
        for action in block['actions']:
            if not 'order_type' in action:
                action['order_type'] = 'limit_refresh'

    return blocks

# получение списка активных блоков
def get_activation_blocks(action_block, blocks):
    if not action_block:
        #activation_blocks = blocks
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

# активация стримов, подготовка таблиц
def init_algo(launch, price, algo, pos):
    print("init_algo")
    # удаление информации из таблицы прайс
    price.delete_pnl_from_price(launch)

    for stream in launch['streams']:
        algorithm_data = algo.db_get_algorithm(stream)
        blocks = convert_algorithm_data(algorithm_data)
        stream['blocks'] = blocks

        stream['was_close'] = False
        stream['was_open'] = False
        stream['action_block'] = None
        stream['activation_blocks'] = get_activation_blocks(stream['action_block'], stream['blocks'])
        print(f"{stream['activation_blocks']=}")
        if len(stream['activation_blocks']) == 0:
            raise Exception('There is no first block in startegy')

        # проверка наличия и создание таблицы позиций
        pos.create_table_positions(stream)
        #if launch['mode'] == 'robot':
        if launch['mode'] != 'robot':
            # очистка таблицы позиций
            pos.clear_table_positions(stream)

#---------------------------functions for every ticks------------------------


# выполнение действия
def execute_action(launch, stream, block, candles, position, pos):
    print("execute_action")
    positions.update_position(launch, stream, block, candles, position, pos)
    stream['activation_blocks'] = get_activation_blocks(block['id'], stream['blocks'])
    print(f"{stream['activation_blocks']=}")


# проверка условий в блоке
def check_block(launch, stream, candles, position, pos):
    print("check_block")
    numbers = 3
    activation_blocks = stream['activation_blocks']
    bool_numbers = [False for _ in range(numbers)]

    # вначале проверяем условия по намберам
    for block in activation_blocks:
        for num in range(numbers):
            for condition in block['conditions']:
                if 'number' in condition:
                    if check_condition(launch, condition, candles, position[stream['id']]):
                        bool_numbers[num] = True
                    else:
                        bool_numbers[num] = False
                        break

            if bool_numbers[num]:
                execute_action(launch, stream, block, candles, position, pos)
                #stream['execute'] = True
                return

    # если намберы не сработали проверяем остльные условия
    number = 1 # если намбер явно не объявлен, то принимается равным 1
    if not (True in bool_numbers):
        for block in activation_blocks:
            for condition in block['conditions']:
                if not ('number' in condition):
                    if check_condition(launch, condition, candles, position[stream['id']]):
                        bool_numbers[number] = True
                    else:
                        bool_numbers[number] = False
                        break
            if bool_numbers[number]:
                execute_action(launch, stream, block, candles, position, pos)
                #stream['execute'] = True
                return




# запись параметров в таблицу прайс
def set_parametrs(launch, candles, price, position):
    print("set_parametrs")
    if len(candles) < 1:
        return

    set_query = ""
    total = {'pnl_total': 0, 'rpl_total': 0, 'rpl_total_percent': 0}
    balance = 100 #!!!!!
    launch['pnl_total'] = 0
    for stream in launch['streams']:
        if position[stream['id']].start:
            launch['pnl_total'] += position[stream['id']].pnl
            total['pnl_total'] += position[stream['id']].pnl
            set_query += f"pnl_{stream['id']}={str(position[stream['id']].pnl)}, "
        else:
            set_query += f"pnl_{stream['id']}={str(0)}, "

    for stream in launch['streams']:
        if 'pnl' in position[stream['id']].__dir__() and 'rpl' in position[stream['id']].__dir__():
            launch['rpl_total'] += position[stream['id']].rpl
            launch['rpl_total_percent'] += 100 * position[stream['id']].rpl / balance
            position[stream['id']].rpl = 0

    total['rpl_total'] += launch['rpl_total']
    total['rpl_total_percent'] += launch['rpl_total_percent']

    price.set_pnl(set_query, total, candles)



@logger.catch
def main_loop(launch, robot_is_stoped):
    config = Config(launch)
    pos = Positions(launch)
    price = Price(launch)
    algo = Algo(launch)
    summary = Summary(launch)

    init_launch(launch, config, summary)

    if config.db_get_state(launch) != True:
        init_algo(launch, price, algo, pos)

    #position = [positions.Position() for _ in launch['streams']]
    position = {'1': positions.Position(), '2': positions.Position()}

    # цикл по прайс
    while True:
        if get_robot_status(launch, robot_is_stoped):
            continue

        candles = price.get_candles(launch)
        if not candles:
            print(f"В таблице price нет новых строк, последняя строка {launch['last_id']}")
            break

        # работа при условии что в таблице прайс есть хотя бы одна запись
        if len(candles) == 0:
            if launch['mode'] != 'robot':
                print(f"В таблице записей price нет")
                break
            else:
                time.sleep(1)
                log_condition(get_cur_time(), "wait tick")
                continue

        # проверка условий и исполнение действий для каждого потока
        for stream in launch['streams']:

            if len(candles) > 2:
                check_block(launch, stream, candles, position, pos)
                if 'direction' in stream['order']:
                    position[stream['id']].update_pnl(float(candles[0]['price']), stream['order']['direction'])

        # запись значений в таблицу прайс
        set_parametrs(launch, candles, price, position)

        print(f"{launch=}")



main_loop(launch, robot_is_stoped)





