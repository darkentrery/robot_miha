import ast
import datetime
import time

import robot_positions as positions
import robot_conditions as conditions
from datetime import timedelta

from sys import argv
from robot_db import Config, Positions, Algo, Price
from loguru import logger
from robot_constants import launch, data, SYMBOL, robot_is_stoped


logger.add("debug.log", format="{time} {level} {message}", level="DEBUG")


print('=============================================================================')


config = Config(launch)
pos = Positions(launch)
price = Price(launch)
algo = Algo(launch)



# создание ордера с нулевыми параметрами
def get_new_order(order, launch):
    if launch['mode'] == 'robot':
        pass

    elif launch['mode'] == 'tester':
        if order == None:
            order = {}

        order['balance'] = 1

        order['open_time_order'] = 0
        order['open_time_position'] = 0
        order['close_time_position'] = 0
        order['close_time_order'] = 0

        order['trailings'] = {}
        order['abs'] = {}
        order['reject'] = {}
        order['candle'] = {}

        order['leverage'] = 1
        order['direction'] = 0
        order['order_type'] = None
        order['state'] = 'start'
        order['block_id'] = ''
        order['pnl'] = 0
        order['rpl'] = 0

        if launch.get('many_metadata') != None and launch.get('many_metadata') != {}:
            order['leverage'] = 1
            order['equity'] = 1

    return order

# наполнение массива launch
def init_launch(launch):
    algorithm_prefix = 'algo_'
    config.get_0_config(launch, data)

    launch['exchange_metadata'] = {"exchange": "binance"}
    launch['streams'] = []
    launch['rpl_total_percent'] = 0

    if launch['traiding_mode'] == 'many':
        for ord_many in launch['many_metadata']['streams']:
            stream_element = {}
            stream_element['algorithm'] = algorithm_prefix + str(ord_many['algorithm'])
            stream_element['order'] = get_new_order(None, launch)
            stream_element['id'] = ord_many['id']
            host = 'http://localhost:1010'
            host = 'http://46.101.164.69:1010'
            stream_element['url'] = ord_many.setdefault('url', host)
            stream_element['balancing_symbol'] = ord_many.setdefault('balancing_symbol', SYMBOL)

            launch['streams'].append(stream_element)


# преобразование полученных данных из таблиц алгоритмов в форму словаря
def convert_algorithm_data(algorithm_data):
    blocks = []
    for i, a in enumerate(algorithm_data):
        h = ast.literal_eval(a[2])
        activations = a[3].split(',')
        blocks.append({'id': str(a[0]), 'name': a[1], 'conditions': h['conditions'], 'actions': h['actions'], 'activations': activations})

    return blocks

# получение списка активных блоков
def get_activation_blocks(action_block, blocks):
    if not action_block:
        activation_blocks = blocks
    else:
        activation_blocks = []
        for block in blocks:
            if block['id'] == action_block:
                for act in block['activations']:
                    for b in blocks:
                        if b['id'] == act:
                            activation_blocks.append(b)

    return activation_blocks

# активация стримов, подготовка таблиц
def init_algo(launch):
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

# проверка срабатывания одного из условий
def check_condition(condition, candles):
    # проверяем каждый тип с помощбю функций из модуля robot_conditions
    if condition['type'] == 'candle':
        if conditions.check_candle(condition, candles):
            return True


    elif condition['type'] == 'trailing':
        pass
    elif condition['type'] == 'reject':
        pass
    elif condition['type'] == 'reverse':
        pass
    return False

# выполнение действия
def execute_action(stream, block, candles, equity):
    print("execute_action")
    positions.update_position(stream, block, candles, equity, pos)
    stream['activation_blocks'] = get_activation_blocks(block['id'], stream['blocks'])
    print(f"{stream['activation_blocks']=}")


# проверка условий в блоке
def check_block(stream, candles, equity):
    print("check_block")
    numbers = 3
    activation_blocks = stream['activation_blocks']
    bool_numbers = [False for n in range(numbers)]

    # вначале проверяем условия по намберам
    for block in activation_blocks:
        for num in range(numbers):
            for condition in block['conditions']:
                if 'number' in condition:
                    if check_condition(condition, candles):
                        bool_numbers[num] = True
                    else:
                        bool_numbers[num] = False

            if bool_numbers[num]:
                execute_action(stream, block, candles, equity)
                return

    # если намберы не сработали проверяем остльные условия
    if not True in bool_numbers:
        for block in activation_blocks:
            for condition in block['conditions']:
                if check_condition(condition, candles):
                    execute_action(stream, block, candles, equity)
                    return


# проверка остановки робота
def get_robot_status(launch, robot_is_stoped):
    if launch['mode'] == 'robot':
        try:
            trading_status = config.get_trading_status()
        except Exception as e:
            print(e)
            return True

        if robot_is_stoped and trading_status == 'on':
            robot_is_stoped = False
            print('Робот запущен')

        order = get_new_order(None, launch)  # !!!!! delete
        robot_must_stop = (trading_status == 'off'
                           or (launch['trading_status'] == "off_after_close" and order['open_time_position'] == 0)
                           or trading_status == 'off_now_close')

        if robot_must_stop and robot_is_stoped == False:
            robot_is_stoped = True
            print('Робот остановлен')
            if trading_status != 'off':
                config.db_clear_state()
                launch = init_launch()
                init_algo(launch)

        launch['trading_status'] = trading_status

        if robot_is_stoped:
            return True

# запись параметров в таблицу прайс
def set_parametrs(launch, candles, price):
    print("set_parametrs")
    if len(candles) < 1:
        return

    set_query = ""
    total = {'pnl_total': 0, 'rpl_total': 0, 'rpl_total_percent': 0}
    balance = 0 #!!!!!
    for stream in launch['streams']:
        total['pnl_total'] += stream['order']['pnl']
        total['rpl_total'] += stream['order']['rpl']
        last_order = stream['order']
        balance += stream['order']['balance']
        set_query = set_query + f"pnl_{stream['id']}={str(last_order['pnl'])},"
    total['rpl_total_percent'] = 100 * total['rpl_total'] / balance + launch['rpl_total_percent']
    launch['rpl_total_percent'] = total['rpl_total_percent']
    price.set_pnl(set_query, total, candles)


@logger.catch
def main_loop(launch, robot_is_stoped):

    init_launch(launch)

    if config.db_get_state(launch) != True:
        init_algo(launch)

    position = [positions.Position() for _ in launch['streams']]

    # цикл по прайс
    while True:
        if get_robot_status(launch, robot_is_stoped):
            continue

        candles = price.get_candles(launch)

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

            if len(candles) > 1:
                check_block(stream, candles, position)

        # запись параметров в таблицу прайс
        set_parametrs(launch, candles, price)

        print(f"{launch=}")



main_loop(launch, robot_is_stoped)





# ---------- old ---------------
cur_time_frame = {}

def log_condition(time, info):
    if time == None:
        time = datetime.datetime.utcnow()
    print(str(time) + " --- " + info)


def get_cur_time():
    return datetime.datetime.utcnow()


def get_cur_timeframe(cur_time_frame, cur_time, time_frame):
    if cur_time_frame == {}:
        cur_time_frame['start'] = cur_time.replace(hour=0, minute=0, second=0, microsecond=0)
        cur_time_frame['finish'] = cur_time_frame['start'] + timedelta(minutes=time_frame)

    while True:
        if cur_time_frame['start'] <= cur_time and cur_time < cur_time_frame['finish']:
            break
        else:
            cur_time_frame['start'] = cur_time_frame['start'] + timedelta(minutes=time_frame)
            cur_time_frame['finish'] = cur_time_frame['start'] + timedelta(minutes=time_frame)

    return cur_time_frame

