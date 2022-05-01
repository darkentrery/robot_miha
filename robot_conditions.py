import ast

# проверка срабатывания одного из условий
def check_condition(launch, condition, candles, stream):
    # проверяем каждый тип с помощбю функций из модуля robot_conditions
    if condition['type'] == 'candle':
        if check_candle(condition, candles):
            return True

    elif condition['type'] == 'trailing':
        if check_trailing(stream, condition, candles):
            return True

    elif condition['type'] == 'reject':
        if check_reject(condition, candles):
            return True

    elif condition['type'] == 'reverse':
        if check_reverse(condition, candles):
            return True

    elif condition['type'] == 'compare':
        if check_compare(launch, condition, stream):
            return True


    return False



def check_candle(condition, candles):
    if len(candles) < 3:
        return False

    if condition['side'] == 'buy' and candles[1]['price'] >= candles[2]['price']:
        return True
    elif condition['side'] == 'sell' and candles[1]['price'] < candles[2]['price']:
        return True
    else:
        return False

def check_compare(launch, condition, stream):
    fields = condition['fields'].split(' ')
    for i, field in enumerate(fields):
        if field == '=':
            fields[i] = '=='
    #print(f"{fields=}")
    fields_price = launch['price'].get_for_compare(launch)

    for i, field in enumerate(fields):
        if field in fields_price:
            fields[i] = str(fields_price[field])
        elif 'stream' in condition and field in launch['position'][str(condition['stream'])].__dir__():
            fields[i] = str(getattr(launch['position'][str(condition['stream'])], field))
        elif not ('stream' in condition) and field in launch['position'][str(stream['id'])].__dir__():
            fields[i] = str(getattr(launch['position'][str(stream['id'])], field))

    compare = ' '.join(fields)

    print(f"{compare=}")
    try:
        if eval(compare):
            return True
    except:
        print(f"Условие {compare} неверно задано.")
        return False


def check_trailing(stream, condition, candles):
    if 'position_price' in stream['order']:
        close = stream['order']['position_price']
    else:
        return False

    for c in range(-stream['order']['candle_id'], -len(candles), -1):
        if condition['side'] == 'up':
            activation_price = close * (1 + condition['activation_percent']/100)
            print(f"{activation_price=} {float(candles[c]['price'])}")
            if float(candles[c]['price']) >= activation_price:
                max_price = float(candles[c]['price'])
                print(f"{max_price=}")
                back_price = close + (max_price - close) * (1 - condition['back_percent']/100)
                print(f"{back_price=}")
                for p in range(c - 1, -len(candles), -1):
                    print(f"{p} {float(candles[p]['price'])}")
                    if float(candles[p]['price']) <= back_price:
                        return True

        elif condition['side'] == 'down':
            activation_price = close * (1 - condition['activation_percent']/100)
            if float(candles[c]['price']) <= activation_price:
                max_price = float(candles[c]['price'])
                back_price = close + (max_price - close) * (1 - condition['back_percent']/100)
                for p in range(-(c + 1), -len(candles), -1):
                    if float(candles[p]['price']) >= back_price:
                        return True

    return False


def check_reject(condition, candles):
    pass

def check_reverse(condition, candles):
    print("check_reverse")
    amount = int(condition['amount'])
    if len(candles) < amount + 3:
        return False
    if 'side' in condition and condition['side'] == 'up':
        for a in range(1, 2):
            if candles[a]['price'] >= candles[a + 1]['price']:
                return False
        for a in range(2, amount + 2):
            if candles[a]['price'] <= candles[a + 1]['price']:
                return False

    if 'side' in condition and condition['side'] == 'down':
        for a in range(1, 2):
            if candles[a]['price'] <= candles[a + 1]['price']:
                return False
        for a in range(2, amount + 2):
            if candles[a]['price'] >= candles[a + 1]['price']:
                return False

    print("Good")
    return True







#------------old------------------


"""
def check_trailing(condition, block, candle, order, launch):
    direction = order['direction']

    back_percent = float(condition['back_percent'])

    result = False

    trailing = order['trailings'].setdefault(str(block['number']), {})

    trailing.setdefault('price', 0)
    trailing.setdefault('max_price', 0)
    trailing.setdefault('min_price', 0)

    if condition.get("type_trailing") == "one_candle":
        start = launch['cur_candle']['open']
    else:
        start = order['open_price_position']

    price_change = True
    if direction == 'long' and (candle['price'] > trailing['max_price'] or trailing['max_price'] == 0):
        trailing['price'] = candle['price'] - (candle['price'] - start) * back_percent / 100
        trailing['max_price'] = candle['price']
    elif direction == 'short' and (candle['price'] < trailing['min_price'] or trailing['min_price'] == 0):
        trailing['price'] = candle['price'] + (start - candle['price']) * back_percent / 100
        trailing['min_price'] = candle['price']
    else:
        price_change = False

    if price_change:
        print("trailing_price(change)=" + str(trailing['price']) + ", time = " + str(candle['time']) + ", price=" + str(
            candle['price']) + ", open_price=" + str(order['open_price_position']))

    if trailing['price'] != 0:
        if direction == 'long' and candle['price'] <= trailing['price']:
            result = trailing['price']
        elif direction == 'short' and candle['price'] >= trailing['price']:
            result = trailing['price']

    if result != False:
        print("trailing_price(finish)=" + str(result) + ", time = " + str(candle['time']) + ", price=" + str(
            candle['price']))

    return result





def check_reject(condition, block, candle, order, prev_candle, prev_prev_candle, launch):
    if prev_candle == {}:
        return False

    side = condition["side"]
    name = condition["name"] + "-" + condition["side"]
    candle_count = condition["candle"]

    if prev_candle.get(name) == None:
        return False

    reject = order['reject'].setdefault(name + '_' + str(block['number']), {})
    if reject == {}:
        result_side = prev_candle['close'] == prev_candle.get(name)
        if result_side == True:
            reject['side'] = side
            reject['candle_count'] = candle_count
            reject['cur_time_frame'] = cur_time_frame['start']
            log_condition(candle['time'], "reject(start)")
        return False

    if reject['cur_time_frame'] == cur_time_frame['start']:
        return False

    next_result_side = ((side == 'high' and prev_candle['open'] > prev_candle['close'])
                        or (side == 'low' and prev_candle['open'] < prev_candle['close']))

    if next_result_side == False:
        reject.clear()
        return False

    reject['candle_count'] = reject['candle_count'] - 1
    reject['cur_time_frame'] = cur_time_frame['start']

    result = reject['candle_count'] == 0
    if result:
        log_condition(candle['time'], "reject(finish)")

    return result


def check_percent(condition, block, candle, order, prev_candle, prev_prev_candle, launch):
    print("check_percent")
    if prev_candle == {}:
        return False

    condition.setdefault('offset_1', -1)
    condition.setdefault('offset_2', -1)

    if condition['offset_2'] == -2 and prev_prev_candle == None:
        return False

    if condition.get('value') == None:
        return False

    if condition['offset_1'] == -1:
        source_candle_1 = prev_candle
    elif condition['offset_1'] == -2:
        source_candle_1 = prev_prev_candle
    elif condition['offset_1'] == -3:
        if prev_prev_candle == {}:
            return False
        else:

            res = connector.get_candles(launch)[0]
            if res == False:
                return False
            else:
                source_candle_1 = res
    else:
        return False

    if condition['offset_2'] == -1:
        source_candle_2 = prev_candle
    elif condition['offset_2'] == -2:
        source_candle_2 = prev_prev_candle
    elif condition['offset_2'] == -3:
        if prev_prev_candle == {}:
            return False
        else:

            res = connector.get_candles(launch)[0]
            if res == False:
                return False
            else:
                source_candle_2 = res
    else:
        return False

    param_1 = source_candle_1.get(condition['param_1'])
    if param_1 == None:
        return False
    param_1 = float(param_1)


    param_2 = source_candle_2.get(condition['param_2'])
    if param_2 == None:
        return False
    param_2 = float(param_2)

    operator = condition['value'].split(' ')[0]
    percent = float(condition['value'].split(' ')[1])

    percent_fact = ((param_1 - param_2) / param_1) * 100

    result = False

    if operator == '>=':
        if percent_fact >= percent:
            result = True
    elif operator == '<=':
        if percent_fact <= percent:
            result = True
    elif operator == '<':
        if percent_fact < percent:
            result = True
    elif operator == '>':
        if percent_fact > percent:
            result = True
    elif operator == '=':
        if percent_fact == percent:
            result = True
    elif operator == '':
        result = True
    else:
        result = False

    if result == True:
        log_condition(candle['time'], "check_percent: " + str(condition))

    return result


def check_reverse(condition, block, candle, order, launch):
    print("check_reverse")
    candles = connector.get_candles(launch)
    print(f"{candles=}")
    print(f"{condition=}")
    amount = int(condition['amount'])
    #compare = reverse['compare'].split(' ')
    if len(candles) < amount + 1:
        return False
    for a in range(1):
        #print(f"{candles[a]['price']=} {candles[a + 1]['price']=}")
        if candles[a]['price'] > candles[a + 1]['price']:
            return False
    for a in range(1, amount):
        #print(f"{candles[a]['price']=} {candles[a + 1]['price']=}")
        if candles[a]['price'] < candles[a + 1]['price']:
            return False


    print("Good")
    return True"""

