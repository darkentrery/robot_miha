
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


# --------------old_positions-----------------


def get_equity_many_robot(stream):
    try:

        http_data = '{"equity": "BTC"}'
        # http_data = '{"equity": "USDT"}'
        r = requests.post(stream['url'], data=http_data, timeout=7)
        if r.status_code != 200:
            raise Exception("Status code = " + str(r.status_code))
        equity = r.text.rstrip()
        print("equity = " + equity + ", time = " + str(datetime.datetime.utcnow()))
        return float(ast.literal_eval(equity))
    except Exception as e:
        time.sleep(2)
        print(e)
        print("equity exception" + ", time = " + str(datetime.datetime.utcnow()))
        return None


def send_leverage_many_robot(launch, stream):
    print("send_leverage_many_robot")
    print(f"{stream=}")

    if launch['mode'] != 'robot':
        return

    try:

        order = stream['order']
        price = equity[int(stream['id']) - 1][0].price_order
        quantity = equity[int(stream['id']) - 1][0].size_order
        # http_data='{"symbol":"' + stream['symbol'] + '","side":"' +  order['direction'] + '","leverage":"' + str(order['leverage']) + '","order_type":"' +  order['order_type'] + '"}'
        http_data = '{"symbol":"' + stream['balancing_symbol'] + '","side":"' + order[
            'direction'] + '","leverage":"' + str(order['leverage']) + '","price":"' + str(
            price) + '","quantity":"' + str(quantity) + '","order_type":"' + order['order_type'] + '"}'
        # http_data = f"symbol:{stream['balancing_symbol']}, side:{order['direction']}, leverage:{order['leverage']}, price:0.9286, order_type:{order['order_type']}"
        # print(f"{http_data=}")
        # http_data = json.dumps(http_data, default=json_serial)
        print(f"{http_data=}")

        r = requests.post(stream['url'], data=http_data, timeout=7)
        if r.status_code != 200:
            raise Exception("Leverage Status code = " + str(r.status_code))
    except Exception as e:
        time.sleep(2)
        print(e)
        print("leverage send exception" + ", time = " + str(datetime.datetime.utcnow()))


def send_balancing_robot(launch, stream):
    print("send_balancing_robot")

    if launch['mode'] != 'robot':
        return

    try:
        http_data = '{"equity_balancing":"' + stream['balancing_symbol'] + '"}'
        r = requests.post(stream['url'], data=http_data, timeout=7)
        if r.status_code != 200:
            raise Exception("Leverage Status code = " + str(r.status_code))

    except Exception as e:
        time.sleep(2)
        print(e)
        print("leverage send exception" + ", time = " + str(datetime.datetime.utcnow()))


def get_total_equity(launch, prev_candle, cursor):
    if launch['traiding_mode'] != 'many':
        return None

    if launch['mode'] == 'tester' and prev_candle != {}:
        query = "select id, total_equity from {0} where id={1}".format(launch['price_table_name'], prev_candle['id'])
        cursor.execute(query)
        for (id, total_equity) in cursor:
            return total_equity

    return None

