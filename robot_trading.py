
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

