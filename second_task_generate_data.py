import datetime
from random import randrange, choice, sample, randint
from datetime import timedelta
from clickhouse_driver import Client
import copy

refs_list = ['ads1', 'ads2', 'ads3', 'social1', 'social2', 'target1', 'target2', 'games1', 'games2']
client = Client(host='localhost')
uids = set([x for x in range(500)])
uids_copy = copy.deepcopy(uids)


def generate_registration_time():
    start_date = datetime.datetime(2020, 9, 1, 0, 0, 0)
    end_date = datetime.datetime(2020, 9, 30, 23, 59, 59)
    delta = end_date - start_date
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start_date + timedelta(seconds=random_second)


def generate_login_time(reg_time_ts):
    reg_time = datetime.datetime.fromtimestamp(reg_time_ts)
    end_date = datetime.datetime(2020, 9, 30, 23, 59, 59)
    delta = end_date - reg_time
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return reg_time + timedelta(seconds=random_second)


## Generate data
for i in range(len(uids)):
    ts = int(generate_registration_time().timestamp())
    uid = sample(uids, 1)[0]
    uids.remove(uid)
    ref = choice(refs_list)
    obj_input = {"ts": ts, "user_id": uid, "ref": ref}
    tuple_input_reg = (ts, uid, ref)
    insert_str = f'INSERT INTO default.fact_reg VALUES {tuple_input_reg}'
    client.execute(insert_str)
    tuple_input_log = (ts, uid)
    log_str = f'INSERT INTO default.fact_login VALUES {tuple_input_log}'
    client.execute(log_str)
    for j in range(randint(0, 75)):
        time_log = int(generate_login_time(ts).timestamp())
        tuple_login = (time_log, uid)
        log_str = f'INSERT INTO default.fact_login VALUES {tuple_login}'
        client.execute(log_str)
    for j in range(randint(0, 30)):
        time_pay = int(generate_login_time(ts).timestamp())
        payment = randint(1, 100)
        tuple_payment = (time_pay, uid, payment)
        log_str = f'INSERT INTO default.fact_payment VALUES {tuple_payment}'
        client.execute(log_str)

