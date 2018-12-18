# aioboto3 では s3 から取得できなかったのでヤメ
# import aioboto3
import aiobotocore
import asyncio
from datetime import datetime
from pprint import pprint
from tzlocal import get_localzone

date_format = "%Y/%m/%d %H:%M"
local_zone = get_localzone()


def get_next_range(curr_to, step, last):
    next_from = curr_to + 1
    next_to = curr_to + step
    if last < next_to:
        next_to = last

    return next_from, next_to


def get_next_range_list(curr_to, step, last, parallel):
    list_ = []
    prev_to = curr_to
    for _ in range(parallel):
        from_, to = get_next_range(prev_to, step, last)
        list_.append([from_, to])
        if last <= to:
            break
        prev_to = to

    return list_


async def check_s3_objects(loop, keys):
    bucket_name = "bitflyer-executions"

    session = aiobotocore.get_session(loop=loop)
    async with session.create_client("s3", region_name="ap-northeast-1") as s3_client:
        for key in keys:
            response = await s3_client.get_object(Bucket=bucket_name, Key=key)
            if response["ContentType"] != "application/json":
                print(f'key: {key}, json でない')

    print(f'{keys[0]} 〜 {keys[-1]} チェック完了')


def main():
    dt = datetime.now(local_zone).strftime(date_format)
    print(dt)

    start = 1
    # max_ = 640000000
    max_ = 100000
    step = 10000
    curr_to = start - 1

    loop = asyncio.get_event_loop()
    while True:
        from_to_list = get_next_range_list(
            curr_to=curr_to, step=500, last=curr_to + step, parallel=20)

        keys = [f'{from_to[0]:0>10}-{from_to[1]:0>10}' for from_to in from_to_list]
        # pprint(keys)
        loop.run_until_complete(check_s3_objects(loop, keys))

        curr_to = curr_to + step
        if max_ <= curr_to:
            break


if __name__ == '__main__':
    # main()

    list_ = get_next_range_list(0, 500, 1002, 10)
    pprint(list_)
