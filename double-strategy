#!/usr/bin/env python

import asyncio
import websockets
import json
import requests
import dateutil.parser as dp
import hmac
import base64
import zlib
import ast
import math
import okex.futures_api as future
import time
from  cacheout import Cache
mycache = Cache()
mycache.set('long',[0,0,0,"orderid",0,0,0])
mycache.set('short',[0,0,0,"orderid",0,0,0])
mycache.set('high',0)
mycache.set('low',0)
#orderopenornot ,price,statusnot ,orderid,stuck status,order num,order_grid_depth
future_name = "XRP-USD-190628"
grid_step=[0,0.0003,0.0016,0.0048,0.015,0.06]
#               4       8    16       32   64
#               3       6   12       24
#               2       4     8      16   32    64
#               1       3     9      27     54
#               1       3      6      24    48
#1 . make grid_step movable factor by short and long open diff
#2.
last_grid=0.0008
mycache.set("close_list",[[],0,0,0])
import logging
#update price_avg to merge order
#reset three long one short to zero

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
handler = logging.FileHandler("log.txt")
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

console = logging.StreamHandler()
console.setLevel(logging.INFO)

logger.addHandler(handler)
logger.addHandler(console)

def get_server_time():
    url = "http://www.okex.com/api/general/v3/time"
    response = requests.get(url)

    if response.status_code == 200:
        return response.json()['iso']
    else:
        return ""

def server_timestamp():
    server_time = get_server_time()
    parsed_t = dp.parse(server_time)
    timestamp = parsed_t.timestamp()
    return timestamp

def login_params(timestamp, api_key, passphrase, secret_key):
    message = timestamp + 'GET' + '/users/self/verify'
    mac = hmac.new(bytes(secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
    d = mac.digest()
    sign = base64.b64encode(d)

    login_param = {"op": "login", "args": [api_key, passphrase, timestamp, sign.decode("utf-8")]}
    login_str = json.dumps(login_param)
    return login_str

def inflate(data):
    decompress = zlib.decompressobj(
            -zlib.MAX_WBITS  # see above
    )
    inflated = decompress.decompress(data)
    inflated += decompress.flush()
    return inflated

async def subscribe(url, api_key, passphrase, secret_key, channels):
    async with websockets.connect(url) as websocket:
        # login
        timestamp = str(server_timestamp())
        login_str = login_params(str(timestamp), api_key, passphrase, secret_key)
        await websocket.send(login_str)

        login_res = await websocket.recv()
        login_res = inflate(login_res)
        print(f"{login_res}")
        sub_param = {"op": "subscribe", "args": channels}
        sub_str = json.dumps(sub_param)
        await  websocket.send(sub_str)
        print(f"send: {sub_str}")

        while True:
            res = await websocket.recv()
            res = inflate(res)
            s2 = bytes.decode(res)
            res=ast.literal_eval(s2)
            logger.info(str(res))
            logger.info(str(type(res)))
            if res.get('data',None)==None:
                continue
            if res['table']=='futures/candle7200s':
                if check_order(res['data'])==True:
                    continue
            elif res['table']=='futures/order':
                 sync_future_order_status(res['data'][0])


def sync_future_order_status(*args,**kwargs):
    aimdata = args[0]
    order_type = aimdata['type']
    order_status = aimdata['status']
    order_size = int(aimdata['size'])
    order_id = aimdata['order_id']
    deal_price = float(aimdata['price_avg'])
    long_data = mycache.get('long')
    short_data = mycache.get('short')
    long_open_num=int(long_data[-2])
    short_open_num=int(short_data[-2])
    long_open_price = float(long_data[1])
    short_open_price = float(short_data[1])
    short_open_depth=int(long_data[-1])
    long_open_depth=int(short_data[-1])
    if order_type=="1" :

        if order_status in ["0","1"]:
            if long_open_num==0:
                mycache.set('long',[0,long_open_price,1,order_id,0,0,0])
            elif long_open_num>=1:
                pass
        elif order_status=='2':
            orders=[]
            if long_open_num==0:
                long_open_num+=order_size
                mycache.set('long',[1,deal_price,0,order_id,0,long_open_num,long_open_depth])

            elif long_open_num>=2:
                if deal_price>0.1:
                    deal_price=(deal_price+long_open_price)/2
                long_open_num+=order_size
                mycache.set('long',[1,deal_price,0,order_id,1,long_open_num,long_open_depth])
            long_close_price = str(deal_price+0.0003)
            order2 = { "type": "3", "price": long_close_price, "size": str(long_open_num),
                      "match_price": "0"}
            orders.append(order2)
            logger.info("close long sum!")
            logger.info(str(long_open_num))
            orders_data = json.dumps(orders)
            result = futureAPI.take_orders(future_name, orders_data=orders_data, leverage=10)
            logger.info(json.dumps(result))

    elif order_type=="2":
        if order_status in ["0","1"]:
            #this place should not change ,if short_open_num>=1
            if short_open_num==0:
                mycache.set('short',[0,short_open_price,1,order_id,0,0,0])
            elif short_open_num>=1:
                pass
        elif order_status=='2':
            if short_open_num==0:
                short_open_num+=order_size
                mycache.set('short',[1,deal_price,0,order_id,0,short_open_num,short_open_depth])
            elif short_open_num>=2:
                if deal_price>0.1:
                    deal_price=(deal_price+short_open_price)/2
                short_open_num+=order_size
                mycache.set('short',[1,deal_price,0,order_id,1,short_open_num,short_open_depth])
            logger.info("close short sum!")
            logger.info(str(short_open_num))
            #mycache.set('short',[1,short_open_price,0,order_id,0,short_open_num])
            orders = []
            short_close_price = str(deal_price-0.0003)
            order2 = { "type": "4", "price": short_close_price, "size": str(short_open_num),
                       "match_price": "0"}
            orders.append(order2)
            orders_data = json.dumps(orders)
            result = futureAPI.take_orders(future_name, orders_data=orders_data, leverage=10)
            logger.info(json.dumps(result))
    elif order_type=="3":
        if order_status in ["0","1"]:
            if long_open_num==2:
                mycache.set('long',[1,long_open_price,1,order_id,0,long_open_num,long_open_depth])
            elif long_open_num>2:
                mycache.set('long',[1,long_open_price,1,order_id,1,long_open_num,long_open_depth])

        elif order_status=='2':
            mycache.set('long',[0,0,0,0,0,0,0])
            #aimcloselist=mycache.get('close_list')[0]
            #aimcloselist.append(1)
            #mycache.set('close_list',[aimcloselist,0,0,int(time.time())])
        elif order_status=="-1":
            #long_open_price=long_open_price-grid_step[int(math.log2(long_open_num))]+0.0001

            long_open_price=long_open_price #market price
            result = futureAPI.take_order(client_oid=None,instrument_id=future_name,otype="1",price=long_open_price,size=str(long_open_num),match_price="0",
                                          leverage=10)
            logger.info("start to open new long !")
            logger.info(str(result))
    elif order_type=="4":
        if order_status in ["0","1"]:
            if short_open_num==2:
                mycache.set('short',[1,short_open_price,1,order_id,0,short_open_num,short_open_depth])
            elif short_open_num>2:
                mycache.set('short',[1,short_open_price,1,order_id,1,short_open_num,short_open_depth])
        elif order_status=='2':
            mycache.set('short',[0,0,0,0,0,0,0])
            #aimcloselist=mycache.get('close_list')[0]
            #aimcloselist.append(0)
            #mycache.set('close_list',[aimcloselist,0,0,int(time.time())])
        elif order_status=="-1":
            logger.info("start to open new short!")
            #short_open_price=short_open_price+grid_step[int(math.log2(short_open_num))]-0.0001
            short_open_price=short_open_price
            result = futureAPI.take_order(client_oid=None,instrument_id=future_name,otype="2",price=short_open_price,size=str(short_open_num),match_price="0",
                                          leverage=10)
            logger.info(str(result))



def check_order(*args,**kwargs):
    #check order only depend on mycache status ,origin order status --> mycache status ---> operation
    #eq . "long" [0                     ,0.2234                        ,0]
    #             opened or closed       price                         stuck or not
    long_data=mycache.get('long')
    short_data=mycache.get('short')
    logger.info(str(long_data))
    logger.info(str(short_data))
    long_status=long_data[0]
    short_status=short_data[0]
    long_open_price=long_data[1]
    short_open_price=short_data[1]
    long_stuck_status=long_data[2]
    longdouble_stuck_status=long_data[4]
    short_stuck_status=short_data[2]
    shortdouble_stuck_status=short_data[4]
    long_order_id=long_data[3]
    short_order_id=short_data[3]
    market_current_price=float(args[0][0]['candle'][4])
    market_high_price = float(args[0][0]['candle'][2])
    market_low_price = float(args[0][0]['candle'][3])
    long_open_num=long_data[-2]
    short_open_num=short_data[-2]
    long_stuck_depth=long_data[-1]
    short_stuck_depth=short_data[-1]
    #check_next_open_direction_and_sync_state()
    #close_list=mycache.get('close_list')
    #logger.info(str(close_list))
    #close_list_long_stun=close_list[1]
    #close_list_short_stun=close_list[2]
    #logger.info(str(result))
    #open must be between in last two hours max-0.0002 and min+0.0002 ,otherwise delay 5min
    if  long_status==0 and long_stuck_status==0 :
            long_open_price=market_current_price+0.001
            long_open_price = str(long_open_price)
            result = futureAPI.take_order(client_oid=None,instrument_id=future_name,otype="1",price=long_open_price,size="1",match_price="0",
                                          leverage=10)
            logger.info(str(result))
            logger.info("long order opened!")
            mycache.set('long',[0,long_open_price,1,0,0,0,0])
            return True

    if long_status==0 and long_stuck_status==1:
            return True

    if  short_status==0 and short_stuck_status==0 :
        short_open_price=market_current_price-0.001
        short_open_price = str(short_open_price)
        result = futureAPI.take_order(client_oid=None,instrument_id=future_name,otype="2",price=short_open_price,size="1",match_price="0",
                                      leverage=10)
        logger.info(str(result))
        logger.info("short order opened!")
        mycache.set('short',[0,short_open_price,1,0,0,0,0])
        return True

    if short_status==0 and short_stuck_status==1:
        return True

    if long_open_num!=0 and market_current_price<=long_open_price-grid_step[int(math.log2(long_open_num))] and long_status==1 and market_current_price>market_low_price+0.0006:# and longdouble_stuck_status!=1:
        locate_index= int(math.log2(int(long_open_num))+1)
        #high_grid=grid_step[locate_index+]
        low_grid=grid_step[locate_index]
        diff_price = long_open_price-market_current_price

        if diff_price >low_grid  :
            if  long_stuck_depth < locate_index:
                result=futureAPI.revoke_order(future_name,long_order_id)
                logger.info("start to cancel long!")
                logger.info(str(result))
                long_stuck_depth+=1
                mycache.set('long',[1,long_open_price,1,long_order_id,1,long_open_num,long_stuck_depth])
            else :
                pass


    if short_open_num>0 and market_current_price>=short_open_price+grid_step[int(math.log2(short_open_num))] and short_status==1 and market_current_price<market_high_price-0.0006:# and shortdouble_stuck_status!=1:
        locate_index= int(math.log2(int(short_open_num))+1)
        #high_grid=grid_step[locate_index]
        low_grid=grid_step[locate_index]
        diff_price = market_current_price-short_open_price

        if diff_price >low_grid  :
            if short_stuck_depth < locate_index:
                result=futureAPI.revoke_order(future_name,short_order_id)
                logger.info("start to cancel short!")
                logger.info(str(result))
                short_stuck_depth+=1
                mycache.set('short',[1,short_open_price,1,short_order_id,1,short_open_num,short_stuck_depth])
            else :
                pass


    return True





api_key = ''
seceret_key = ''
passphrase = ''
url = 'wss://real.okex.com:10442/ws/v3'
# asyncio.get_event_loop().run_until_complete(login(url, api_key, passphrase, seceret_key))
channels=["futures/order:"+future_name,"futures/candle7200s:"+future_name]

#channels = ["swap/position:XRP-USD-SWAP"]
#channels = ["futures/order:XRP-USD-190329"]
#channels = ["spot/account:XRP"]
#channels = ["spot/ticker:ETH-USDT"]
futureAPI = future.FutureAPI(api_key, seceret_key, passphrase, True)
asyncio.get_event_loop().run_until_complete(subscribe(url, api_key, passphrase, seceret_key, channels))
#asyncio.get_event_loop().run_until_complete(local_logic())
# asyncio.get_event_loop().run_until_complete(unsubscribe(url, api_key, passphrase, seceret_key, channels))
#asyncio.get_event_loop().run_until_complete(subscribe_without_login(url, channels))
#asyncio.get_event_loop().run_until_complete(unsubscribe_without_login(url, channels))
