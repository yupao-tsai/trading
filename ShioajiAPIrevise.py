import sys
import datetime
from typing import List, Tuple, Dict, Callable
from collections import defaultdict, Counter

import typing
import secrets

import numpy
import numpy as np
from scipy import stats
from shioaji.backend.solace.api import StrictInt
from shioaji.contracts import Contracts
from Base import APIBase
import shioaji as sj
from shioaji import BidAskSTKv1, BidAskFOPv1, Exchange
import pandas as pd
from typing import Union
import threading
import queue
from decimal import *
import math
import time
import exchange_calendars as ecals

g_debug: bool = False
g_simulation: bool = False
g_record_only: bool = False
g_record: bool = True
g_enable_trading: bool = True

#todo list
#1. 快點結算日時， 要不要轉倉
#2. 股東會或除權的時間不能做券賣

# contract = api.Contracts.Futures.TXF.TXF202004
# order = api.Order(action=sj.constant.Action.Buy,
#                   price=10200,
#                   quantity=2,
#                   price_type=sj.constant.StockPriceType.LMT,
#                   order_type=sj.constant.FuturesOrderType.ROD,
#                   octype=sj.constant.FuturesOCType.Auto,
#                   account=api.futopt_account)

# Attributes:
# price (float or int): the price of order
# quantity (int): the quantity of order
# action (str): order action to buy or sell
#     {Buy, Sell}
# price_type (str): pricing type of order
#     {LMT, MKT, MKP}
# order_type (str): the type of order
#     {ROD, IOC, FOK}
# octype (str): the type or order to open new position or close position future only
#     {Auto, NewPosition, Cover, DayTrade} (自動、新倉、平倉、當沖)
# account (:obj:Account): which account to place this order
# ca (binary): the ca of this order

# trade = api.place_order(contract, order)
# PendingSubmit: 傳送中
# PreSubmitted: 預約單
# Submitted: 傳送成功
# Failed: 失敗
# Cancelled: 已刪除
# Filled: 完全成交
# Filling: 部分成交

import abc


#
# class OrderMonitoring():
#     def __init__(self):
#         self.notify_dict:Dict[sj.order.Trade,threading.Event]=defaultdict(threading.Event)
#
#     def non_blocking_cb(self, trade: sj.order.Trade):
#         print('non_block_callback....')
#         print(f'{trade.contract.name} : {trade.status}')
#         # event = self.notify_dict[trade.contract.code]
#         # event.set()
#
#     # @staticmethod
#     # def non_blocking_cb(trade: sj.order.Trade):
#     #     # event = self.notify_dict[trade.contract.code]
#     #     # event.set()
#     #     print('non_block_callback....')
#     #     print(trade.status.status)
#
#     @staticmethod
#     def non_blocking_cb2(trade: sj.order.Trade):
#         # event = self.notify_dict[trade.contract.code]
#         # event.set()
#         print('non block callback 222222')
#         print(f'{trade.contract.name} : {trade.status}')
#
#     @staticmethod
#     def order_callback(stat,msg):
#         print('my_place_callback__')
#         # print(stat,msg)
#         print(f'type of stat ={type(stat)}, {stat}')
#         print(f'type of msg ={type(msg)}, {msg}')
#
#     def add_trade(self,trade:sj.order.Trade, notify_event:threading.Event):
#         key = trade.contract.code
#         self.notify_dict[key]=notify_event
#         return key
#
#     def remove_trade(self, key:str):
#         del self.notify_dict[key]
class ShareInHouse(threading.Thread):
    def __init__(self, in_api: sj.Shioaji, event_to_terminate: threading.Event):
        super(ShareInHouse, self).__init__()
        self.api = in_api
        self.event_to_terminate = event_to_terminate
        self.share_info = {"stock": None, "future": None, "option": None, "balance": None}
        self.lock_data = threading.Lock()
        self.event_to_update_data = threading.Event()
        self.get_shares_in_house()
        self.calc_risk_indicators()

    def run(self) -> None:
        while not self.event_to_terminate.isSet():
            self.event_to_update_data.wait(5)
            if self.event_to_update_data.isSet():
                self.get_shares_in_house()
                self.calc_risk_indicators()
                self.event_to_update_data.clear()

        print(f"{self.__class__} is shut down")
        del self

    def update(self):
        self.event_to_update_data.set()

    def get_stock_info_in_house(self, code):
        with self.lock_data:
            si = self.share_info["stock"]
            return si.get(code, None)

    def get_future_info_in_house(self, code):
        with self.lock_data:
            si = self.share_info["future"]
            return si.get(code, None)

    def get_option_info_in_house(self, code):
        with self.lock_data:
            si = self.share_info["option"]
            return si.get(code, None)

    def is_available_capital(self) -> bool:
        return self.stock_risk_indicator > 500 and self.future_risk_indicator > 800

    def get_shares_in_house(self):
        future_positions = self.api.get_account_openposition(product_type = "1", query_type = "1",
                                                             account = self.api.futopt_account)
        future_dict = {}
        for position in future_positions.data():
            if len(position) > 0:
                if position["Code"] not in future_dict.keys():
                    future_dict[position["Code"]] = \
                        [{'code': position["Code"], 'action': "Buy" if position["OrderBS"] == "B" else "Sell",
                         'qty': int(Decimal(position["Volume"])), 'price': Decimal(position["ContractAverPrice"])}]
                else:
                    future_dict[position["Code"]].append(
                        {'code': position["Code"], 'action': "Buy" if position["OrderBS"] == "B" else "Sell",
                         'qty': int(Decimal(position["Volume"])), 'price': Decimal(position["ContractAverPrice"])})
        with self.lock_data:
            self.share_info["future"] = future_dict
        option_positions = self.api.get_account_openposition(product_type = "0", query_type = "1",
                                                             account = self.api.futopt_account)
        option_dict = {}
        for position in [s for s in option_positions.data() if 'TX' in s['Code']]:
            if len(position) > 0:
                if position["Code"] not in option_dict.keys():
                    option_dict[position["Code"]] = \
                        [{'code': position["Code"], 'action': "Buy" if position["OrderBS"] == "B" else "Sell",
                         'qty': int(Decimal(position["Volume"])), 'price': Decimal(position["ContractAverPrice"])}]
                else:
                    option_dict[position["Code"]].append(
                        {'code': position["Code"], 'action': "Buy" if position["OrderBS"] == "B" else "Sell",
                          'qty': int(Decimal(position["Volume"])), 'price': Decimal(position["ContractAverPrice"])})
        with self.lock_data:
            self.share_info["option"] = option_dict

        stock_positions = self.api.list_positions(self.api.stock_account)
        stock_dict = {}
        for position in stock_positions:
            if len(position) > 0:
                if position["code"] not in stock_dict.keys():
                    stock_dict[position["code"]] = \
                        [{'code': position["code"], 'action': position["direction"],
                         'qty': int(Decimal(position["quantity"])), 'price': Decimal(position["price"])}]
                else:
                    stock_dict[position["code"]].append(
                        {'code': position["code"], 'action': position["direction"],
                          'qty': int(Decimal(position["quantity"])), 'price': Decimal(position["price"])})
        with self.lock_data:
            self.share_info["stock"] = stock_dict

    def calc_risk_indicators(self):
        balance = Decimal(self.api.account_balance()[0]['acc_balance'])
        setlement_stock = self.api.list_settlements(self.api.stock_account)
        balance += Decimal(setlement_stock[0]['t1_money'])
        balance += Decimal(setlement_stock[0]['t2_money'])
        self.stock_balance = balance
        self.stock_positions = self.api.list_positions(self.api.stock_account)
        total_value = 0
        stocks = {}
        if len(self.stock_positions) > 0:
            for pos in self.stock_positions:
                qty = pos['quantity']
                price = pos['price']
                sign = 1 if pos['direction'] == sj.constant.Action.Buy else -1
                sum = stocks.get(pos['code'], 0)
                stocks[pos['code']] = sum + qty * price * sign

        for v in stocks.values():
            total_value += abs(v) * 1000
        if balance < 10000:
            self.stock_risk_indicator = Decimal(0)
        else:
            self.stock_risk_indicator = Decimal(balance) * 1000 / (Decimal(balance) + Decimal(total_value))

        print(f'self.stock_risk_indicator = {self.stock_risk_indicator}')
        account_margin_future = self.api.get_account_margin(account = self.api.futopt_account).data()
        self.future_risk_indicator = Decimal(account_margin_future[0]['Flow'])

        # self.future_risk_indicator = Decimal(account_margin_future[0]['Flow'])
        # self.future_cash_amount = Decimal(account_margin_future[0]['Cashamt'])
        # self.future_equity = Decimal(account_margin_future[0]['Equity'])
        # self.stock_profit_loss = self.api.list_profit_loss_detail(self.api.stock_account)
        # self.future_profit_loss = self.api.list_profit_loss_detail(self.api.futopt_account)
        # self.stock_positions = self.api.list_positions(self.api.stock_account)
        # self.future_positions = self.api.list_positions(self.api.futopt_account)
        # print(self.stock_profit_loss)

class ThreadSafeList(List):
    def __init__(self,seq=()):
        super(ThreadSafeList, self).__init__(seq)
        self.locker = threading.Lock()

    def append(self, *args, **kwargs):  # real signature unknown
        """ Append object to the end of the list. """
        with self.locker:
            super(ThreadSafeList, self).append(*args, **kwargs)

    def clear(self, *args, **kwargs):  # real signature unknown
        """ Remove all items from list. """
        with self.locker:
            super(ThreadSafeList, self).clear(*args, **kwargs)

    def copy(self, *args, **kwargs):  # real signature unknown
        """ Return a shallow copy of the list. """
        with self.locker:
            return super(ThreadSafeList, self).copy(*args **kwargs)

    def count(self, *args, **kwargs):  # real signature unknown
        """ Return number of occurrences of value. """
        with self.locker:
            return super(ThreadSafeList, self).count(*args, **kwargs)

    def extend(self, *args, **kwargs):  # real signature unknown
        """ Extend list by appending elements from the iterable. """
        with self.locker:
            super(ThreadSafeList, self).extend(*args, **kwargs)

    def index(self, *args, **kwargs):  # real signature unknown
        """
        Return first index of value.

        Raises ValueError if the value is not present.
        """
        with self.locker:
            return super(ThreadSafeList, self).index(*args, **kwargs)

    def insert(self, *args, **kwargs):  # real signature unknown
        """ Insert object before index. """
        with self.locker:
            super(ThreadSafeList, self).insert(*args, **kwargs)

    def pop(self, *args, **kwargs):  # real signature unknown
        """
        Remove and return item at index (default last).

        Raises IndexError if list is empty or index is out of range.
        """
        with self.locker:
            return super(ThreadSafeList, self).pop(*args, **kwargs)

    def remove(self, *args, **kwargs):  # real signature unknown
        """
        Remove first occurrence of value.

        Raises ValueError if the value is not present.
        """
        with self.locker:
            super(ThreadSafeList, self).remove(*args, **kwargs)

class ThreadSafeDict(Dict):
    def __init__(self, seq = None, **kwargs):
        if seq is not None:
            super(ThreadSafeDict, self).__init__(seq)
        elif kwargs is not None:
            super(ThreadSafeDict, self).__init__(**kwargs)
        else:
            super(ThreadSafeDict, self).__init__()
        self.locker = threading.Lock()

    def __getitem__(self, item):
        with self.locker:
            return super(ThreadSafeDict, self).__getitem__(item)

    def __setitem__(self, key, value):
        with self.locker:
            super(ThreadSafeDict, self).__setitem__(key, value)


class MyTrading(threading.Thread):
    def __init__(self,
                 in_api: sj.Shioaji,
                 in_share_holder: ShareInHouse,
                 event_to_terminate: threading.Event):
        super(MyTrading, self).__init__()
        self.event_to_terminate = event_to_terminate
        self.api = in_api
        self.share_holder = in_share_holder
        self.contracts_dict = self._get_contracts()
        self.trading_dict = {}
        self.map_id_to_token = {}
        self.event_update_trade_state = threading.Event()
        self.event_to_start_trading = threading.Event()
        self.update_queue = queue.Queue()
        self.trading_package_queue = queue.Queue()

        self.done_states = [sj.constant.Status.Failed, sj.constant.Status.Cancelled, sj.constant.Status.Filled,
                            sj.constant.Status.Inactive]
        self.filling_states = [sj.constant.Status.PartFilled]
        self.success_states = [sj.constant.Status.Filled]
        self.fail_states = [sj.constant.Status.Failed, sj.constant.Status.Cancelled]
        self.pendding_states = [sj.constant.Status.PendingSubmit, sj.constant.Status.PreSubmitted]
        self.lock_share_data = threading.Lock()
        self.max_time_to_resubmit = 10
        self.cancel_all_trading_order()
        self.api.set_order_callback(self.order_callback)
        self.order_status_uncompleted = [sj.constant.OrderState.TFTOrder, sj.constant.OrderState.FOrder,
                                         sj.constant.OrderState.Order]
        self.order_status_completed = [sj.constant.OrderState.TFTDeal, sj.constant.OrderState.FDeal,
                                       sj.constant.OrderState.Deal]
        # self.status_id_dict = {}
        # self.pending_order_dict:ThreadSafeDict = ThreadSafeDict()
        self.status_id_dict: ThreadSafeDict = ThreadSafeDict()
        self.status_queue: queue.Queue = queue.Queue()
        self.pending_order_queue: queue.Queue = queue.Queue()
        self.partfilled_order_queue: queue.Queue=queue.Queue()
        self.timedelta_withdraw_order:datetime.timedelta=datetime.timedelta(days = 0, hours = 0, minutes = 1, seconds = 0, milliseconds = 0, microseconds = 0)

    def check_trade_status(self, trade: sj.order.Trade):
        if trade.status.status not in self.done_states:
            status = "wait"
        elif trade.status.status in self.fail_states:
            status = "fail"
        elif trade.status.status in self.pendding_states and trade.order.order_type == sj.constant.ORDER_TYPE_IOC:
            status = "fail"
        else:
            status = "filled"
        return status

    def cancel_all_trading_order(self):
        trades = self.api.list_trades()
        for trade in trades:
            print(trade)
            self.api.cancel_order(trade)

    def check_status(self, token: str) -> str:
        trade_seq = self.trading_dict[token]
        status = "filled"
        for trade_info in trade_seq[0]:
            trade = trade_info["trade"]
            status = self.check_status(trade)
            if status != "filled":
                break
        return status

    def run(self) -> None:
        while not self.event_to_terminate.isSet():
            self.event_to_start_trading.wait(5)
            if self.event_to_start_trading.isSet():
                while not self.trading_package_queue.empty():
                    trade_package = self.trading_package_queue.get()
                    if trade_package["block"]:
                        index = trade_package["status"]["waited"].pop(0)
                        trade = self._submit_a_trade_to_server(trade_package, index)
                    else:
                        while len(trade_package["status"]["waited"]) > 0:
                            index = trade_package["status"]["waited"].pop(0)
                            trade = self._submit_a_trade_to_server(trade_package, index)
                self.event_to_start_trading.clear()
            self.api.update_status(self.api.futopt_account)
            self.api.update_status(self.api.stock_account)
            self.process_pending_trade()
            self.process_partfilled_trade()
        print(f'{self.__class__} is shut down')
        del self

    def order_callback(self, stype: sj.order.OrderStatus, status: dict):
        self.status_queue.put((status, stype))
        self.process_order()

    def process_order(self):
        '''
        OrderState.TFTOrder {
            'operation': {
                'op_type': 'New',
                'op_code': '00',
                'op_msg': ''
            },
            'order': {
                'id': 'c21b876d',
                'seqno': '429832',
                'ordno': 'W2892',
                'action': 'Buy',
                'price': 12.0,
                'quantity': 10,
                'order_cond': 'Cash',
                'order_lot': 'Common',
                'order_type': 'ROD',
                'price_type': 'LMT'
            },
            'status': {
                'id': 'c21b876d',
                'exchange_ts': 1583828972,
                'modified_price': 0,
                'cancel_quantity': 0
            },
            'contract': {
                'security_type': 'STK',
                'exchange': 'TSE',
                'code': '2890',
                'symbol': '',
                'name': '',
                'currency': 'TWD'
            }
        }
        '''
        '''
        OrderState.TFTDeal {
                'trade_id': '12ab3456',
                'exchange_seq': '123456',
                'broker_id': 'your_broker_id',
                'account_id': 'your_account_id',
                'action': <Action.Buy: 'Buy'>,
                'code': '2890',
                'order_cond': <StockOrderCond.Cash: 'Cash'>,
                'order_lot': <TFTStockOrderLot.Common: 'Common'>,
                'price': 12,
                'quantity': 10,
                'ts': 1583828972
            }
        '''
        '''
        OrderState.FOrder {
            "operation":{
                "op_type":"New",
                "op_code":"00",
                "op_msg":""
            },
            "order":{
                "id":"aa78ef1f",
                "seqno":"956344",
                "ordno":"ky00N",
                "action":"Sell",
                "price":17650.0,
                "quantity":4,
                "order_cond":None,
                "order_type":"ROD",
                "price_type":"LMT",
                "market_type":"Day",
                "oc_type":"New",
                "subaccount":""
            },
            "status":{
                "id":"aa78ef1f",
                "exchange_ts":1625800170,
                "modified_price":0.0,
                "cancel_quantity":0
            },
            "contract":{
                "security_type":"FUT",
                "code":"TXF",
                "exchange":"TIM",
                "delivery_month":"202107",
                "strike_price":0.0,
                "option_right":"Future"
            }
        }
        '''
        '''
        OrderState.FDeal {
            "trade_id":"aa78ef1f",
            "seqno":"956344",
            "ordno":"ky00N11O",
            "exchange_seq":"a0000060",
            "broker_id":"F002000",
            "account_id":"9104000",
            "action":"Sell",
            "code":"TXF",
            "price":17650.0,
            "quantity":4,
            "subaccount":"",
            "security_type":"FUT",
            "delivery_month":"202107",
            "strike_price":0.0,
            "option_right":"Future",
            "market_type":"Day",
            "ts":1625800369
        }
        '''
        while not self.status_queue.empty():
            status, stype = self.status_queue.get()
            print(f"<<<{stype},{status}>>>")
            if stype in self.order_status_uncompleted:
                self.process_uncompleted_trade(status)
            else:
                self.process_completed_trade(status)

    def process_trading_package(self, trade_package):
        if len(trade_package["status"]["waited"]) == 0 and len(
                trade_package["status"]["submitted"]) == 0:  # 所有的trade都完成了
            if len(trade_package["status"]["failed"]) == 0:
                cb = trade_package["callback"]
                cb(trade_package["token"], "filled")
                # print('cb(trade_package["token"],"filled")')
            else:
                cb = trade_package["callback"]
                cb(trade_package["token"], "failed")
                # print('cb(trade_package["token"], "failed")')
            self.share_holder.calc_risk_indicators()
        elif len(trade_package["status"]["waited"]) > 0:
            if not trade_package['block'] or len(trade_package["status"]["submitted"]) == 0:
                self.trading_package_queue.put(trade_package)
                self.event_to_start_trading.set()

    def process_retry(self, trade_package: dict, trade_index: int):
        trade_seq = trade_package["trade_seq"]  # [{"contract":contract, "order":order, "time_to_try":0, "trade":None}]
        time_to_try = trade_seq[trade_index]["time_to_try"] + 1
        if time_to_try >= self.max_time_to_resubmit:  # 重試次數超過上限
            with self.lock_share_data:
                if trade_package["block"]:
                    trade_package["status"]["waited"].clear()
                trade_package["status"]["failed"].append(trade_index)
                if len(trade_package["status"]["submitted"]) == 0 and len(
                        trade_package["status"]["filled"]) > 0:  # 都沒有trade被完成過
                    while len(trade_package["status"]["filled"]) > 0:
                        index = trade_package["status"]["filled"].pop(0)
                        if index not in trade_package["status"]["failed"]:
                            trade_info = trade_seq[index]
                            order = trade_info["order"]
                            trade = trade_info["trade"]
                            order.action = "Buy" if order.action == "Sell" else "Sell"
                            order.price_type = "MKT"
                            order.order_type= "Cash"
                            sum_qty = 0
                            for deal in trade.status.deals:
                                sum_qty += deal["quantity"]
                            order.quantity = sum_qty
                            del trade_info["trade"]
                            trade_info["time_to_try"] = 0
                            trade_package["status"]["waited"].append(index)
        else:  # 重試
            trade_seq[trade_index]["time_to_try"] += 1
            trade_package["status"]["waited"].insert(0, trade_index)

    def process_completed_trade(self, status: dict):
        status_id = status['trade_id']
        if status_id in self.status_id_dict.keys():
            trade_package, trade_index, trade = self.status_id_dict[status_id]

            print(f'【process_completed_trade】=> {trade.contract.name}, status={status}')
            if trade.status.status in self.success_states:  # trade成功
                trade_package['status']['submitted'].remove(trade_index)
                del self.status_id_dict[status_id]
                with self.lock_share_data:
                    trade_package['status']['filled'].append(trade_index)
            elif trade.status.status in self.done_states:  # trade完成但沒有成功
                trade_package['status']['submitted'].remove(trade_index)
                del self.status_id_dict[status_id]
                self.process_retry(trade_package, trade_index)
                # trade_seq = trade_package["trade_seq"] #[{"contract":contract, "order":order, "time_to_try":0, "trade":None}]
                # time_to_try = trade_seq[trade_index]["time_to_try"] + 1
                # if time_to_try>=self.max_time_to_resubmit:   #重試次數超過上限
                #     with self.lock_share_data:
                #         if trade_package["block"]:
                #             trade_package["status"]["waited"].clear()
                #         trade_package["status"]["failed"].append(trade_index)
                #         if len(trade_package["status"]["submitted"])==0 and len(trade_package["status"]["filled"])>0: #都沒有trade被完成過
                #             while len(trade_package["status"]["filled"])>0:
                #                 index = trade_package["status"]["filled"].pop(0)
                #                 if index not in trade_package["status"]["failed"]:
                #                     trade_info = trade_seq[index]
                #                     order = trade_info["order"]
                #                     trade = trade_info["trade"]
                #                     order.action = "Buy" if order.action=="Sell" else "Sell"
                #                     order.price_type = "MKT"
                #                     sum_qty =0
                #                     for deal in trade.status.deals:
                #                         sum_qty += deal["quantity"]
                #                     order.quantity = sum_qty
                #                     del trade_info["trade"]
                #                     trade_info["time_to_try"]=0
                #                     trade_package["status"]["waited"].append(index)
                # else: #重試
                #     trade_seq[trade_index]["time_to_try"] += 1
                #     trade_package["status"]["waited"].insert(0,trade_index)
            elif trade.status.status in self.filling_states:
                # print(f"{self.__class__}:process_completed_trade not implemented")
                self.partfilled_order_queue.put(status_id)

            self.process_trading_package(trade_package)

    def process_uncompleted_trade(self, status: dict):
        # trade_package, trade_index, trade = self.status_id_dict[status['order']['id']]
        # print(f'【{self.__class__}:process_uncompleted_trade】=> {trade.contract.name}, status={status}')
        if status["operation"]["op_msg"] != "" and status['order']['id'] in self.status_id_dict.keys():  # error occurs
            trade_package, trade_index, trade = self.status_id_dict[status['order']['id']]
            del self.status_id_dict[status['order']['id']]
            trade_package['status']['submitted'].remove(trade_index)
            self.process_retry(trade_package, trade_index)
            # trade_seq = trade_package["trade_seq"]
            # time_to_try = trade_seq[trade_index]["time_to_try"] + 1
            # if time_to_try >= self.max_time_to_resubmit:  # 重試次數超過上限
            #     with self.lock_share_data:
            #         if trade_package["block"]:
            #             trade_package["status"]["waited"].clear()
            #         trade_package["status"]["failed"].append(trade_index)
            #         if len(trade_package["status"]["submitted"]) == 0 and len(
            #                 trade_package["status"]["filled"]) > 0:  # 都沒有trade被完成過
            #             while len(trade_package["status"]["filled"]) > 0:
            #                 index = trade_package["status"]["filled"].pop(0)
            #                 if index not in trade_package["status"]["failed"]:
            #                     trade_info = trade_seq[index]
            #                     order = trade_info["order"]
            #                     trade = trade_info["trade"]
            #                     order.action = "Buy" if order.action == "Sell" else "Sell"
            #                     order.price_type = "MKT"
            #                     sum_qty = 0
            #                     for deal in trade.status.deals:
            #                         sum_qty += deal["quantity"]
            #                     order.quantity = sum_qty
            #                     del trade_info["trade"]
            #                     trade_info["time_to_try"] = 0
            #                     trade_package["status"]["waited"].append(index)
            # else:  # 重試
            #     trade_seq[trade_index]["time_to_try"] += 1
            #     trade_package["status"]["waited"].insert(0, trade_index)
            self.process_trading_package(trade_package)
    def process_partfilled_trade(self):
        not_done_list = []
        while not self.partfilled_order_queue.empty():
            status_id = self.partfilled_order_queue.get()
            if status_id in self.status_id_dict.keys():
                trade_package, trade_index, trade = self.status_id_dict[status_id]
                print(f'【{self.__class__}:process_partfilled_trade】 => trade = {trade}')
                deals = trade.status.deals
                sign = 1 if trade.order.action == sj.constant.Action.Buy else -1
                order_qty = sign * trade.order.quantity
                if len(deals) > 0:
                    for deal in deals:
                        sign = 1 if deal["action"] == sj.constant.Action.Buy else -1
                        qty = sign * deal["quantity"]
                        order_qty -= qty
                    if order_qty == 0:
                        trade_package['status']['submitted'].remove(trade_index)
                        trade_package['status']['filled'].append(trade_index)
                        del self.status_id_dict[status_id]
                        self.process_trading_package(trade_package)
                    else:
                        tnow = datetime.datetime.now()
                        order_timestamp = trade.order.order_datetime
                        time_has_past = tnow - order_timestamp
                        if time_has_past>self.timedelta_withdraw_order:
                            print(f'【{self.__class__}:process_partfilled_trade】 => time over, trade = {trade}')
                            trade_package['status']['submitted'].remove(trade_index)
                            trade_package['status']['filled'].append(trade_index)
                            del self.status_id_dict[status_id]
                            self.process_trading_package(trade_package)
                        else:
                            not_done_list.append(status_id)

        if len(not_done_list)>0:
            for sid in not_done_list:
                self.partfilled_order_queue.put(sid)


    def process_pending_trade(self):
        not_done_list = []
        while not self.pending_order_queue.empty():
            status_id = self.pending_order_queue.get()
            if status_id in self.status_id_dict.keys():
                trade_package, trade_index, trade = self.status_id_dict[status_id]
                print(f'【{self.__class__}:process_pending_trade】 => trade = {trade}')
                if trade.status.status in self.success_states:
                    with self.lock_share_data:
                        trade_package['status']['submitted'].remove(trade_index)
                        trade_package['status']['filled'].append(trade_index)
                    del self.status_id_dict[status_id]
                    print(f'filled : {trade_package["token"]}')
                    self.process_trading_package(trade_package)
                elif trade.status.status in self.fail_states:
                    with self.lock_share_data:
                        trade_package['status']['submitted'].remove(trade_index)
                    del self.status_id_dict[status_id]
                    self.process_retry(trade_package, trade_index)
                    print(f'retry : {trade_package["token"]}')
                    self.process_trading_package(trade_package)
                else:
                    not_done_list.append(status_id)

        for k in not_done_list:
            self.pending_order_queue.put(k)
            print(f'put {k} back to pending_order_queue')

    def non_blocking_cb2(self, trade: sj.order.Trade):
        with self.lock_share_data:
            trade_package, trade_index = self.trading_dict[id(trade)]
            if "id" in trade["status"].keys():
                self.status_id_dict[trade["status"]["id"]] = (trade_package, trade_index, trade)
                if trade.status.status in self.pendding_states:
                    self.pending_order_queue.put(trade.status.id)
                del self.trading_dict[id(trade)]
                # print(f'[{trade_package["token"]}]=> trade_index = {trade_index}, trade_id = {trade.status.id}, trade={trade_package["trade_seq"][trade_index]["contract"].name} - action = {trade_package["trade_seq"][trade_index]["order"].action} - price = {trade_package["trade_seq"][trade_index]["order"].price} -try {trade_package["trade_seq"][trade_index]["time_to_try"]} - status = {trade.status.status}')
                print(f'【{trade_package["token"]}】=> \n{trade} \n<=【{trade_package["token"]}】')
            else:
                print(f"{self.__class__.__name__}:non_blocking_cb2 - 'id' in trade['status'].keys() - trade[status]=>{trade['status']}")

    def non_blocking_cb(self, trade: sj.order.Trade):
        with self.lock_share_data:
            trade_package, trade_index = self.trading_dict[id(trade)]
            if (trade.status.status not in self.pendding_states or trade.order.order_type not in [
                sj.constant.ORDER_TYPE_IOC, sj.constant.ORDER_TYPE_FOK]):
                del self.trading_dict[id(trade)]
                trade_package['status']['submitted'].remove(trade_index)
            else:
                self.api.cancel_order(trade, 0, self.non_blocking_cb)
            # trade_index = -1
            # for i in trade_package['status']['submitted']:
            #     if id(trade_package["trade_seq"][i]["trade"])==id(trade):
            #         trade_index =i
            #         print(f'index = {index}')
            #         break
            # trade_index = trade_package['status']['submitted'].pop(trade_index)

            print(
                f'[{trade_package["token"]}]=> trade_index = {trade_index}, trade={trade_package["trade_seq"][trade_index]["contract"].name} - action = {trade_package["trade_seq"][trade_index]["order"].action} - price = {trade_package["trade_seq"][trade_index]["order"].price} -try {trade_package["trade_seq"][trade_index]["time_to_try"]} - status = {trade.status.status}')

        # print(f"{trade.contract.name} - status = {trade.status.status}")
        if trade.status.status in self.success_states:  # trade成功
            with self.lock_share_data:
                trade_package['status']['filled'].append(trade_index)
                # print(f"trade_package['status']['filled'].append(index)")
        elif trade.status.status in self.done_states:  # trade完成但沒有成功
            trade_seq = trade_package[
                "trade_seq"]  # [{"contract":contract, "order":order, "time_to_try":0, "trade":None}]
            # trade_index = index
            # trade_index = trade_package["index_of_trade"]
            time_to_try = trade_seq[trade_index]["time_to_try"] + 1
            # print(f'{trade.contract.name}, status = {trade.status.status}, try {time_to_try}th time')

            if time_to_try >= self.max_time_to_resubmit:  # 重試次數超過上限

                with self.lock_share_data:
                    if trade_package["block"]:
                        trade_package["status"]["waited"].clear()
                    trade_package["status"]["failed"].append(trade_index)
                    if len(trade_package["status"]["submitted"]) == 0 and len(
                            trade_package["status"]["filled"]) == 0:  # 都沒有trade被完成過
                        # cb = trade_package["callback"]
                        # cb(trade_package["token"], "failed")
                        print(f'cb(trade_package["token"], "failed")')
                        # pass
                    else:  # todo 要把之前成交的單，反向平倉
                        # trade_package["trade_seq"] = trade_seq[:trade_index+1]
                        # trade_package["index_of_trade"]=0

                        while len(trade_package["status"]["filled"]) > 0:
                            index = trade_package["status"]["filled"].pop(0)
                            if index not in trade_package["status"]["failed"]:
                                trade_info = trade_seq[index]
                                order = trade_info["order"]
                                trade = trade_info["trade"]
                                order.action = "Buy" if order.action == "Sell" else "Sell"
                                order.price_type = "MKT"
                                sum_qty = 0
                                for deal in trade.status.deals:
                                    sum_qty += deal["quantity"]
                                order.quantity = sum_qty
                                del trade_info["trade"]
                                trade_info["time_to_try"] = 0
                                trade_package["status"]["waited"].append(index)
                                # print('trade_package["status"]["waited"].append(index)')
                        # self.trading_sequence_queue.put(trade_package)
                        # self.event_to_start_trading.set()
            else:  # 重試
                trade_seq[trade_index]["time_to_try"] += 1
                trade_package["status"]["waited"].insert(0, trade_index)
                # print(f'{trade_package["status"]["waited"]}')
                # self.trading_sequence_queue.put(trade_package)
                # self.event_to_start_trading.set()

        if len(trade_package["status"]["waited"]) == 0 and len(
                trade_package["status"]["submitted"]) == 0:  # 所有的trade都完成了
            if len(trade_package["status"]["failed"]) == 0:
                cb = trade_package["callback"]
                cb(trade_package["token"], "filled")
                # print('cb(trade_package["token"],"filled")')
            else:
                cb = trade_package["callback"]
                cb(trade_package["token"], "failed")
                # print('cb(trade_package["token"], "failed")')
            self.share_holder.calc_risk_indicators()
        elif len(trade_package["status"]["waited"]) > 0:
            if not trade_package['block'] or len(trade_package["status"]["submitted"]) == 0:
                self.trading_package_queue.put(trade_package)
                self.event_to_start_trading.set()

    def prepare_a_trade(self, product_type: str, code: str, price: Union[float, Decimal, int], qty: int, action: str,
                        price_type: str = "LMT", order_type: str = "IOC", order_cond: str = "Cash",
                        octype: str = "Auto") -> Dict:
        contract = self.contracts_dict[code]
        if product_type == "stock":
            '''
                price (float or int): the price of order
                quantity (int): the quantity of order
                action (str): order action to buy or sell
                        {Buy, Sell}
                price_type (str): pricing type of order
                    {LMT, MKT, MKP}
                order_type (str): the type of order
                    {ROD, IOC, FOK}
                order_cond (str): order condition stock only
                    {Cash, MarginTrading, ShortSelling} (現股、融資、融券)
                order_lot (str): the type of order
                    {Common, Fixing, Odd, IntradayOdd} (整股、定盤、盤後零股、盤中零股)
                first_sell {str}: the type of first sell
                    {true, false}
                account (:obj:Account): which account to place this order
                ca (binary): the ca of this order
            '''
            order = self.api.Order(
                price = price,
                quantity = qty,
                action = action,
                price_type = price_type,
                order_type = order_type,
                order_lot = sj.constant.TFTStockOrderLot.Common,
                order_cond = order_cond,
                first_sell = sj.constant.StockFirstSell.No,
                account = self.api.stock_account
            )
        else:
            '''
                price (float or int): the price of order
                quantity (int): the quantity of order
                action (str): order action to buy or sell
                    {Buy, Sell}
                price_type (str): pricing type of order
                    {LMT, MKT, MKP}
                order_type (str): the type of order
                    {ROD, IOC, FOK}
                octype (str): the type or order to open new position or close position future only
                    {Auto, NewPosition, Cover, DayTrade} (自動、新倉、平倉、當沖)
                account (:obj:Account): which account to place this order
                ca (binary): the ca of this order
            '''
            order = self.api.Order(
                action = action,
                price = price,
                quantity = qty,
                price_type = price_type,
                order_type = order_type,
                octype = octype,
                account = self.api.futopt_account)
        return {"contract": contract, "order": order, "time_to_try": 0}

    def submit_a_trade_sequence(self, trade_seq: List[Dict], cb: typing.Callable[[str, str], typing.Any] = None,
                                block: bool = True) -> str:
        do_trading = True
        if not self.share_holder.is_available_capital():
            for trade_info in trade_seq:
                contract = trade_info["contract"]
                if type(contract) in [sj.contracts.Stock]:
                    share_info = self.share_holder.get_stock_info_in_house(contract.code)
                elif type(contract) in [sj.contracts.Future]:
                    share_info = self.share_holder.get_future_info_in_house(contract.code)
                else:
                    share_info = self.share_holder.get_option_info_in_house(contract.code)
                order = trade_info["order"]
                if share_info is None or order.action in [share["action"] for share in share_info]:
                    do_trading = False

        if do_trading:
            trade_token = secrets.token_hex()
            trade_status = {"submitted": [], "waited": [], "filled": [], "failed": []}
            for i in range(len(trade_seq)):
                trade_status["waited"].append(i)
            trade_package = {"token": trade_token, "trade_seq": trade_seq, "callback": cb, "index_of_trade": 0,
                             "block": block, "status": trade_status}
            self.trading_package_queue.put(trade_package)
            self.event_to_start_trading.set()
            return trade_token
        else:
            return ""

    def _submit_a_trade_to_server(self, trade_package: Dict, index: int):
        with self.lock_share_data:
            trade_info = trade_package["trade_seq"][index]
            trade_package["status"]["submitted"].append(index)
            trade = self.api.place_order(trade_info["contract"], trade_info["order"], 0, self.non_blocking_cb2)
            trade_info["trade"] = trade
            self.trading_dict[id(trade)] = (trade_package, index)
            print(f'【submit to server】=》{trade.contract.name},trade_status = {trade.status.status}')
        return trade

    def _get_contracts(self):

        contracts = {
            code: contract
            for name, iter_contract in self.api.Contracts
            for code, contract in iter_contract._code2contract.items()
        }
        return contracts


class BidAskHolder:
    def __init__(self, in_api: sj.Shioaji):
        self.api = in_api
        self.lock_update_bidask_dict = threading.Lock()
        self.broadcast_event_dict: Dict[str, threading.Event] = defaultdict(threading.Event)
        self.bidask_dict: defaultdict = defaultdict(BidAskHolder.get_default_bidask)

        # def quote_callback(topic: str, quote: dict):
        #     print("hahaha")
        #     print(f"Topic: {topic}, Quote: {quote}")
        #     bidask: BidAskFOPv1 = BidAskFOPv1(code=quote["Code"], bid_price=quote["BidPrice"], bid_volume=quote["BidVolume"], ask_price=quote["AskPrice"],ask_volume=quote["AskVolume"] )
        #     self.on_quote(bidask)
        #
        # self.api.quote.set_quote_callback(quote_callback)

        self.api.set_context(self)

        # In order to use context, set bind=True

        @self.api.on_bidask_fop_v1(bind = True)
        def quote_callback_fu(self, exchange: Exchange, bidask: BidAskFOPv1):
            # pass tick object to Stop Order Excecutor
            # print(f'{bidask.code} : {bidask.bid_price[0]} - {bidask.ask_price[0]}',end = '\r')
            self.on_quote(bidask)

        @self.api.on_bidask_stk_v1(bind = True)
        def quote_callback_st(self, exchange: Exchange, bidask: BidAskSTKv1):
            # pass tick object to Stop Order Excecutor
            # print(f'{bidask.code} : {bidask.bid_price[0]} - {bidask.ask_price[0]}', end = '\r')
            self.on_quote(bidask)

    @staticmethod
    def get_default_bidask():
        return None

    def on_quote(self, quote: Union[sj.BidAskFOPv1, sj.BidAskSTKv1]) -> None:
        code = quote.code
        # print(f'{code} : {quote.bid_price[0]}, {quote.ask_price[0]}', end='\r')
        with self.lock_update_bidask_dict:
            self.bidask_dict[code] = quote
        self.broadcast_event_dict[code].set()

    def add_bidask(self, contract: sj.contracts.Contract, notify_event: threading.Event):
        code = contract.code
        self.api.quote.subscribe(
            contract, quote_type = sj.constant.QuoteType.BidAsk, version = 'v1')
        self.broadcast_event_dict[code] = notify_event

    def remove_bidask(self, contract: sj.contracts.Contract):
        code = contract.code
        self.api.quote.unsubscribe(
            contract, quote_type = sj.constant.QuoteType.BidAsk, version = 'v1')
        del self.broadcast_event_dict[code]

    def get_latest_bidask(self, contract: sj.contracts.Contract):
        code = contract.code
        with self.lock_update_bidask_dict:
            return self.bidask_dict[code]


class MIOC(threading.Thread):
    def __init__(self, in_api: sj.Shioaji, contract: sj.contracts.Contract, order: sj.order.Order, cb: typing.Callable,
                 event_to_terminate: threading.Event):
        super(MIOC, self).__init__()
        self.contract = contract
        self.order = order
        self.cb = cb
        self.api = in_api
        self.event_callback = threading.Event()
        self.trade = None
        self.event_to_terminate = event_to_terminate

    def callback(self, trade: sj.order.Trade):
        self.trade = trade
        self.event_callback.set()

    def get_trade(self) -> sj.order.Trade:
        return self.trade

    def run(self) -> None:
        self.order.order_type = sj.constant.ORDER_TYPE_IOC
        try_times = 0
        do_again = True
        while do_again and try_times < 10 and not self.event_to_terminate.isSet():
            trade = self.api.place_order(contract = self.contract, order = self.order, timeout = 0, cb = self.callback)
            is_wait = True
            while is_wait and not self.event_to_terminate.isSet():
                self.event_callback.wait(3)
                if trade is None or trade.status.status in [sj.constant.Status.Cancelled, sj.constant.Status.Failed,
                                                            sj.constant.Status.PendingSubmit]:
                    is_wait = False
                    try_times += 1
                elif trade.status.status in [sj.constant.Status.Filled]:
                    self.cb(trade)
                    do_again = False
                    is_wait = False
                self.event_callback.clear()
                self.trade = trade


class ThreadSafeShioajiAPI(sj.Shioaji):
    def __init__(self, **kwargs):
        super(ThreadSafeShioajiAPI, self).__init__(**kwargs)
        is_simulation = kwargs.get("simulation", False)
        if not is_simulation:
            self.login(
                person_id = "S122352189",
                passwd = "baupas123",
                # contracts_timeout=100,
                contracts_cb = print,
                # contracts_cb=lambda security_type: print(f"{repr(security_type)} fetch done.")
            )
            self.activate_ca(
                ca_path = r"C:\ekey\551\S122352189\S\Sinopac.pfx",
                ca_passwd = "S122352189",
                person_id = "S122352189",
            )
        else:
            self.login(
                person_id = "PAPIUSER03",
                passwd = "2222",
                contracts_cb = lambda security_type: print(
                    f"{repr(security_type)} fetch done.")
            )
        self.lock_to_access_api = threading.Lock()
        self.fp = open(f"{datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.csv", "at")
        self.event_multiple_IOC = threading.Event()
        import time
        while "TXO" not in self.Contracts.Options.keys() or  self.Contracts.Options.TXO is None:
            time.sleep(1)

    def account_balance(
            self, timeout: int = 5000, cb: typing.Callable[[sj.shioaji.AccountBalance], None] = None,
    ):
        with self.lock_to_access_api:
            return super(ThreadSafeShioajiAPI, self).account_balance(timeout, cb)

    def get_account_margin(self, currency = "NTD", margin_type = "1", account = {}):
        with self.lock_to_access_api:
            return super(ThreadSafeShioajiAPI, self).get_account_margin(currency, margin_type, account)

    def multiple_IOC_callback(self, trade: sj.order.Trade):
        self.event_multiple_IOC.set()

    def place_order(
            self,
            contract: sj.contracts.Contract,
            order: sj.order.Order,
            timeout: int = 5000,
            cb: typing.Callable[[sj.order.Trade], None] = None,
    ) -> sj.order.Trade:
        global g_record_only
        global g_record
        with self.lock_to_access_api:
            if g_record_only:
                self.fp.write(
                    f"{contract.name},{order.action},{order.quantity},{order.price},{order.order_cond},{order.order_type}\n")
                self.fp.flush()
                return None
            else:
                if g_record:
                    self.fp.write(
                        f"{contract.name},{order.action},{order.quantity},{order.price},{order.order_cond},{order.order_type}\n")
                    self.fp.flush()
                return super(ThreadSafeShioajiAPI, self).place_order(contract, order, timeout, cb)

    def list_positions(
            self,
            account: sj.account.Account = None,
            unit: sj.constant.Unit = sj.constant.Unit.Common,
            timeout: int = 5000,
            cb: typing.Callable[[typing.List[sj.shioaji.Position]], None] = None,
    ) -> typing.List[sj.shioaji.Position]:
        with self.lock_to_access_api:
            return super(ThreadSafeShioajiAPI, self).list_positions(account, unit, timeout, cb)

    def get_account_openposition(self, product_type = "0", query_type = "0", account = {}):
        with self.lock_to_access_api:
            return super(ThreadSafeShioajiAPI, self).get_account_openposition(product_type, query_type, account)

    def update_order(
            self,
            trade: sj.order.Trade,
            price: typing.Union[sj.order.StrictInt, float] = None,
            qty: int = None,
            timeout: int = 5000,
            cb: typing.Callable[[sj.order.Trade], None] = None,
    ) -> sj.order.Trade:
        with self.lock_to_access_api:
            return super(ThreadSafeShioajiAPI, self).update_order(trade, price, qty, timeout, cb)

    def cancel_order(
            self,
            trade: sj.order.Trade,
            timeout: int = 5000,
            cb: typing.Callable[[sj.order.Trade], None] = None,
    ) -> sj.order.Trade:
        with self.lock_to_access_api:
            return super(ThreadSafeShioajiAPI, self).cancel_order(trade, timeout, cb)

    def update_status(
            self,
            account: sj.account.Account = None,
            timeout: int = 5000,
            cb: typing.Callable[[typing.List[sj.order.Trade]], None] = None,
    ):
        with self.lock_to_access_api:
            try:
                super(ThreadSafeShioajiAPI, self).update_status(account, timeout, cb)
            except:
                pass

    def account_balance(
            self, timeout: int = 5000, cb: typing.Callable[[sj.shioaji.AccountBalance], None] = None,
    ):
        with self.lock_to_access_api:
            return super(ThreadSafeShioajiAPI, self).account_balance(timeout, cb)


class ConditionBase(abc.ABC):
    @abc.abstractmethod
    def check(self) -> None:
        pass

    @abc.abstractmethod
    def get_contracts(self) -> List[sj.contracts.Contract]:
        return []

    @abc.abstractmethod
    def get_codes(self) -> List[str]:
        return []

    @abc.abstractmethod
    def add_qty_in_house(self, code: str, quantity: int) -> None:
        pass

    @abc.abstractmethod
    def set_qty_for_first_sell(self, code: str, quantity: int) -> None:
        pass


class OptionNeturalCondition(ConditionBase):
    def __init__(self,
                 in_api: sj.Shioaji,
                 contracts: List[sj.contracts.Contract],
                 bidask_holder: BidAskHolder,
                 trading_holder: MyTrading,
                 notify_event_to_terminate: threading.Event):
        self.api = in_api
        self.bidask_holder = bidask_holder
        self.trading_holder = trading_holder
        self.notify_event_to_terminate = notify_event_to_terminate
        self.contracts = contracts  # strike_price, option_right=OptionRight.Put/Call
        nowdt = datetime.datetime.now()
        endate = datetime.datetime.strptime('2021-9-15 13:30:00', '%Y-%m-%d %H:%M:%S')
        T = ((endate - nowdt).days + (endate - nowdt).seconds / 3600.0 / 24.0) / 225.0
        self.delta_call(17209.93, 17000, 20 / 225.0, 0.01, 425)

    def delta_call(self, S, K, T, r, c):
        '''
        S指數價，
        K履約價<
        T=到期剩餘時間(年化)
        r=無風險利率(就放1年定存利率)
        c 買權成交價
        '''
        implied_vol = 1.0
        min_value = 100.0
        tts = 0.0001
        for i in range(0, 10000, 1):
            sigma = implied_vol - tts * i
            d1 = (numpy.lib.scimath.log(S / K) + (r + sigma * sigma / 2.0) * T) / (sigma * numpy.lib.scimath.sqrt(T))
            d2 = d1 - sigma * numpy.lib.scimath.sqrt(T)
            delta_value = stats.norm.cdf(d2)
            call = S * stats.norm.cdf(d1) - K * numpy.exp(-r * T) * stats.norm.cdf(d2)
            abs_diff = abs(call - c)
            if abs_diff <= min_value:
                min_value = abs_diff
                implied_vol = sigma

        return implied_vol * 100

    def check(self) -> None:
        bidasks = [self.bidask_holder.get_latest_bidask(contr) for contr in self.contracts]
        print('OptionNeturalCondition::Check()')

    def get_contracts(self) -> List[sj.contracts.Contract]:
        return self.contracts

    def get_codes(self) -> List[str]:
        ret_list = [k.code for k in self.contracts]
        return ret_list

    def add_qty_in_house(self, code: str, quantity: int) -> None:
        pass

    def set_qty_for_first_sell(self, code: str, quantity: int) -> None:
        pass


class ArbitrageCondition(ConditionBase):
    def __init__(self,
                 in_api: sj.Shioaji,
                 bidask_holder: BidAskHolder,
                 share_holder: ShareInHouse,
                 trading_holder: MyTrading,
                 price_change_interval: Dict,
                 min_interval_by_price: Dict,
                 # order_mointor:OrderMonitoring,
                 max_order_qty: Dict,
                 contract_st: sj.contracts.Contract,
                 contract_fu: sj.contracts.Contract,
                 notify_event_to_terminate: threading.Event):
        self.api = in_api
        self.price_change_interval = price_change_interval
        self.contract_st = contract_st
        self.contract_fu = contract_fu
        self.bidask_holder = bidask_holder
        self.share_holder = share_holder
        self.trading_holder = trading_holder
        self.min_interval_by_price = min_interval_by_price
        self.trade = None
        self.max_amount_st = max_order_qty[contract_st.code]
        self.max_amount_fu = max_order_qty[contract_fu.code]
        self.min_required_volume_for_stock = 4
        self.min_required_volume_for_future = 2
        self.order_qty = Counter({contract_st.code: 0, contract_fu.code: 0})
        self.deal_qty = Counter({contract_st.code: 0, contract_fu.code: 0})
        self.deal_price = Counter({contract_st.code: 0, contract_fu.code: 0})
        self.max_amount = Counter(
            {contract_st.code: max_order_qty[contract_st.code], contract_fu.code: max_order_qty[contract_fu.code]})
        self.default_gain = Counter({'forwardation': 0.5, 'backwardation': 0.5})
        self.min_gain = Counter({'forwardation': 0.0, 'backwardation': 0.0})
        self.amount_per_trading = Counter({'stock': 2, 'future': 1})
        self.max_amount_for_first_sell = Counter({contract_st.code: 0, contract_fu.code: 0})
        # self.order_monitor=order_mointor
        self.orders: List[Tuple[str, str, Union[float, Decimal], sj.order.Trade]] = []
        self.order_queue = queue.Queue(1024)
        self.notify_event_for_order_callback = threading.Event()
        self.notify_event_to_terminate = notify_event_to_terminate
        # self.wait_for_order_feedback_thr = threading.Thread(target = self.wait_for_order_feedback)
        # self.wait_for_order_feedback_thr.start()
        self.time_to_close_all_backwardation = datetime.time(hour = 13, minute = 10, second = 0, microsecond = 0)
        self.time_to_call_auction = datetime.time(hour = 13, minute = 25, second = 0, microsecond = 0)
        self.time_to_close_market = datetime.time(hour = 13, minute = 30, second = 0, microsecond = 0)
        self.time_to_open_market = datetime.time(hour = 9, minute = 0, second = 0, microsecond = 0)
        self.time_to_pause_submit_order = datetime.time(hour = 8, minute = 00, second = 0, microsecond = 0)
        self.lock_for_sharing_data = threading.Lock()

        self.trade_status = {}
        self.submitted_trade = {}
        self.check_shares_in_house()

    def check_shares_in_house(self):
        stock_shares = self.share_holder.get_stock_info_in_house(self.contract_st.code)
        sum_value = 0
        sum_amount = 0
        if stock_shares is not None:
            for stock in stock_shares:
                sign = 1 if stock['action'] == 'Buy' else -1
                sum_value += stock['qty'] * stock['price']
                sum_amount += stock['qty']
                self.add_qty_in_house(self.contract_st.code, stock['qty'] * sign)
                print(
                    f'found {self.contract_st.name} - qty = {stock["qty"]}, direction= {stock["action"]} in house')
            self.deal_price[self.contract_st.code] = sum_value/sum_amount

        sum_value = 0
        sum_amount = 0
        future_shares = self.share_holder.get_future_info_in_house(self.contract_fu.code)
        if future_shares is not None:
            for future in future_shares:
                sign = 1 if future['action'] == 'Buy' else -1
                sum_value += future['qty'] * future['price']
                sum_amount += future['qty']
                self.add_qty_in_house(self.contract_fu.code, future['qty'] * sign)
                print(
                    f'found {self.contract_fu.name} - qty = {future["qty"]}, direction= {future["action"]} in house')
            self.deal_price[self.contract_fu.code] = sum_value / sum_amount

    def trading_callback(self, token: str, status: str):
        # status : "filled" or "failed"
        self.trade_status[token] = status
        with self.lock_for_sharing_data:
            if status == "filled":
                self.deal_qty[self.contract_st.code] += self.submitted_trade[token]["stock"]
                self.deal_qty[self.contract_fu.code] += self.submitted_trade[token]["future"]

            self.order_qty[self.contract_st.code] -= self.submitted_trade[token]["stock"]
            self.order_qty[self.contract_fu.code] -= self.submitted_trade[token]["future"]

        print(f'token = {token} - status = {status}')

    def add_qty_in_house(self, code: str, quantity: int) -> None:
        with self.lock_for_sharing_data:
            self.deal_qty[code] += quantity
            # self.order_qty[code] -= quantity

    def set_qty_for_first_sell(self, code: str, quantity: int) -> None:
        with self.lock_for_sharing_data:
            self.max_amount_for_first_sell[code] = quantity

    def get_codes(self) -> List[str]:
        return [self.contract_st.code, self.contract_fu.code]

    def get_contracts(self) -> List[sj.contracts.Contract]:
        return [self.contract_st, self.contract_fu]

    def non_blocking_cb_st(self, trade: sj.order.Trade):
        # print('non_block_callback stock....')
        # print(f'{trade.contract.name} : {trade.status}')
        self.notify_event_for_order_callback.set()

    def non_blocking_cb_fu(self, trade: sj.order.Trade):
        # print('non_block_callback future....')
        # print(f'{trade.contract.name} : {trade.status}')
        self.notify_event_for_order_callback.set()

    def submit_a_stock_position(self, contract: sj.contracts.Stock, qty: int, price: Union[Decimal, float],
                                action: sj.order.Action) -> sj.order.Trade:
        now = datetime.datetime.now()
        now_time = now.time()  # datetime.time(hour = now.hour, minute = now.minute, second = now.second, microsecond = now.microsecond)
        order_type = sj.constant.TFTOrderType.ROD if now_time > self.time_to_call_auction else sj.constant.TFTOrderType.IOC
        order_cond = sj.constant.STOCK_ORDER_COND_CASH if action == sj.order.Action.Buy or self.deal_qty[
            contract.code] >= qty else sj.constant.STOCK_ORDER_COND_SHORTSELLING
        order = self.api.Order(
            price = price,
            quantity = qty,
            action = action,
            price_type = sj.constant.StockPriceType.LMT,
            order_type = order_type,
            order_lot = sj.constant.TFTStockOrderLot.Common,
            first_sell = sj.constant.StockFirstSell.No,
            order_cond = order_cond,
            account = self.api.stock_account
        )
        if False:  # order.order_type==sj.constant.ORDER_TYPE_IOC:
            new_thread = MIOC(self.api, contract, order, self.non_blocking_cb_st, self.notify_event_to_terminate)
            new_thread.start()
            new_thread.join()
            trade = new_thread.get_trade()
        else:
            trade = self.api.place_order(
                contract,
                order,
                # timeout = 0,
                # cb = self.non_blocking_cb_st  # only work in non-blocking mode
            )
        return trade

    def cancel_a_stock_position(self, trade: sj.order.Trade) -> None:
        self.api.update_status(self.api.stock_account)
        self.api.cancel_order(trade)
        self.api.update_status(self.api.stock_account)

    def submit_a_future_position(self, contract: sj.contracts.Future, qty: int, price: Union[Decimal, float],
                                 action: sj.order.Action) -> sj.order.Trade:
        order = self.api.Order(
            action = action,
            price = price,
            quantity = qty,
            price_type = 'LMT',
            order_type = 'IOC',
            octype = sj.constant.FuturesOCType.Auto,
            account = self.api.futopt_account
        )
        if False:  # order.order_type==sj.constant.ORDER_TYPE_IOC:
            new_thread = MIOC(self.api, contract, order, self.non_blocking_cb_st, self.notify_event_to_terminate)
            new_thread.start()
            new_thread.join()
            trade = new_thread.get_trade()
        else:
            trade = self.api.place_order(
                contract,
                order,
                # timeout = 0,
                # cb = self.non_blocking_cb_fu  # only work in non-blocking mode
            )
        return trade

    def close_a_future_position(self, contract: sj.contracts.Future, qty: int, price: Union[Decimal, float],
                                action: sj.order.Action) -> sj.order.Trade:
        order = self.api.Order(
            action = action,
            price = price,
            quantity = qty,
            price_type = 'LMT',
            order_type = 'IOC',
            octype = sj.constant.FuturesOCType.Cover,
            account = self.api.futopt_account
        )
        trade = self.api.place_order(
            contract,
            order,
            timeout = 0,
            cb = self.non_blocking_cb_fu  # only work in non-blocking mode
        )
        return trade

    def cancel_a_future_position(self, trade: sj.order.Trade) -> None:
        self.api.update_status(self.api.futopt_account)
        try:
            self.api.cancel_order(trade)
        except:
            pass
        self.api.update_status(self.api.futopt_account)

    def wait_for_order_feedback(self):
        # done 如果PendingSubmit卡太久，就把单子取消掉，把order_qty加回来 trade.status.order_datetime cancel_trade
        # todo 如果成交， 要跟库存做比对确认

        done_states = [sj.constant.Status.Failed, sj.constant.Status.Cancelled, sj.constant.Status.Filled,
                       sj.constant.Status.Inactive]
        timedelta_to_cancel = datetime.timedelta(days = 0, hours = 0, minutes = 0, seconds = 10, microseconds = 0,
                                                 weeks = 0, milliseconds = 0)
        while not self.notify_event_to_terminate.isSet():
            self.notify_event_for_order_callback.wait(5)
            if self.notify_event_for_order_callback.isSet():
                self.notify_event_for_order_callback.clear()
                # qsize = self.order_queue.qsize()
                # for _ in range(qsize):
                while not self.order_queue.empty():
                    orders = self.order_queue.get()
                    st_sum = 0
                    fu_sum = 0
                    st_submitted_qty = 0
                    fu_submitted_qty = 0
                    is_completed: bool = True
                    for order in orders:
                        trade: sj.order.Trade = order[3]
                        if trade is None:
                            print("when simulation no trade is made")
                            continue
                        elif trade.status.status == sj.constant.Status.PendingSubmit:
                            now = datetime.datetime.now()
                            submited_time = trade.status.order_datetime
                            dif_time = now - submited_time
                            if dif_time > timedelta_to_cancel:
                                if type(trade.contract) in [sj.contracts.Future]:
                                    self.cancel_a_future_position(trade)
                                elif type(trade.contract) in [sj.contracts.Stock]:
                                    self.cancel_a_stock_position(trade)
                            else:
                                is_completed = False
                        else:
                            is_completed = is_completed and trade.status.status in done_states

                        print(f'status of {trade.contract.name} = {trade.status.status}')
                        # print(trade)
                        if type(trade.contract) in [sj.contracts.Stock]:
                            sign = 1 if trade.order.action == sj.constant.Action.Buy else -1
                            qty = trade.order.quantity * sign
                            st_submitted_qty += qty
                            if trade.status.status in [sj.constant.Status.Filled]:
                                for deal in trade.status.deals:
                                    sign = 1 if deal['action'] == sj.constant.Action.Buy else -1
                                    qty = deal['quantity'] * sign
                                    st_sum += qty
                        elif type(trade.contract) in [sj.contracts.Future]:
                            sign = 1 if trade.order.action == sj.constant.Action.Buy else -1
                            qty = trade.order.quantity * sign
                            fu_submitted_qty += qty
                            if trade.status.status in [sj.constant.Status.Filled]:
                                for deal in trade.status.deals:
                                    sign = 1 if deal['action'] == sj.constant.Action.Buy else -1
                                    qty = deal['quantity'] * sign
                                    fu_sum += qty
                        else:
                            raise NotImplementedError(
                                "unexpected onctract type in ArbitrageCondition::wait_for_order_feedback")

                    if is_completed:
                        # 期貨現貨的張數不對
                        if (st_sum + fu_sum * 2) != 0:
                            print(
                                f'xxxxx  submitted stock qty = {st_submitted_qty}, filled qty ={st_sum}, submitted future qty ={fu_submitted_qty}, filled qty = {fu_sum}')
                            if abs(st_sum) > abs(fu_sum * 2):
                                qty = abs(st_sum) - abs(fu_sum * 2)
                                bid_st = self.bidask_holder.get_latest_bidask(self.contract_st)
                                if st_sum > 0:
                                    action = sj.constant.Action.Sell
                                    price = bid_st.bid_price[1]
                                else:
                                    action = sj.constant.Action.Buy
                                    price = bid_st.ask_price[1]
                                trade = self.submit_a_stock_position(self.contract_st, qty, price, action)
                                orders.append((self.contract_st.code,
                                               "Sell" if action == sj.constant.Action.Sell else "Buy", price, trade))
                            else:
                                qty = abs(fu_sum) - int(abs(st_sum / 2))
                                bid_fu = self.bidask_holder.get_latest_bidask(self.contract_fu)
                                if fu_sum > 0:
                                    action = sj.constant.Action.Sell
                                    price = bid_fu.bid_price[1]
                                else:
                                    action = sj.constant.Action.Buy
                                    price = bid_fu.ask_price[1]
                                trade = self.close_a_future_position(self.contract_fu, qty, price, action)
                                orders.append((self.contract_fu.code,
                                               "Sell" if action == sj.constant.Action.Sell else "Buy", price, trade))
                            self.order_queue.put(orders)
                        else:
                            print(
                                f'====  submitted stock qty = {st_submitted_qty}, filled qty ={st_sum}, submitted future qty ={fu_submitted_qty}, filled qty = {fu_sum}')
                            dif_st = st_submitted_qty - st_sum
                            dif_fu = fu_submitted_qty - fu_sum
                            with self.lock_for_sharing_data:
                                self.deal_qty[self.contract_st.code] += st_sum
                                self.deal_qty[self.contract_fu.code] += fu_sum
                                self.order_qty[self.contract_st.code] -= dif_st
                                self.order_qty[self.contract_fu.code] -= dif_fu
                    else:
                        # print(len(orders))
                        self.order_queue.put(orders)

                self.share_holder.update()
                st_sum_in_house = 0
                stock_position = self.share_holder.get_stock_info_in_house(self.contract_st.code)
                if stock_position is not None:
                    sign = 1 if stock_position["action"] == "Buy" else -1
                    qty = stock_position["qty"] * sign
                    st_sum_in_house += qty

                self.deal_qty[self.contract_st.code] = st_sum_in_house

                fu_sum_in_house = 0

                future_position = self.share_holder.get_future_info_in_house(self.contract_fu.code)
                if future_position is not None:
                    sign = 1 if future_position["action"] == "Buy" else -1
                    qty = future_position["qty"] * sign
                    fu_sum_in_house += qty

                self.deal_qty[self.contract_st.code] = st_sum_in_house
                # 如果期貨現貨數量不一致， 要當衝掉
                if (st_sum_in_house + 2 * fu_sum_in_house) != 0:
                    if abs(st_sum_in_house) > abs(fu_sum_in_house * 2):
                        qty = abs(st_sum_in_house) - abs(fu_sum_in_house * 2)
                        bid_st = self.bidask_holder.get_latest_bidask(self.contract_st)
                        if bid_st is not None:
                            if st_sum_in_house > 0:
                                action = sj.constant.Action.Sell
                                price = bid_st.bid_price[1]
                            else:
                                action = sj.constant.Action.Buy
                                price = bid_st.ask_price[1]
                            # trade = self.submit_a_stock_position(self.contract_st, qty, price, action)
                            # print("trade = self.submit_a_stock_position(self.contract_st, qty, price, action)")
                            # self.order_queue.put(
                            #     [(self.contract_st.code, "Sell" if action == sj.constant.Action.Sell else "Buy", price, trade)])
                        else:
                            print(
                                f'qty in house is not matched but cannot get price: {self.contract_st.name}={st_sum_in_house}, {self.contract_fu.name}={fu_sum_in_house}')
                    else:
                        qty = abs(fu_sum_in_house) - int(abs(st_sum_in_house / 2))
                        bid_fu = self.bidask_holder.get_latest_bidask(self.contract_fu)
                        if bid_fu is not None:
                            if fu_sum_in_house > 0:
                                action = sj.constant.Action.Sell
                                price = bid_fu.bid_price[1]
                            else:
                                action = sj.constant.Action.Buy
                                price = bid_fu.ask_price[1]
                            # trade = self.close_a_future_position(self.contract_fu, qty, price, action)
                            # print("trade = self.close_a_future_position(self.contract_fu, qty, price, action)")
                            # self.order_queue.put(
                            #     [(self.contract_fu.code, "Sell" if action == sj.constant.Action.Sell else "Buy", price, trade)])
                        else:
                            print(
                                f'qty in house is not matched but cannot get price : {self.contract_st.name}={st_sum_in_house}, {self.contract_fu.name}={fu_sum_in_house}')

                self.adjust_min_gain()
        print(f"exist wait for order feedback {self.contract_st.name}")
        del self

    def adjust_min_gain(self):
        now = datetime.datetime.now()
        now_time = now.time()  # datetime.time(hour = now.hour, minute = now.minute, second = now.second, microsecond = now.microsecond)
        with self.lock_for_sharing_data:
            if self.time_to_close_all_backwardation < now_time < self.time_to_call_auction and self.deal_qty[
                self.contract_st.code] < 0:
                self.min_gain['forwardation'] = -1
                self.min_gain['backwardation'] = 999
                # print(
                #     f'{self.contract_st.name}:forwardation gain={self.min_gain["forwardation"]}, backwardation gain={self.min_gain["backwardation"]}')
            elif self.time_to_pause_submit_order < now_time < self.time_to_open_market:
                self.min_gain['forwardation'] = 999
                self.min_gain['backwardation'] = 999
            elif self.time_to_call_auction < now_time <= self.time_to_close_market:
                self.min_gain['forwardation'] = 999
                self.min_gain['backwardation'] = 999
            else:
                ratio_of_full_positions = self.deal_qty[self.contract_st.code] / float(
                    self.max_amount[self.contract_st.code])
                self.min_gain['forwardation'] = max(0, self.default_gain['forwardation'] + math.ceil(
                    self.default_gain['forwardation'] * ratio_of_full_positions))
                self.min_gain['backwardation'] = max(0, self.default_gain['backwardation'] - math.ceil(
                    self.default_gain['backwardation'] * ratio_of_full_positions))

    def check(self) -> None:
        bidask_st = self.bidask_holder.get_latest_bidask(self.contract_st)
        bidask_fu = self.bidask_holder.get_latest_bidask(self.contract_fu)
        if bidask_st is not None and bidask_fu is not None:
            self.check_and_submit_order_for_open_a_position(bidask_st, bidask_fu)

    def check_and_submit_order_for_open_a_position(self, stock_bidask, future_bidask):
        global g_enable_trading
        contr_st: sj.contracts.Contract = self.contract_st
        contr_fu: sj.contracts.Contract = self.contract_fu
        st = contr_st.code
        fu = contr_fu.code

        # print(contr_st.name, contr_fu.name)
        # bid_st, bid_fu = self.api.snapshots([contr_st, contr_fu])
        bid_st = stock_bidask
        bid_fu = future_bidask
        buy_price = (bid_st.ask_price[0])
        buy_volume = bid_st.ask_volume[0]
        sell_price = (bid_fu.bid_price[0])
        sell_volume = bid_fu.bid_volume[0]
        now_time = datetime.datetime.now().time()
        # skip if now is not trading time
        if self.time_to_pause_submit_order <= now_time < self.time_to_open_market or self.time_to_call_auction <= now_time <= self.time_to_close_market or buy_price < 1 or sell_price < 1:
            # print(
            # f'{datetime.datetime.now()}: {contr_st.name} - {bid_st.bid_price[0]:.2f}/{bid_st.bid_volume[0]} - {bid_st.ask_price[0]:.2f}/{bid_st.ask_volume[0]}, {contr_fu.name} - {bid_fu.bid_price[0]:.2f}/{bid_fu.bid_volume[0]} - {bid_fu.ask_price[0]:.2f}/{bid_fu.ask_volume[0]}',end = '\r')
            return
        # print(f'({contr_st.name},{contr_fu.name}', end='\r')
        # cost = bid_st.bid_price[0] * (0.001425*2*0.3*2000+0.003) +bid_fu.ask_price[0] * (2000*2/100000.0)*2+(45*2)
        # cost = buy_price * \
        #        Decimal((0.001425 * 1 * 0.3 + 0.003) * 2000) + \
        #        sell_price * Decimal((2000 * 2 / 100000.0) * 1) + Decimal(18 * 1)
        # value_diff = Decimal((sell_price - buy_price) * 2000)
        # #
        # forwardation_margin = value_diff - cost

        # min_qty_for_deal=0
        # for q in range(2,20,2):
        #     _cost = buy_price * Decimal((0.001425*2*0.3+0.003)*q*1000) + sell_price * Decimal((2*2/100000.0)*1000*q)+Decimal(18*q)
        #     _value_diff = Decimal((sell_price - buy_price)*1000*q)
        #     if ((_value_diff-_cost) > self.price_change_interval[int(buy_price)]*self.min_gain['forwardation']*1000*q):
        #         min_qty_for_deal=q
        #         cost=_cost
        #         value_diff=_value_diff
        #         break
        is_first_buy = self.deal_qty[st] <= 0 and self.time_to_open_market < now_time < self.time_to_call_auction
        extra_gain_for_first_buy = 1 if is_first_buy else 0
        exist_margin = self.deal_price[contr_st.code] - self.deal_price[contr_fu.code] if self.deal_qty[contr_fu.code]!=0 and self.deal_qty[contr_st.code]!=0 and (self.deal_qty[st] + self.order_qty[st])!=0 else 0
        min_margin = (Decimal(self.min_interval_by_price[int(buy_price)][0]) - int(buy_price))
        expected_margin = Decimal(sell_price - buy_price + exist_margin)
        forwardation_margin = expected_margin
        price_verify: bool = contr_st.limit_up >= buy_price >= contr_st.limit_down and \
                             contr_fu.limit_down <= sell_price <= contr_fu.limit_up and \
                             sell_price>=buy_price and exist_margin>=0 and \
                             ((sell_price - buy_price) * 2 > (Decimal(self.price_change_interval[int(buy_price)]) * Decimal(extra_gain_for_first_buy) + min_margin) or expected_margin > min_margin)
        volume_verify: bool = buy_volume > self.min_required_volume_for_stock and \
                              sell_volume > self.min_required_volume_for_future and \
                              (abs(self.deal_qty[st] + self.order_qty[st] + self.amount_per_trading["stock"]) <= self.max_amount[st] or abs(self.deal_qty[st] + self.order_qty[st] + self.amount_per_trading["stock"])< abs(self.deal_qty[st] + self.order_qty[st])) # and self.max_amount[st] >= abs(order_qty_by_code+self.amount_per_trading["stock"])
        # print(
        #     f'{datetime.datetime.now()}: {contr_st.name} - {bid_st.bid_price[0]:.2f}/{bid_st.bid_volume[0]} - {bid_st.ask_price[0]:.2f}/{bid_st.ask_volume[0]}, {contr_fu.name} - {bid_fu.bid_price[0]:.2f}/{bid_fu.bid_volume[0]} - {bid_fu.ask_price[0]:.2f}/{bid_fu.ask_volume[0]}, F=>{forwardation_margin:.2f},B=>???，TB={self.price_change_interval[int(buy_price)] * self.min_interval_by_price[int(buy_price)][1]:.2f}，PT={price_verify}, VT={volume_verify}   ',
        #     end = '\r')
        # try:
        #     print(f'{self.min_interval_by_price[int(buy_price)][1]}',end = '\r')
        # except:
        #     print(f'\n\n{int(buy_price)}')
        #     print("Unexpected error:", sys.exc_info()[0],"\n\n")
        #     raise
        # print(
        #     f'{datetime.datetime.now()}: {contr_st.name} - {bid_st.bid_price[0]:.2f}/{bid_st.bid_volume[0]} - {bid_st.ask_price[0]:.2f}/{bid_st.ask_volume[0]}, {contr_fu.name} - {bid_fu.bid_price[0]:.2f}/{bid_fu.bid_volume[0]} - {bid_fu.ask_price[0]:.2f}/{bid_fu.ask_volume[0]}, F=>{forwardation_margin:.2f},B=>???，PT={price_verify}, VT={volume_verify}   ',
        #     end = '\r')
        if volume_verify and price_verify:  # and ((value_diff-cost) > self.price_change_interval[int(buy_price)]*self.min_gain['forwardation']*2000):
            print(
                f'\nbuy {contr_st.name}@{buy_price:.2f}*{self.amount_per_trading["stock"]} and sell {contr_fu.name}@{sell_price:.2f}*{self.amount_per_trading["future"]}, expected_margin = {expected_margin}, interval={self.price_change_interval[int(buy_price)]}')

            if g_enable_trading:
                # trade_st = self.submit_a_stock_position(contr_st,self.amount_per_trading["stock"],bid_st.ask_price[0],sj.constant.Action.Buy)
                # if trade_st is not None:
                #     if trade_st.status.status in [sj.constant.Status.Filled]:
                #         with self.lock_for_sharing_data:
                #             self.order_qty[st] += self.amount_per_trading["stock"]
                #             self.order_qty[fu] -= self.amount_per_trading["future"]
                #         trade_fu = self.submit_a_future_position(contr_fu,self.amount_per_trading["future"],bid_fu.bid_price[0], sj.constant.Action.Sell)
                #         self.order_queue.put([(st, "Buy", buy_price, trade_st), (fu, "Sell", sell_price, trade_fu)])
                trades = []
                trades.append(self.trading_holder.prepare_a_trade("future", fu, bid_fu.bid_price[0],
                                                                  self.amount_per_trading["future"], "Sell"))
                trades.append(self.trading_holder.prepare_a_trade("stock", st, bid_st.ask_price[0],
                                                                  self.amount_per_trading["stock"], "Buy",
                                                                  order_type = "FOK"))
                with self.lock_for_sharing_data:
                    token = self.trading_holder.submit_a_trade_sequence(trades, self.trading_callback,block = False)
                    if token != "":
                        self.order_qty[st] += self.amount_per_trading["stock"]
                        self.order_qty[fu] -= self.amount_per_trading["future"]
                        self.submitted_trade[token] = {"stock": self.amount_per_trading["stock"],
                                                       "future": -self.amount_per_trading["future"]}
                    print(
                        f'token = {token}:sell {contr_fu.name} buy {contr_st.name}, self.deal_qty[st]={self.deal_qty[st]}, self.order_qty[st]= {self.order_qty[st]}')

        buy_price = (bid_fu.ask_price[0])
        buy_volume = (bid_fu.ask_volume[0])
        sell_price = (bid_st.bid_price[0])
        sell_volume = (bid_st.bid_volume[0])
        # cost = sell_price * \
        #        Decimal((0.001425 * 1 * 0.3 + 0.003) * 2000) + \
        #        buy_price * Decimal((2000 * 2 / 100000.0) * 1) + Decimal(18 * 1)
        # value_diff = (sell_price - buy_price) * Decimal(2000)
        # backwardation_margin = value_diff - cost
        # min_qty_for_deal = 0
        # for q in range(2, 20, 2):
        #     _cost = sell_price * Decimal((0.001425 * 2 * 0.3 + 0.003) * q * 1000) + buy_price * Decimal(
        #         (2 * 2 / 100000.0) * 1000 * q) + Decimal(18 * q)
        #     _value_diff = Decimal((sell_price - buy_price) * 1000 * q)
        #     if ((_value_diff - _cost) > self.price_change_interval[int(buy_price)] * self.min_gain['backwardation'] * 1000 *q):
        #         min_qty_for_deal = q
        #         cost = _cost
        #         value_diff=_value_diff
        #         break

        is_first_sell = self.deal_qty[st] <= 0 and self.time_to_open_market < now_time < self.time_to_call_auction
        extra_gain_for_first_sell = 2 if is_first_sell else 0
        exist_margin = self.deal_price[contr_fu.code] - self.deal_price[contr_st.code] if self.deal_qty[contr_fu.code] != 0 and self.deal_qty[contr_st.code] != 0 and (self.deal_qty[st] + self.order_qty[st])!=0 else 0
        min_margin = Decimal(self.min_interval_by_price[int(buy_price)][0] - int(buy_price))
        expected_margin = Decimal(sell_price - buy_price + exist_margin)
        backwardation_margin= expected_margin
        price_verify = contr_st.limit_up >= buy_price >= contr_st.limit_down and \
                       contr_fu.limit_down <= sell_price <= contr_fu.limit_up and \
                       sell_price>=buy_price and \
                       exist_margin>=0 and \
                       (Decimal(sell_price - buy_price) * 2 > (Decimal(self.price_change_interval[int(buy_price)]) * Decimal(extra_gain_for_first_sell) + min_margin) or expected_margin > min_margin)  # and ((value_diff-cost) > self.price_change_interval[int(buy_price)]*(self.min_gain['backwardation']+extra_gain_for_first_sell)*2000)
        volume_verify = buy_volume > self.min_required_volume_for_future and sell_volume > self.min_required_volume_for_stock and (abs(self.deal_qty[st] + self.order_qty[st] - self.amount_per_trading["stock"]) <= self.max_amount[st] or  abs(self.deal_qty[st] + self.order_qty[st] - self.amount_per_trading["stock"])<abs(self.deal_qty[st] + self.order_qty[st]))  # and -self.max_amount[st] <= (order_qty_by_code-self.amount_per_trading["stock"])#((self.deal_qty[st]-2)>=self.max_amount_for_first_sell[st] or ((self.deal_qty[st]-2) >= 0)) #-self.max_amount_st))
        print(
            f'{datetime.datetime.now()}: {contr_st.name} - {bid_st.bid_price[0]:.2f}/{bid_st.bid_volume[0]} - {bid_st.ask_price[0]:.2f}/{bid_st.ask_volume[0]}, {contr_fu.name} - {bid_fu.bid_price[0]:.2f}/{bid_fu.bid_volume[0]} - {bid_fu.ask_price[0]:.2f}/{bid_fu.ask_volume[0]}, F=>{forwardation_margin:.2f},B=>{backwardation_margin:.2f}',
            end = '\r')
        # f'，TB={self.price_change_interval[int(buy_price)] * (self.min_gain["backwardation"] + 1) * 2000:.2f}，PT={price_verify}, VT={volume_verify}   ',end = '\r')

        if volume_verify and price_verify and contr_st.code != "3260":
            print(
                f'\nbuy {contr_fu.name}@{buy_price:.2f}*{self.amount_per_trading["future"]} and sell {contr_st.name}@{sell_price:.2f}*{self.amount_per_trading["stock"]}, expected_margin = {expected_margin}')
            # self.order_list.append([(bid_st, "Sell"), (bid_fu, "Buy")])
            # self.order_qty[bid_st] -= 2
            if g_enable_trading:
                # trade_st = self.submit_a_stock_position(contr_st,self.amount_per_trading["stock"],bid_st.bid_price[0],sj.constant.Action.Sell)
                # if trade_st is not None:
                #     if trade_st.status.status in [sj.constant.Status.Filled]:
                #         with self.lock_for_sharing_data:
                #             self.order_qty[st] -= self.amount_per_trading["stock"]
                #             self.order_qty[fu] += self.amount_per_trading["future"]
                #         trade_fu = self.submit_a_future_position(contr_fu,self.amount_per_trading["future"],bid_fu.ask_price[0],sj.constant.Action.Buy)
                #         self.order_queue.put([(st, "Sell", sell_price, trade_st), (fu, "Buy", buy_price, trade_fu)])
                trades = []
                trades.append(self.trading_holder.prepare_a_trade("stock", st, bid_st.bid_price[0],
                                                                  self.amount_per_trading["stock"], action = "Sell",
                                                                  order_cond = "ShortSelling", order_type = "FOK"))
                trades.append(self.trading_holder.prepare_a_trade("future", fu, bid_fu.ask_price[0],
                                                                  self.amount_per_trading["future"], "Buy"))
                with self.lock_for_sharing_data:
                    token = self.trading_holder.submit_a_trade_sequence(trades, self.trading_callback)
                    if token != "":
                        self.order_qty[st] -= self.amount_per_trading["stock"]
                        self.order_qty[fu] += self.amount_per_trading["future"]
                        self.submitted_trade[token] = {"stock": -self.amount_per_trading["stock"],
                                                       "future": self.amount_per_trading["future"]}
                    print(
                        f'\ntoken = {token}:sell {contr_st.name} buy {contr_fu.name}, self.deal_qty[st]={self.deal_qty[st]}, self.order_qty[st]= {self.order_qty[st]}')

        self.adjust_min_gain()


class ConditionCheck(threading.Thread):
    def __init__(self, terminate_event: threading.Event, condition):
        # super(threading.Thread, self).__init__()
        threading.Thread.__init__(self)
        self.terminate_event = terminate_event
        self.condition = condition
        self.notify_event = threading.Event()

    def get_nontify_event(self):
        return self.notify_event

    def get_condition(self):
        return self.condition

    def run(self):
        try:
            while not self.terminate_event.isSet():
                self.notify_event.wait(5)
                if self.notify_event.isSet():
                    self.condition.check()
                    self.notify_event.clear()
            print(f"{self.__class__} is shut down")
            del self
        except:
            print('ConditionCheck is terminated cased by except')
            print("Unexpected error:", sys.exc_info()[0])
            raise


# class StopOrderExcecutor:
#     def __init__(self, api) -> None:
#         self.api = api.api
#         self.code_to_pair = api.code_to_pairs
#         self.max_order_qty = api.max_order
#         self.price_change_interval = api.price_change_interval
#         self.order_qty = {}
#         self.order_list = []
#
#         self._stop_orders = {}
#         self.submited_orders = {}
#         self.bought_futures = {}
#         self.biask_dict = {}
#         self.updated_bidask = queue.Queue(1024)
#         self.code_in_queue = {}
#         self.lock_check_code_in_queue = threading.Lock()
#         self.lock_update_bidask_dict = threading.Lock()
#         self.event_make_order = threading.Event()
#         self.event_verify_trading_status = threading.Event()
#         self.order_pairs = queue.Queue(1024)
#         self.trade_queue = queue.Queue(1024)
#         self.do_make_order = True
#         self.generate_data_thr = threading.Thread(
#             name='generator', target=self.generate_bidask_data)
#         self.generate_data_thr.start()
#         self.make_order_thr = threading.Thread(
#             name="submit_order", target=self.make_order)
#         self.make_order_thr.start()
#
#     def generate_bidask_data(self):
#         import random
#         import time
#         for _ in range(10):
#             f_or_s = random.randint(0, 1)
#             if f_or_s > 0:
#                 bid = sj.BidAskSTKv1(
#                     code='2330',
#                     datetime=datetime.datetime(2021, 7, 1, 9, 9, 54, 36828),
#                     bid_price=[Decimal('593'), Decimal('592'), Decimal(
#                         '591'), Decimal('590'), Decimal('589')],
#                     bid_volume=[248, 180, 258, 267, 163],
#                     diff_bid_vol=[3, 0, 0, 0, 0],
#                     ask_price=[Decimal('594'), Decimal('595'), Decimal(
#                         '596'), Decimal('597'), Decimal('598')],
#                     ask_volume=[1457, 531, 506, 90, 259],
#                     diff_ask_vol=[0, 0, 0, 0, 0],
#                     suspend=0,
#                     simtrade=0,
#                     intraday_odd=0
#                 )
#                 bid.code = '2303'
#                 bid.bid_price[0] = Decimal(random.randint(1, 20) + 54)
#                 bid.ask_price[0] = Decimal(
#                     random.randint(1, 5) + bid.bid_price[0])
#             else:
#                 bid = sj.BidAskFOPv1(
#                     code='TXFG1',
#                     datetime=datetime.datetime(2021, 7, 1, 10, 51, 31, 999000),
#                     bid_total_vol=66,
#                     ask_total_vol=101,
#                     bid_price=[Decimal('17746'), Decimal('17745'), Decimal(
#                         '17744'), Decimal('17743'), Decimal('17742')],
#                     bid_volume=[1, 14, 19, 17, 15],
#                     diff_bid_vol=[0, 1, 0, 0, 0],
#                     ask_price=[Decimal('17747'), Decimal('17748'), Decimal(
#                         '17749'), Decimal('17750'), Decimal('17751')],
#                     ask_volume=[6, 22, 25, 32, 16],
#                     diff_ask_vol=[0, 0, 0, 0, 0],
#                     first_derived_bid_price=Decimal('17743'),
#                     first_derived_ask_price=Decimal('17751'),
#                     first_derived_bid_vol=1,
#                     first_derived_ask_vol=1,
#                     underlying_price=Decimal('17827.94'),
#                     simtrade=0
#                 )
#                 bid.code = 'CCFH1'
#                 bid.bid_price[0] = Decimal(random.randint(1, 20) + 54)
#                 bid.ask_price[0] = Decimal(
#                     random.randint(1, 5) + bid.bid_price[0])
#             self.on_quote(bid)
#             time.sleep(0.1)
#         self.api.update_status()
#         # print(self.api.list_trades())
#
#     def check_and_submit_order(self, stock_bidask, future_bidask):
#         stocks = self.api.Contracts.Stocks
#         futures = self.api.Contracts.Futures
#         st = stock_bidask.code
#         fu = future_bidask.code
#
#         contr_st = stocks[st]
#         contr_fu = futures[fu]
#         # print(contr_st.name, contr_fu.name)
#         # bid_st, bid_fu = self.api.snapshots([contr_st, contr_fu])
#         bid_st = stock_bidask
#         bid_fu = future_bidask
#         buy_price = float(bid_st.ask_price[0])
#         sell_price = float(bid_fu.bid_price[0])
#         # print(f'({contr_st.name},{contr_fu.name}', end='\r')
#         # cost = bid_st.bid_price[0] * (0.001425*2*0.3*2000+0.003) +bid_fu.ask_price[0] * (2000*2/100000.0)*2+(45*2)
#         cost = buy_price * \
#             (0.001425*2*0.3+0.003)*2000 + \
#             sell_price * (2*2/100000.0)*2000+(45*2)
#         value_diff = (sell_price - buy_price)*2000
#         order_qty_by_code = self.order_qty.get(bid_st.code, 0)
#
#         if (value_diff-cost) > self.price_change_interval[int(buy_price)]*2*2000 and self.max_order_qty[bid_st.code] > order_qty_by_code:
#             print(
#                 f'buy {contr_st.name}@{buy_price} and sell {contr_fu.name}@{sell_price}, cost = {cost}, value_diff = {value_diff}, interval={self.price_change_interval[int(buy_price)]}')
#             # out_str = f'{datetime.now()},{contr_st.name},{buy_price},{contr_fu.name},{sell_price},{cost},{value_diff},{value_diff-cost},{self.price_change_interval[int(buy_price)]}\n'
#             # self.file_write_out.write(out_str)
#             # self.file_write_out.flush()
#             self.order_pairs.put([(bid_st, "Buy"), (bid_fu, "Sell")])
#             self.order_qty[bid_st.code] = order_qty_by_code + 2
#             order = self.api.Order(
#                 price=buy_price,
#                 quantity=2,
#                 action=sj.constant.Action.Buy,
#                 price_type=sj.constant.StockPriceType.LMT,
#                 order_type=sj.constant.TFTOrderType.FOK,
#                 order_lot=sj.constant.TFTStockOrderLot.Common,
#                 account=self.api.stock_account
#             )
#             trade_st = self.api.place_order(
#                 contr_st,
#                 order,
#                 timeout=0,
#                 cb=self.trade_callback  # only work in non-blocking mode
#             )
#             order = self.api.Order(
#                 action=sj.constant.Action.Sell,
#                 price=sell_price,
#                 quantity=1,
#                 price_type=sj.constant.StockPriceType.LMT,
#                 order_type=sj.constant.FuturesOrderType.FOK,
#                 octype=sj.constant.FuturesOCType.New,
#                 account=self.api.futopt_account
#             )
#             trade_fu = self.api.place_order(
#                 contr_fu,
#                 order,
#                 timeout=0,
#                 cb=self.trade_callback  # only work in non-blocking mode
#             )
#             self.trade_queue.put((trade_st, trade_fu))
#
#         buy_price = float(bid_fu.ask_price[0])
#         sell_price = float(bid_st.bid_price[0])
#         cost = sell_price * \
#             (0.001425*2*0.3+0.003)*2000 + \
#             buy_price * (2000*2/100000.0)*2+(45*2)
#         value_diff = (sell_price - buy_price)*2000
#
#         if (value_diff-cost) > self.price_change_interval[int(buy_price)]*2*2000 and order_qty_by_code >= 2:
#             print(
#                 f'buy {contr_fu.name}@{buy_price} and sell {contr_st.name}@{sell_price}, cost = {cost}, value_diff = {value_diff}, interval={self.price_change_interval[int(buy_price)]}')
#             self.order_list.append([(bid_st, "Sell"), (bid_fu, "Buy")])
#             # self.order_qty[bid_st] -= 2
#             self.order_qty[bid_st.code] = order_qty_by_code - 2
#             # out_str = f'{datetime.now()},{contr_fu.name},{buy_price},{contr_st.name},{sell_price},{cost},{value_diff},{value_diff-cost},{self.price_change_interval[int(buy_price)]}\n'
#             # self.file_write_out.write(out_str)
#             # self.file_write_out.flush()
#             order = self.api.Order(
#                 price=sell_price,
#                 quantity=2,
#                 action=sj.constant.Action.Sell,
#                 price_type=sj.constant.StockPriceType.LMT,
#                 order_type=sj.constant.TFTOrderType.FOK,
#                 order_lot=sj.constant.TFTStockOrderLot.Common,
#                 account=self.api.stock_account
#             )
#             trade_st = self.api.place_order(
#                 contr_st,
#                 order,
#                 timeout=0,
#                 cb=self.trade_callback  # only work in non-blocking mode
#             )
#             order = self.api.Order(
#                 action=sj.constant.Action.Buy,
#                 price=buy_price,
#                 quantity=1,
#                 price_type=sj.constant.StockPriceType.LMT,
#                 order_type=sj.constant.FuturesOrderType.FOK,
#                 octype=sj.constant.FuturesOCType.New,
#                 account=self.api.futopt_account
#             )
#             trade_fu = self.api.place_order(
#                 contr_fu,
#                 order,
#                 timeout=0,
#                 cb=self.trade_callback  # only work in non-blocking mode
#             )
#             self.trade_queue.put((trade_st, trade_fu))
#
#     def __exit__(self):
#         self.do_make_order = False
#         self.event_make_order.set()
#
#     def make_order(self):
#         while(self.do_make_order):
#             while not self.updated_bidask.empty():
#                 st_code, fu_code = self.updated_bidask.get()
#                 self.updated_bidask.task_done()
#                 with self.lock_update_bidask_dict:
#                     st_bidask = self.biask_dict.get(st_code, None)
#                     fu_bidask = self.biask_dict.get(fu_code, None)
#                 self.check_and_submit_order(st_bidask, fu_bidask)
#                 with self.lock_check_code_in_queue:
#                     self.code_in_queue[st_code] = False
#             self.event_make_order.wait()
#
#     def trade_callback(self, trade: sj.order.Trade):
#         # print('__my_callback__')
#         if trade.order.action == sj.constant.Action.Buy:
#             print(
#                 f'< trade >Buy {trade.contract.name}@{trade.order.price}-{trade.order.quantity}, Status:{trade.status.status}')
#         else:
#             print(
#                 f'< trade >Sell {trade.contract.name}@{trade.order.price}-{trade.order.quantity}, Status: {trade.status.status}')
#
#     def verify_trading_status(self):
#         while len(self.order_list):
#             order_pair = self.order_list.pop(0)
#             trade_list = []
#             for bid, trade_type in order_pair:
#                 if type(bid) == sj.BidAskSTKv1:
#                     contract = self.api.Contracts.Stocks[bid.code]
#                     if trade_type == "Buy":
#                         order = self.api.Order(
#                             price=bid.bid_price[0],
#                             quantity=2,
#                             action=sj.constant.Action.Buy,
#                             price_type=sj.constant.StockPriceType.LMT,
#                             order_type=sj.constant.TFTOrderType.FOK,
#                             order_lot=sj.constant.TFTStockOrderLot.Common,
#                             account=self.api.stock_account)
#                     else:  # Sell
#                         order = self.api.Order(
#                             price=bid.ask_price[0],
#                             quantity=2,
#                             action=sj.constant.Action.Sell,
#                             price_type=sj.constant.StockPriceType.LMT,
#                             order_type=sj.constant.TFTOrderType.FOK,
#                             order_lot=sj.constant.TFTStockOrderLot.Common,
#                             account=self.api.stock_account)
#                     trade = self.api.place_order(contract, order)
#                     trade_list.append(trade)
#                     if trade.status.status in (sj.constant.Status.Failed, sj.constant.Status.Cancelled):
#                         break
#                 elif type(bid) == sj.BidAskFOPv1:
#                     contract = self.api.Contracts.Futures[bid.code]
#                     if trade_type == "Buy":
#                         order = self.api.Order(
#                             action=sj.constant.Action.Buy,
#                             price=bid.bid_price[0],
#                             quantity=1,
#                             price_type=sj.constant.StockPriceType.LMT,
#                             order_type=sj.constant.FuturesOrderType.FOK,
#                             octype=sj.constant.FuturesOCType.New,
#                             account=self.api.futopt_account)
#                     else:
#                         order = self.api.Order(
#                             action=sj.constant.Action.Sell,
#                             price=bid.ask_bid[0],
#                             quantity=1,
#                             price_type=sj.constant.StockPriceType.LMT,
#                             order_type=sj.constant.FuturesOrderType.FOK,
#                             octype=sj.constant.FuturesOCType.New,
#                             account=self.api.futopt_account)
#                     trade = self.api.place_order(contract, order)
#                     trade_list.append(trade)
#                     if trade.status.status in (sj.constant.Status.Failed, sj.constant.Status.Cancelled):
#                         break
#                 else:
#                     raise NotImplemented
#         bDelete = False
#
#         for trade in trade_list:
#             # print(type(trade.contract))
#             if type(trade.contract) == type(sj.contracts.Future):
#                 self.api.update_status(self.api.futopt_account)
#                 if trade.status.status in (sj.constant.Status.Failed, sj.constant.Status.Cancelled):
#                     bDelete = True
#                     break
#         if bDelete:
#             for trade in trade_list:
#                 if type(trade.contract) == type(sj.contracts.Future):
#                     self.api.update_status(self.api.futopt_account)
#                 else:
#                     self.api.update_status(self.api.stock_account)
#                 if trade.status.status in (sj.constant.Status.Filled):
#                     contract = trade.contract
#                     order = trade.order
#                     if order.action == sj.constant.Action.Buy:
#                         order.action = sj.constant.Action.Sell
#                     else:
#                         order.action = sj.constant.Action.Buy
#                     order.price_type = sj.constant.StockPriceType.MKT
#                     order.order_type = sj.constant.FuturesOrderType.ROD
#                     new_trade = self.api.place_order(contract, order)
#                 elif trade.status.status in (sj.constant.Status.Filling):
#                     raise NotImplemented
#
#     def on_quote(self, quote: Union[sj.BidAskFOPv1, sj.BidAskSTKv1]) -> None:
#
#         code = quote.code
#         # print(f'{code} : {quote.bid_price[0]}, {quote.ask_price[0]}', end='\r')
#         with self.lock_update_bidask_dict:
#             self.biask_dict[code] = quote
#         if True:  # code in self._stop_orders:
#             if type(quote) is sj.BidAskSTKv1:
#                 st_code = code
#                 fu_code = self.code_to_pair[st_code]
#                 fu_bidask = self.biask_dict.get(fu_code, None)
#                 if fu_bidask is not None:
#                     with self.lock_check_code_in_queue:
#                         is_code_in_queue = self.code_in_queue.get(
#                             st_code, False)
#                         if not is_code_in_queue:
#                             self.code_in_queue[st_code] = True
#                             self.updated_bidask.put((st_code, fu_code))
#
#             elif type(quote) is sj.BidAskFOPv1:
#                 st_code = self.code_to_pair[code]
#                 fu_code = code
#                 st_bidask = self.biask_dict.get(st_code, None)
#                 if st_bidask is not None:
#                     with self.lock_check_code_in_queue:
#                         is_code_in_queue = self.code_in_queue.get(
#                             st_code, False)
#                         if not is_code_in_queue:
#                             self.code_in_queue[st_code] = True
#                             self.updated_bidask.put((st_code, fu_code))
#             self.event_make_order.set()
#             # for order in self._stop_orders[code]:
#             #     if hasattr(quote, 'ask_price'):
#             #         price = 0.5 * float(quote.bid_price[0] + quote.ask_price[0]) #BidAsk mid price
#             #     else:
#             #             raise NotImplemented
#
#             #     if (order['direction'] == 'up' and price >= order['stop_price']) or \
#             #             (order['direction'] == 'down' and price <= order['stop_price']):
#
#             #         self.api.place_order(order['contract'], order['order'])
#             #         executed_orders.append(order)
#             #         print(f"execute stop order: {order}")
#
#             # remove executed orders
#             # for order in executed_orders:
#             #     self._stop_orders[code].remove(order)
#             #     if len(self._stop_orders[code]) == 0:
#             #         _ = self._stop_orders.pop(code)
#
#     def add_stop_order(self, contract: sj.contracts.Contract, stop_price: float, order: sj.order.Order) -> None:
#         code = contract.code
#         snap = self.api.snapshots([contract])[0]
#         # use mid price as current price to avoid illiquidity
#         curr_price = 0.5*(snap.buy_price + snap.sell_price)
#         if curr_price > stop_price:
#             direction = 'down'
#         else:
#             direction = 'up'
#
#         stop_order = {
#             'code': contract.code,
#             'stop_price': stop_price,
#             'contract': contract,
#             'order': order,
#             'direction': direction,
#             'ts': time.time()
#         }
#
#         if code not in self._stop_orders:
#             self._stop_orders[code] = []
#         self._stop_orders[code].append(stop_order)
#         print(f"add stop order: {stop_order}")
#
#     def get_stop_orders(self) -> dict:
#         return self._stop_orders
#
#     def cancel_stop_order_by_code(self, code: str) -> None:
#         if code in self._stop_orders:
#             _ = self._stop_orders.pop(code)
#
#     def cancel_stop_order(self, stop_order: dict) -> None:
#         code = stop_order['code']
#         if code in self._stop_orders:
#             self._stop_orders[code].remove(stop_order)
#             if len(self._stop_orders[code]) == 0:
#                 _ = self._stop_orders.pop(code)
#
#     def cancel_all_stop_orders(self) -> None:
#         self._stop_orders = {}


class ShioajiAPI(APIBase):
    def __init__(self) -> None:
        super(ShioajiAPI).__init__()
        # real account
        # global g_simulation
        # if not g_simulation:
        #     _api = sj.Shioaji()
        #     _api = ThreadSafeShioajiAPI()
        #     _api.login(
        #         person_id="S122352189",
        #         passwd="baupas123",
        #         # contracts_timeout=100,
        #         contracts_cb=print,
        #         # contracts_cb=lambda security_type: print(f"{repr(security_type)} fetch done.")
        #     )
        #     _api.activate_ca(
        #         ca_path=r"C:\ekey\551\S122352189\S\Sinopac.pfx",
        #         ca_passwd="S122352189",
        #         person_id="S122352189",
        #     )
        # else:
        #     # simulation
        #     _api = ThreadSafeShioajiAPI(simulation=True)
        #     _api.login(
        #         person_id="PAPIUSER03",
        #         passwd="2222",
        #         contracts_cb=lambda security_type: print(
        #             f"{repr(security_type)} fetch done.")
        #     )
        #
        # self.api = _api
        global g_simulation
        self.api = ThreadSafeShioajiAPI(simulation = g_simulation)
        self.pairs = [('2603', 'CZF'), ('2303', 'CCF'), ('2618', 'HSF'), ('2002', 'CBF'), ('3006', 'IIF'),
                      ('3481', 'DQF'), ('2409', 'CHF'), ('2344', 'FZF'), ('2027', 'FEF'), ('5347', 'NLF'),
                      ('2401', 'GNF'), ('2317', 'DHF'), ('2606', 'QCF'), ('3260', 'NDF'),
                      ('2330', 'CDF'), ('6271', 'KDF'), ('3037', 'IRF'), ('3189', 'IXF'), ('2337', 'DIF'),
                      ('6116', 'OEF'), ('6182', 'PLF'), ('3034', 'IOF'), ('1605', 'CSF'), ('2449', 'GRF'),
                      ('2328', 'KWF'), ('5483', 'NOF'), ('6147', 'NSF'), ('2313', 'FTF')]
        # self.pairs = [('2603', 'CZF'), ('2303', 'CCF'), ('2618', 'HSF'), ('2002', 'CBF'), ('3006', 'IIF'),
        #               ('3481', 'DQF'), ('2409', 'CHF'), ('2344', 'FZF'), ('2027', 'FEF'), ('5347', 'NLF'),
        #               ('2401', 'GNF'), ('2317', 'DHF'), ('2606', 'QCF'), ('3260', 'NDF')]
        # self.pairs = [('2401', 'GNF')]
        # self.pairs = [('2409', 'CHF'),('3481', 'DQF'), ('2618', 'HSF')]
        interval = [(0, 10, 0.01), (10, 50, 0.05), (50, 100, 0.1),
                    (100, 500, 0.5), (500, 1000, 1), (1000, 5000, 5)]
        self.price_change_interval = {}
        for s, e, v in interval:
            for i in range(s, e):
                self.price_change_interval[i] = v
        self.make_dicts_of_pairs()
        # import time
        # time.sleep(10)
        # self.contracts = self.get_contracts(self.api)
        self.event_terminate_all: threading.Event = threading.Event()
        # self.soe = StopOrderExcecutor(self)
        # api.set_context(self.soe)
        # # In order to use context, set bind=True
        #
        # @api.on_bidask_fop_v1(bind=True)
        # def quote_callback(self, exchange: Exchange, bidask: BidAskFOPv1):
        #     # pass tick object to Stop Order Excecutor
        #     self.on_quote(bidask)
        #
        # @api.on_bidask_stk_v1(bind=True)
        # def quote_callback(self, exchange: Exchange, bidask: BidAskSTKv1):
        #     # pass tick object to Stop Order Excecutor
        #     self.on_quote(bidask)

        # self.order_monitor = OrderMonitoring()
        # self.map_code_to_contract = {}
        self.share_manager = ShareInHouse(self.api, self.event_terminate_all)
        self.share_manager.start()
        self.bidask_holder = BidAskHolder(self.api)
        # self.api.set_context(self.bidask_holder)
        #
        # # In order to use context, set bind=True
        #
        # @self.api.on_bidask_fop_v1(bind = True)
        # def quote_callback_fu(self, exchange: Exchange, bidask: BidAskFOPv1):
        #     # pass tick object to Stop Order Excecutor
        #     # print(f'{bidask.code} : {bidask.bid_price[0]} - {bidask.ask_price[0]}',end = '\r')
        #     self.on_quote(bidask)
        #
        # @self.api.on_bidask_stk_v1(bind = True)
        # def quote_callback_st(self, exchange: Exchange, bidask: BidAskSTKv1):
        #     # pass tick object to Stop Order Excecutor
        #     # print(f'{bidask.code} : {bidask.bid_price[0]} - {bidask.ask_price[0]}', end = '\r')
        #     self.on_quote(bidask)

        self.condition_checks = defaultdict(ConditionCheck)
        self.mytrading_center = MyTrading(self.api, self.share_manager, self.event_terminate_all)
        self.mytrading_center.start()
        self.register_quote_auto()
        # orders = []
        # orders.append(self.mytrading_center.prepare_a_trade("stock","2303",58,1,"Buy","LMT","IOC"))
        # orders.append(self.mytrading_center.prepare_a_trade("stock", "2303", 58, 1, "Sell", "LMT", "IOC"))
        # self.mytrading_center.submit_a_trade_sequence(orders, self.callback)
        # self.check_qty_in_house()
        # condition_check = self.create_condition_check_for_options()
        # self.condition_checks["Option"] = condition_check
        # balance = Decimal(self.api.account_balance()[0]['acc_balance'])
        # setlement_stock = self.api.list_settlements(self.api.stock_account)
        # balance += Decimal(setlement_stock[0]['t1_money'])
        # balance += Decimal(setlement_stock[0]['t2_money'])
        # setlement_future = self.api.list_settlements(self.api.futopt_account)
        # account_margin_stock = self.api.get_account_margin(account = self.api.stock_account).data()
        # account_margin_future = self.api.get_account_margin(account = self.api.futopt_account).data()
        # future_balance = Decimal(account_margin_future[0]['Flow'])
        # future_balance = Decimal(account_margin_future[0]['Cashamt'])
        # future_balance = Decimal(account_margin_future[0]['Equity'])
        # future_balance += Decimal(setlement_future[0]['t1_money'])
        # future_balance += Decimal(setlement_future[0]['t2_money'])

        self.start_trading()

        # api.set_order_callback(self.order_monitor.order_callback)
        # self.condition = ArbitrageCondition(self.api,self.bidask_holder,self.price_change_interval,self.max_order,self.api.Contracts.Stocks['2303'],self.api.Contracts.Futures['CCFH1'], self.event_terminate_all)
        # self.condition_check = ConditionCheck(self.event_terminate_all,self.condition)
        # self.condition_check.start()
        # self.bidask_holder.add_bidask(self.api.Contracts.Stocks['2303'], self.condition_check.get_nontify_event())
        # self.bidask_holder.add_bidask(self.api.Contracts.Futures['CCFH1'], self.condition_check.get_nontify_event())
        # self.file_write_out = open(
        #     date.today().strftime("%Y-%d-%m")+".csv", "at", encoding='UTF-8')

        # self.get_max_avaliable_quantity('2303')
        # self.sim_order()
        # accounts = api.login("YOUR_PERSON_ID", "YOUR_PASSWORD")

        # def quote_callback(topic: str, quote: dict):
        #     print(f"Topic: {topic}, Quote: {quote}")
        # api.quote.set_quote_callback(quote_callback)

        # @api.on_tick_stk_v1()
        # def quote_callback(exchange: Exchange, tick: TickSTKv1):
        #     print(f"Exchange: {exchange},\n Tick: {tick}")
        # api.quote.set_on_tick_stk_v1_callback(quote_callback)

        # @api.on_bidask_stk_v1()
        # def quote_callback(exchange: Exchange, bidask: BidAskSTKv1):
        #     # print(f"Exchange: {exchange},\nBidAsk: {bidask}", end='\r')
        #     global stock_quote
        #     global future_quote
        #     # print('---1---')
        #     stock_quote[bidask.code] = bidask
        #     future_code = self.code_dict_stock_to_future[bidask.code]
        #     future_bidask = future_quote.get(future_code, None)
        #     # print('---3---', future_bidask.code)
        #     if future_bidask is not None:
        #         self.strategy2(bidask, future_bidask)
        #         # print('---4---')

        # api.quote.set_on_bidask_stk_v1_callback(quote_callback)

        # @api.on_tick_fop_v1()
        # def quote_callback(exchange: Exchange, tick: TickFOPv1):
        #     print(f"Exchange: {exchange},\n Tick: {tick}")
        # api.quote.set_on_tick_fop_v1_callback(quote_callback)

        # @api.on_bidask_fop_v1()
        # def quote_callback(exchange: Exchange, bidask: BidAskFOPv1):
        #     # print(f"Exchange: {exchange},\n BidAsk: {bidask}")
        #     global future_quote
        #     global stock_quote
        #     future_quote[bidask.code] = bidask
        #     st_code = self.code_dict_future_to_stock[bidask.code]
        #     st_bidask = stock_quote.get(st_code, None)
        #     st_price = -1 if st_bidask is None else st_bidask.bid_price[0]
        #     print(bidask.code,
        #           bidask.bid_price[0], st_code, st_price, end='\r')
        # api.quote.set_on_bidask_fop_v1_callback(quote_callback)

        # self.api = api
        # self.get_quote(())
        # self.strategy1()
        # self.generate_bidask_data()
        global g_debug
        if g_debug:
            self.generate_data_thr = threading.Thread(
                name = 'generator', target = self.generate_bidask_data)
            self.generate_data_thr.start()

    def calc2(self):
        self.interval_map = {0: (9999999, 9999999)}
        for buy_price in range(10, 5000):
            for interval in range(1, 100):
                sell_price = buy_price + interval * self.price_change_interval[int(buy_price)]
                cost = (Decimal(buy_price * 2000) * Decimal((0.001425 * 0.3 + 0.003)) + \
                        Decimal(sell_price * 2000) * Decimal(0.00002) + Decimal(18)) * Decimal(2)
                value_diff = Decimal((sell_price - buy_price) * 2000)
                gain = (value_diff - cost)
                thr = 0 #self.price_change_interval[int(buy_price)] * 2000
                if gain > thr:
                    # print(
                    #     f"buy={buy_price}, sell={sell_price}, interval={interval}, gain={gain:.2f}, ratio = {gain / buy_price}")
                    self.interval_map[buy_price] = (sell_price, interval)
                    break
        return self.interval_map

    def calc(self):
        self.interval_map = {}
        for buy_price in range(10, 1000):
            for interval in [4]:
                sell_price = buy_price + interval * self.price_change_interval[int(buy_price)]
                cost = (Decimal(buy_price * 2000) * Decimal((0.001425 * 0.3 + 0.003)) + \
                        Decimal(sell_price * 2000) * Decimal(0.00002) + Decimal(18)) * Decimal(2)
                value_diff = Decimal((sell_price - buy_price) * 2000)
                gain = (value_diff - cost)
                thr = self.price_change_interval[int(buy_price)] * 2000
                print(
                    f"buy={buy_price}, sell={sell_price}, interval={interval}, gain={gain:.2f}, ratio = {gain / buy_price}")
                self.interval_map[buy_price] = (sell_price, interval)

    def create_condition_check_for_options(self) -> ConditionCheck:
        option_contracts = self.get_option_contracts()
        condition = OptionLargeInterval(self.api, option_contracts, self.bidask_holder, self.mytrading_center,
                                           self.event_terminate_all,datetime.date.today())
        # condition = OptionNeturalCondition(self.api, option_contracts, self.bidask_holder, self.mytrading_center,
        #                                    self.event_terminate_all)
        condition_check = ConditionCheck(self.event_terminate_all, condition)
        event = condition_check.get_nontify_event()
        for contr in condition.get_contracts():
            self.bidask_holder.add_bidask(contr, event)
        return condition_check

    def callback(self, token: str, result: str):
        print(f'token = {token}, result = {result}')

    def get_option_contracts(self) -> List[sj.contracts.Contract]:
        week_key = 'TXO'

        all_keys_in_options = [k for k in self.api.Contracts.Options.keys()]
        for i in range(5, 0, -1):
            key = f'TX{i}'
            if key in all_keys_in_options:
                week_key = key
                break
        ret_list = [k for k in self.api.Contracts.Options[week_key]]
        return ret_list

    def generate_bidask_data(self):
        import random
        import time
        times = 10
        while not self.event_terminate_all.isSet() and times > 0:
            times -= 1
            bid_st = sj.BidAskSTKv1(
                code = '2330',
                datetime = datetime.datetime(2021, 7, 1, 9, 9, 54, 36828),
                bid_price = [Decimal('593'), Decimal('592'), Decimal(
                    '591'), Decimal('590'), Decimal('589')],
                bid_volume = [248, 180, 258, 267, 163],
                diff_bid_vol = [3, 0, 0, 0, 0],
                ask_price = [Decimal('594'), Decimal('595'), Decimal(
                    '596'), Decimal('597'), Decimal('598')],
                ask_volume = [1457, 531, 506, 90, 259],
                diff_ask_vol = [0, 0, 0, 0, 0],
                suspend = 0,
                simtrade = 0,
                intraday_odd = 0
            )
            stock_id = '2401'
            bid_st.code = stock_id
            bid_st.bid_price[0] = Decimal(random.random() * (
                        self.api.Contracts.Stocks[stock_id].limit_up - self.api.Contracts.Stocks[stock_id].limit_down) +
                                          self.api.Contracts.Stocks[stock_id].limit_down)
            bid_st.ask_price[0] = bid_st.bid_price[0] + Decimal(random.random()) * 2
            # bid_st.bid_price[0] = Decimal(54)
            # bid_st.ask_price[0] = Decimal(56)
            bid_fu = sj.BidAskFOPv1(
                code = 'TXFG1',
                datetime = datetime.datetime(2021, 7, 1, 10, 51, 31, 999000),
                bid_total_vol = 66,
                ask_total_vol = 101,
                bid_price = [Decimal('17746'), Decimal('17745'), Decimal(
                    '17744'), Decimal('17743'), Decimal('17742')],
                bid_volume = [90, 14, 19, 17, 15],
                diff_bid_vol = [0, 1, 0, 0, 0],
                ask_price = [Decimal('17747'), Decimal('17748'), Decimal(
                    '17749'), Decimal('17750'), Decimal('17751')],
                ask_volume = [96, 22, 25, 32, 16],
                diff_ask_vol = [0, 0, 0, 0, 0],
                first_derived_bid_price = Decimal('17743'),
                first_derived_ask_price = Decimal('17751'),
                first_derived_bid_vol = 1,
                first_derived_ask_vol = 1,
                underlying_price = Decimal('17827.94'),
                simtrade = 0
            )
            fu_id = 'GNFI1'
            bid_fu.code = fu_id
            bid_fu.bid_price[0] = Decimal(random.random() * (
                        self.api.Contracts.Futures[fu_id].limit_up - self.api.Contracts.Futures[fu_id].limit_down) +
                                          self.api.Contracts.Futures[fu_id].limit_down)
            bid_fu.ask_price[0] = bid_fu.bid_price[0] + Decimal(random.random()) * 2
            # bid_fu.bid_price[0] = Decimal(60)
            # bid_fu.ask_price[0] = Decimal(62)
            # self.check_and_submit_order_for_open_a_position(bid_st,bid_fu)
            self.bidask_holder.on_quote(bid_st)
            self.bidask_holder.on_quote(bid_fu)
            time.sleep(1)
        # self.api.update_status()
        # print(self.api.list_trades())

    def get_contracts(self, in_api: sj.Shioaji):

        contracts = {
            code: contract
            for name, iter_contract in in_api.Contracts
            for code, contract in iter_contract._code2contract.items()
        }
        return contracts

    def make_dicts_of_pairs(self):
        self.code_dict_stock_to_future = {}
        self.code_dict_future_to_stock = {}
        self.code_to_pairs = {}
        self.max_order = {}
        month_map = {1: 'A', 2: 'B', 3: 'C', 4: 'D', 5: 'E', 6: 'F', 7: 'G', 8: 'H', 9: 'I', 10: 'J', 11: 'K', 12: 'L'}
        today = datetime.datetime.today()
        month = today.month
        weekday = today.isoweekday()
        week_index = math.ceil((today.day - weekday) / 7.0)
        if week_index > 3 or (week_index == 3 and weekday >= 3):
            month += 1
        for s, f in self.pairs:
            f = f + f'{month_map[month]}1'
            self.code_dict_future_to_stock[f] = s
            self.code_dict_stock_to_future[s] = f
            self.code_to_pairs[s] = f
            self.code_to_pairs[f] = s
            self.max_order[s] = 2
            self.max_order[f] = 1

    def check_qty_in_house(self):
        fu_positions_in_house = self.api.get_account_openposition(query_type = '1',
                                                                  account = self.api.futopt_account).data()
        st_positions_in_house = self.api.list_positions(self.api.stock_account)
        # fu_positions_in_house2 = self.api.list_positions(self.api.futopt_account)

        for st_posi in st_positions_in_house:
            if len(st_posi) > 0:
                st_qty = st_posi.quantity
                st_code = st_posi.code
                st_dir = st_posi.direction
                if st_code in self.condition_checks.keys():
                    condi: ConditionBase = self.condition_checks[st_code].condition
                    sign = 1 if st_dir == sj.constant.Action.Buy else -1
                    condi.add_qty_in_house(st_code, sign * st_qty)
                    # codes = condi.get_codes()
                    print(f'found ({st_code} - {st_dir} - {st_qty}) in house')
        for posi in fu_positions_in_house:
            if len(posi.keys()) > 0:
                fu_code = posi['Code']
                fu_qty = int(Decimal(posi['Volume']))
                fu_dir = sj.constant.Action.Buy if posi['OrderBS'] == 'B' else sj.constant.Action.Sell
                if fu_code in self.condition_checks.keys():
                    condi: ConditionBase = self.condition_checks[fu_code].get_condition()
                    sign = 1 if fu_dir == sj.constant.Action.Buy else -1
                    condi.add_qty_in_house(fu_code, sign * fu_qty)
                    # codes = condi.get_codes()
                    print(f'found ({fu_code} - {fu_dir} - {fu_qty}) in house')
                    # for st_posi in st_positions_in_house:
                    #     if st_posi.code in codes:
                    #         st_qty = st_posi.quantity
                    #         sign = 1 if st_posi.direction == sj.constant.Action.Buy else -1
                    #         print(f'found ({st_posi.code} - {st_posi.direction} - {st_qty}) in house, pair with {fu_code}')
                    #         if st_qty>= fu_qty*2 and fu_dir!=st_posi.direction:
                    #             condi.add_qty_in_house(st_posi.code, sign * fu_qty * 2)
                    #         elif fu_dir!=st_posi.direction:
                    #             condi.add_qty_in_house(st_posi.code, sign * st_qty)
        # self.condition_checks["2303"].condition.set_qty_for_first_sell("2303", 2)
        # for st_posi in st_positions_in_house:
        #     st_qty = st_posi.quantity
        #     st_code = st_posi.code
        #     if st_code in self.condition_checks.keys():
        #         condi: ConditionBase = self.condition_checks[st_code].condition
        #         codes = condi.get_codes()
        #         fu_qty = 0
        #         fu_dir = sj.constant.Action.Buy if st_posi.direction == sj.constant.Action.Sell else sj.constant.Action.Sell
        #         for posi in fu_positions_in_house:
        #             if len(posi.keys())>0:
        #                 fu_code = posi['Code']
        #                 fu_qty = int(Decimal(posi['Volume']))
        #                 fu_dir = sj.constant.Action.Buy if posi['OrderBS'] == 'B' else sj.constant.Action.Sell
        #                 if fu_code in codes and st_qty >= fu_qty * 2 and fu_dir != st_posi.direction:
        #                     st_qty -= fu_qty * 2
        #         condi.set_qty_for_first_sell(st_posi.code, st_qty)

    def start_trading(self):
        has_done = []
        for k in self.condition_checks.keys():
            if self.condition_checks[k] not in has_done:
                self.condition_checks[k].start()
                has_done.append(self.condition_checks[k])

    def register_quote(self):
        month_map = {1: 'A', 2: 'B', 3: 'C', 4: 'D', 5: 'E', 6: 'F', 7: 'G', 8: 'H', 9: 'I', 10: 'J', 11: 'K', 12: 'L'}
        day_to_change_contract_indexed_by_weekday = {1: 17, 2: 16, 3: 15, 4: 21, 5: 20, 6: 19, 7: 18}
        today = datetime.datetime.today()
        month = today.month
        weekday = datetime.date(year = today.year, month = today.month, day = 1).isoweekday()
        change_contract_day_per_month = day_to_change_contract_indexed_by_weekday[weekday]
        ret_dict = self.calc2()
        if today.day >= change_contract_day_per_month:
            month += 1
        for s, t in self.pairs:
            contract_s = self.api.Contracts.Stocks[s]
            # self.api.quote.subscribe(
            #     contract_s, quote_type=sj.constant.QuoteType.BidAsk, version='v1')
            contract_t = self.api.Contracts.Futures[t + f'{month_map[month]}1']
            # self.api.quote.subscribe(
            #     contract_t, quote_type=sj.constant.QuoteType.BidAsk, version='v1')
            condition = ArbitrageCondition(self.api, self.bidask_holder, self.share_manager, self.mytrading_center,
                                           self.price_change_interval, ret_dict,  # self.order_monitor,
                                           self.max_order, contract_s, contract_t, self.event_terminate_all)
            condition_check = ConditionCheck(self.event_terminate_all, condition)
            # condition_check.start()
            self.bidask_holder.add_bidask(contract_s, condition_check.get_nontify_event())
            self.bidask_holder.add_bidask(contract_t, condition_check.get_nontify_event())
            self.condition_checks[contract_s.code] = condition_check
            self.condition_checks[contract_t.code] = condition_check
            # self.map_code_to_contract[s] = contract_s
            # self.map_code_to_contract[t] = contract_t

        for s, t in self.pairs:
            contract_s = self.api.Contracts.Stocks[s]
            # self.api.quote.subscribe(
            #     contract_s, quote_type=sj.constant.QuoteType.BidAsk, version='v1')
            contract_t = self.api.Contracts.Futures[t + f'{month_map[month]}1']
            # self.api.quote.subscribe(
            #     contract_t, quote_type=sj.constant.QuoteType.BidAsk, version='v1')
            condition = ArbitrageCondition(self.api, self.bidask_holder, self.share_manager, self.mytrading_center,
                                           self.price_change_interval, ret_dict,  # self.order_monitor,
                                           self.max_order, contract_s, contract_t, self.event_terminate_all)
            condition_check = ConditionCheck(self.event_terminate_all, condition)
            # condition_check.start()
            self.bidask_holder.add_bidask(contract_s, condition_check.get_nontify_event())
            self.bidask_holder.add_bidask(contract_t, condition_check.get_nontify_event())
            self.condition_checks[contract_s.code] = condition_check
            self.condition_checks[contract_t.code] = condition_check
            # self.map_code_to_contract[s] = contract_s
            # self.map_code_to_contract[t] = contract_t

    def register_quote_auto(self):
        today = datetime.datetime.today()
        xtai = ecals.get_calendar("XTAI")
        session_days = 0
        one_week_ago = today
        while session_days<3:
            one_week_ago -= datetime.timedelta(days = 1)
            if xtai.is_session(pd.Timestamp(one_week_ago.date().isoformat(),tz='UTC')):
                session_days +=1

        month = today.month
        next_month = (today.month+1)%12
        ret_dict = self.calc2()
        care_delivery_months=[f'{today.year}{month:02d}', f'{today.year if next_month>month else today.year+1}{next_month:02d}']
        future_codes = [k for k in self.api.Contracts.Futures._code2contract]
        candidate_futures = [self.api.Contracts.Futures[code] for code in future_codes if self.api.Contracts.Futures[code].underlying_kind=='S' and self.api.Contracts.Futures[code].delivery_month in care_delivery_months]
        for fut in candidate_futures:
            # self.api.quote.subscribe(
            #     contract_s, quote_type=sj.constant.QuoteType.BidAsk, version='v1')
            contract_t = fut
            kbars = self.api.kbars(fut, start=one_week_ago.strftime("%Y-%m-%d"), end = today.strftime("%Y-%m-%d"))
            try:
                mean = np.mean(kbars['Volume'])
                sum = np.sum(kbars['Volume'])
                price = np.mean(kbars['Close'])
            except:
                continue
            if sum<session_days*1000 or mean<2 or price>300:
                continue
            contract_s = self.api.Contracts.Stocks[fut.underlying_code]

            # self.api.quote.subscribe(
            #     contract_t, quote_type=sj.constant.QuoteType.BidAsk, version='v1')
            self.max_order[contract_s.code]=2
            self.max_order[contract_t.code]=1
            print(f'{contract_s.name} : sum = {sum}, mean = {mean}')
            condition = ArbitrageCondition(self.api, self.bidask_holder, self.share_manager, self.mytrading_center,
                                           self.price_change_interval, ret_dict,  # self.order_monitor,
                                           self.max_order, contract_s, contract_t, self.event_terminate_all)
            condition_check = ConditionCheck(self.event_terminate_all, condition)
            # condition_check.start()
            self.bidask_holder.add_bidask(contract_s, condition_check.get_nontify_event())
            self.bidask_holder.add_bidask(contract_t, condition_check.get_nontify_event())
            self.condition_checks[contract_s.code] = condition_check
            self.condition_checks[contract_t.code] = condition_check
            # self.map_code_to_contract[s] = contract_s
            # self.map_code_to_contract[t] = contract_t

    def stop(self):
        self.event_terminate_all.set()
        self.mytrading_center.join()
        self.share_manager.join()
        for cond in self.condition_checks.values():
            cond.join()
        self.condition_checks.clear()
        print("ShioajiAPI::exit()")

    # def strategy2(self, stock_bidask, future_bidask):
    #     stocks = self.api.Contracts.Stocks
    #     futures = self.api.Contracts.Futures
    #     st = stock_bidask.code
    #     fu = future_bidask.code
    #
    #     contr_st = stocks[st]
    #     contr_fu = futures[fu]
    #     # print(contr_st.name, contr_fu.name)
    #     # bid_st, bid_fu = self.api.snapshots([contr_st, contr_fu])
    #     bid_st = stock_bidask
    #     bid_fu = future_bidask
    #     buy_price = float(bid_st.bid_price[0])
    #     sell_price = float(bid_fu.ask_price[0])
    #     # print(f'({contr_st.name},{contr_fu.name}', end='\r')
    #     # cost = bid_st.bid_price[0] * (0.001425*2*0.3*2000+0.003) +bid_fu.ask_price[0] * (2000*2/100000.0)*2+(45*2)
    #     cost = buy_price * \
    #         (0.001425*2*0.3*2000+0.003) + \
    #         sell_price * (2000*2/100000.0)*2+(45*2)
    #     value_diff = (sell_price - buy_price)*2000
    #     if (value_diff-cost) > self.price_change_interval[int(buy_price)]*2*2000:
    #         print(
    #             f'buy {contr_st.name}@{buy_price} and sell {contr_fu.name}@{sell_price}, cost = {cost}, value_diff = {value_diff}, interval={self.price_change_interval[int(buy_price)]}')
    #         out_str = f'{datetime.datetime.now()},{contr_st.name},{buy_price},{contr_fu.name},{sell_price},{cost},{value_diff},{value_diff-cost},{self.price_change_interval[int(buy_price)]}\n'
    #         self.file_write_out.write(out_str)
    #         self.file_write_out.flush()
    #
    #     buy_price = float(bid_fu.bid_price[0])
    #     sell_price = float(bid_st.ask_price[0])
    #     cost = sell_price * \
    #         (0.001425*2*0.3*2000+0.003) + \
    #         buy_price * (2000*2/100000.0)*2+(45*2)
    #     value_diff = (sell_price - buy_price)*2000
    #     if (value_diff-cost) > self.price_change_interval[int(buy_price)]*2*2000 and self.get_max_avaliable_quantity(st) > -1:
    #         print(
    #             f'buy {contr_fu.name}@{buy_price} and sell {contr_st.name}@{sell_price}, cost = {cost}, value_diff = {value_diff}, interval={self.price_change_interval[int(buy_price)]}')
    #         out_str = f'{datetime.datetime.now()},{contr_fu.name},{buy_price},{contr_st.name},{sell_price},{cost},{value_diff},{value_diff-cost},{self.price_change_interval[int(buy_price)]}\n'
    #         self.file_write_out.write(out_str)
    #         self.file_write_out.flush()
    #
    # def strategy1(self):
    #     stocks = self.api.Contracts.Stocks
    #     futures = self.api.Contracts.Futures
    #     pairs = [('2603', 'CZF'), ('2303', 'CCF'), ('2618', 'HSF'), ('2002', 'CBF'), ('3006', 'IIF'), ('3481', 'DQF'), ('2409', 'CHF'), ('2344', 'FZF'), ('2027', 'FEF'), ('5347', 'NLF'), ('2401', 'GNF'), ('2317', 'DHF'), ('2606', 'QCF'), ('3260', 'NDF'),
    #              ('2330', 'CDF'), ('6271', 'KDF'), ('3037', 'IRF'), ('3189', 'IXF'), ('2337', 'DIF'), ('6116', 'OEF'), ('6182', 'PLF'), ('3034', 'IOF'), ('1605', 'CSF'), ('2449', 'GRF'), ('2328', 'KWF'), ('5483', 'NOF'), ('6147', 'NSF'), ('2313', 'FTF')]
    #     interval = [(0, 10, 0.01), (10, 50, 0.05), (50, 100, 0.1),
    #                 (100, 500, 0.5), (500, 1000, 1), (1000, 5000, 5)]
    #
    #     price_change_interval = {}
    #     for s, e, v in interval:
    #         for i in range(s, e):
    #             price_change_interval[i] = v
    #     # target_pairs=[(stocks[], futures['CZF'])]
    #     for st, fu in pairs:
    #
    #         contr_st = stocks[st]
    #         contr_fu = futures[fu+'H1']
    #         # print(contr_st.name, contr_fu.name)
    #         bid_st, bid_fu = self.api.snapshots([contr_st, contr_fu])
    #         # cost = bid_st.bid_price[0] * (0.001425*2*0.3*2000+0.003) +bid_fu.ask_price[0] * (2000*2/100000.0)*2+(45*2)
    #         cost = bid_st.buy_price * \
    #             (0.001425*2*0.3*2000+0.003) + \
    #             bid_fu.sell_price * (2000*2/100000.0)*2+(45*2)
    #         value_diff = (bid_fu.sell_price - bid_st.buy_price)*2000
    #         if (value_diff-cost) > price_change_interval[int(bid_st.buy_price)]*3*2000:
    #             print(
    #                 f'buy {contr_st.name}@{bid_st.buy_price} and sell {contr_fu.name}@{bid_fu.sell_price}, cost = {cost}, value_diff = {value_diff}, interval={price_change_interval[int(bid_st.buy_price)]}')
    #         cost = bid_st.sell_price * \
    #             (0.001425*2*0.3*2000+0.003) + \
    #             bid_fu.buy_price * (2000*2/100000.0)*2+(45*2)
    #         value_diff = (bid_st.sell_price - bid_fu.buy_price)*2000
    #         if (value_diff-cost) > price_change_interval[int(bid_st.buy_price)]*3*2000 and self.get_max_avaliable_quantity(st) > 0:
    #             print(
    #                 f'buy {contr_fu.name}@{bid_fu.buy_price} and sell {contr_st.name}@{bid_st.sell_price}, cost = {cost}, value_diff = {value_diff}, interval={price_change_interval[int(bid_st.buy_price)]}')

    def get_quote(self, info: Tuple[str, Callable]) -> int:
        # self.api.quote.subscribe(
        #     self.api.Contracts.Options.TXO.TXO202108018000C, quote_type='tick', intraday_odd=True, version='v1')
        # self.api.quote.subscribe(
        #     self.api.Contracts.Options.TX2.TX2202108018000C, quote_type='tick', intraday_odd=True, version='v1')

        # self.api.quote.subscribe(
        #     self.api.Contracts.Stocks["2330"],
        #     quote_type=sj.constant.QuoteType.Tick,  # or 'tick'
        #     version=sj.constant.QuoteVersion.v1  # or 'v1'
        # )
        # self.api.quote.subscribe(
        #     # self.api.Contracts.Options.TXO.TXO202108018000C,
        #     self.api.Contracts.Futures.TXF['TXF202108'],
        #     quote_type=sj.constant.QuoteType.Tick,  # or 'tick'
        #     version=sj.constant.QuoteVersion.v1  # or 'v1'
        # )
        # self.api.quote.subscribe(
        #     # self.api.Contracts.Options.TXO.TXO202108017000C,
        #     self.api.Contracts.Futures.TXF['TXF202108'],
        #     quote_type=sj.constant.QuoteType.Tick,  # or 'tick'
        #     version=sj.constant.QuoteVersion.v1,  # or 'v1'
        #     intraday_odd=True
        # )
        contract_0050 = self.api.Contracts.Stocks["0050"]
        ticks = self.api.ticks(contract_0050, "2021-08-10")
        tick_data_df = pd.DataFrame({**ticks})
        tick_data_df.ts = pd.to_datetime(tick_data_df.ts)
        print(tick_data_df.head())

        self.api.quote.subscribe(
            contract_0050, quote_type = sj.constant.QuoteType.BidAsk, version = 'v1')

        contracts = [contract_0050,
                     self.api.Contracts.Options['TX218000H1']]
        #  self.api.Contracts.Options.TX2.TX2202108018000C]
        snapshots = self.api.snapshots(contracts)
        print(snapshots)
        print('-----------')
        contract = self.api.Contracts.Stocks.TSE.TSE2330
        order = self.api.Order(price = 400,
                               quantity = 1,
                               action = "Buy",
                               price_type = "LMT",
                               order_type = "ROD",
                               order_lot = "Common",
                               account = self.api.stock_account
                               )
        trade = self.api.place_order(contract, order)
        # print(trade)

        self.api.update_status(self.api.stock_account)
        self.api.update_order(trade = trade, price = 410)
        print('------2--------')
        print(trade)
        self.api.update_order(trade = trade, qty = 3)
        self.api.update_status(self.api.stock_account)
        print('------3--------')
        print(trade)
        print('------4--------')
        self.api.cancel_order(trade)
        print(trade)
        self.api.update_status(self.api.stock_account)
        print('------5--------')
        # self.api.quote.unsubscribe(
        #     contract_0050, quote_type=sj.constant.QuoteType.BidAsk, version='v1')
        self.api.list_positions(self.api.stock_account)
        return 1

    def sim_order(self):
        # stock order
        order = self.api.Order(
            price = 12,
            quantity = 1,
            action = sj.constant.Action.Buy,
            price_type = sj.constant.StockPriceType.LMT,
            order_type = sj.constant.TFTOrderType.ROD,
            order_lot = sj.constant.TFTStockOrderLot.Common,
            account = self.api.stock_account
        )

        # futures order
        order = self.api.Order(
            action = sj.constant.Action.Buy,
            price = 10200,
            quantity = 2,
            price_type = sj.constant.StockPriceType.LMT,
            order_type = sj.constant.FuturesOrderType.ROD,
            octype = sj.constant.FuturesOCType.Auto,
            account = self.api.futopt_account
        )

        contract = self.api.Contracts.Futures.TXF.TXF202108
        order = self.api.Order(action = sj.constant.Action.Buy,
                               price = 17500,
                               quantity = 2,
                               price_type = sj.constant.StockPriceType.LMT,
                               order_type = sj.constant.FuturesOrderType.ROD,
                               octype = sj.constant.FuturesOCType.Auto,
                               account = self.api.futopt_account)

        trade = self.api.place_order(contract, order)
        """
        PendingSubmit: 傳送中
        PreSubmitted: 預約單
        Submitted: 傳送成功
        Failed: 失敗
        Cancelled: 已刪除
        Filled: 完全成交
        Filling: 部分成交
        """
        print(trade)

        # cancel order
        # self.api.update_status(self.api.futopt_account)
        # self.api.cancel_order(trade)
        # self.api.update_status(self.api.futopt_account)
        # print(trade)

        # modify order
        # self.api.update_status(self.api.futopt_account)
        # self.api.update_order(trade=trade, price=18080, qty=1)
        # self.api.update_status(self.api.futopt_account)
        # print(trade)


if __name__ == "__main__":
    import time

    api = ShioajiAPI()
    time.sleep(100)
