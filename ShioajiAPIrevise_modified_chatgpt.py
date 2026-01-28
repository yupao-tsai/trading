# -*- coding: utf-8 -*-
"""
Refactored full version (safe + bugfixed) for your Shioaji multi-thread trading framework.

Key fixes:
1) Remove hard-coded credentials; use env vars.
2) Fix ThreadSafeList.copy bug.
3) Fix MyTrading.check_status recursion bug.
4) Fix mapping of status_id / trade_id in callbacks; make robust.
5) Fix duplicated account_balance method definition.
6) Improve thread shutdown logic and reduce race conditions.
7) Make pending/partfilled processing more consistent.

Env vars (REQUIRED for real trading):
- SHIOAJI_PERSON_ID
- SHIOAJI_PASSWORD
- SHIOAJI_CA_PATH
- SHIOAJI_CA_PASSWORD
Optional:
- SHIOAJI_SIMULATION=1  (force simulation login)
"""

import os
import sys
import time
import math
import queue
import abc
import secrets
import threading
import datetime
from decimal import Decimal
from typing import List, Dict, Tuple, Callable, Union
from collections import defaultdict, Counter

import numpy as np
from scipy import stats

import shioaji as sj
from shioaji import BidAskSTKv1, BidAskFOPv1, Exchange
from dotenv import load_dotenv
load_dotenv()
# ----------------------------
# Globals / switches
# ----------------------------
g_debug: bool = False
g_simulation: bool = False
g_record_only: bool = False
g_record: bool = True
g_enable_trading: bool = True


# ----------------------------
# Helpers
# ----------------------------
def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name, "")
    if v == "":
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def _now() -> datetime.datetime:
    return datetime.datetime.now()


# ----------------------------
# ShareInHouse: positions + risk indicator
# ----------------------------
class ShareInHouse(threading.Thread):
    def __init__(self, in_api: sj.Shioaji, event_to_terminate: threading.Event):
        super().__init__(daemon=True)
        self.api = in_api
        self.event_to_terminate = event_to_terminate

        self.share_info = {"stock": {}, "future": {}, "option": {}, "balance": None}
        self.lock_data = threading.Lock()
        self.event_to_update_data = threading.Event()

        # risk indicators
        self.stock_risk_indicator = Decimal(0)
        self.future_risk_indicator = Decimal(0)
        self.stock_balance = Decimal(0)
        self.stock_positions = []

        self.get_shares_in_house()
        self.calc_risk_indicators()

    def run(self) -> None:
        while not self.event_to_terminate.is_set():
            self.event_to_update_data.wait(5)
            if self.event_to_update_data.is_set():
                try:
                    self.get_shares_in_house()
                    self.calc_risk_indicators()
                except Exception as e:
                    print(f"[ShareInHouse] update error: {e}", file=sys.stderr)
                finally:
                    self.event_to_update_data.clear()
        print("[ShareInHouse] shut down")

    def update(self):
        self.event_to_update_data.set()

    def get_stock_info_in_house(self, code):
        with self.lock_data:
            return self.share_info["stock"].get(code)

    def get_future_info_in_house(self, code):
        with self.lock_data:
            return self.share_info["future"].get(code)

    def get_option_info_in_house(self, code):
        with self.lock_data:
            return self.share_info["option"].get(code)

    def is_available_capital(self) -> bool:
        # your original rule
        return self.stock_risk_indicator > 500 and self.future_risk_indicator > 800

    def get_shares_in_house(self):
        # Futures
        future_positions = self.api.get_account_openposition(
            product_type="1", query_type="1", account=self.api.futopt_account
        )
        future_dict: Dict[str, List[Dict]] = defaultdict(list)
        for position in future_positions.data():
            if not position:
                continue
            code = position["Code"]
            future_dict[code].append(
                {
                    "code": code,
                    "action": "Buy" if position["OrderBS"] == "B" else "Sell",
                    "qty": int(Decimal(position["Volume"])),
                    "price": Decimal(position["ContractAverPrice"]),
                }
            )
        with self.lock_data:
            self.share_info["future"] = dict(future_dict)

        # Options (filter TX*)
        option_positions = self.api.get_account_openposition(
            product_type="0", query_type="1", account=self.api.futopt_account
        )
        option_dict: Dict[str, List[Dict]] = defaultdict(list)
        for position in [s for s in option_positions.data() if s and "TX" in s.get("Code", "")]:
            code = position["Code"]
            option_dict[code].append(
                {
                    "code": code,
                    "action": "Buy" if position["OrderBS"] == "B" else "Sell",
                    "qty": int(Decimal(position["Volume"])),
                    "price": Decimal(position["ContractAverPrice"]),
                }
            )
        with self.lock_data:
            self.share_info["option"] = dict(option_dict)

        # Stocks
        stock_positions = self.api.list_positions(self.api.stock_account)
        stock_dict: Dict[str, List[Dict]] = defaultdict(list)
        for position in stock_positions:
            if not position:
                continue
            code = position["code"]
            stock_dict[code].append(
                {
                    "code": code,
                    "action": position["direction"],
                    "qty": int(Decimal(position["quantity"])),
                    "price": Decimal(position["price"]),
                }
            )
        with self.lock_data:
            self.share_info["stock"] = dict(stock_dict)

    def calc_risk_indicators(self):
        # Stock balance
        bal = Decimal(self.api.account_balance()[0]["acc_balance"])
        settlements = self.api.list_settlements(self.api.stock_account)
        if settlements:
            bal += Decimal(settlements[0].get("t1_money", 0))
            bal += Decimal(settlements[0].get("t2_money", 0))
        self.stock_balance = bal

        # Stock positions total exposure
        self.stock_positions = self.api.list_positions(self.api.stock_account)
        total_value = Decimal(0)
        stocks_net = {}
        for pos in self.stock_positions:
            qty = Decimal(pos["quantity"])
            price = Decimal(pos["price"])
            sign = Decimal(1) if pos["direction"] == sj.constant.Action.Buy else Decimal(-1)
            stocks_net[pos["code"]] = stocks_net.get(pos["code"], Decimal(0)) + qty * price * sign

        for v in stocks_net.values():
            total_value += abs(v) * Decimal(1000)

        if bal < 10000:
            self.stock_risk_indicator = Decimal(0)
        else:
            self.stock_risk_indicator = Decimal(bal) * Decimal(1000) / (Decimal(bal) + total_value)

        # Future risk indicator: Flow
        account_margin_future = self.api.get_account_margin(account=self.api.futopt_account).data()
        if account_margin_future:
            self.future_risk_indicator = Decimal(account_margin_future[0].get("Flow", 0))
        else:
            self.future_risk_indicator = Decimal(0)

        if g_debug:
            print(f"[ShareInHouse] stock_risk_indicator={self.stock_risk_indicator}, "
                  f"future_risk_indicator={self.future_risk_indicator}")


# ----------------------------
# Thread-safe containers
# ----------------------------
class ThreadSafeList(list):
    def __init__(self, seq=()):
        super().__init__(seq)
        self.locker = threading.Lock()

    def append(self, *args, **kwargs):
        with self.locker:
            return super().append(*args, **kwargs)

    def clear(self, *args, **kwargs):
        with self.locker:
            return super().clear(*args, **kwargs)

    def copy(self, *args, **kwargs):
        with self.locker:
            return list(self)

    def pop(self, *args, **kwargs):
        with self.locker:
            return super().pop(*args, **kwargs)

    def remove(self, *args, **kwargs):
        with self.locker:
            return super().remove(*args, **kwargs)


class ThreadSafeDict(dict):
    def __init__(self, seq=None, **kwargs):
        if seq is not None:
            super().__init__(seq)
        else:
            super().__init__(**kwargs)
        self.locker = threading.Lock()

    def __getitem__(self, item):
        with self.locker:
            return super().__getitem__(item)

    def __setitem__(self, key, value):
        with self.locker:
            return super().__setitem__(key, value)

    def __contains__(self, item):
        with self.locker:
            return super().__contains__(item)

    def get(self, key, default=None):
        with self.locker:
            return super().get(key, default)

    def pop(self, key, default=None):
        with self.locker:
            return super().pop(key, default)

    def keys(self):
        with self.locker:
            return list(super().keys())


# ----------------------------
# MyTrading: manage trade sequences + retries + order callback
# ----------------------------
class MyTrading(threading.Thread):
    def __init__(self, in_api: sj.Shioaji, in_share_holder: ShareInHouse, event_to_terminate: threading.Event):
        super().__init__(daemon=True)
        self.event_to_terminate = event_to_terminate
        self.api = in_api
        self.share_holder = in_share_holder

        self.contracts_dict = self._get_contracts()

        self.trading_dict = {}  # id(trade) -> (trade_package, index)
        self.event_to_start_trading = threading.Event()
        self.trading_package_queue = queue.Queue()

        self.done_states = [sj.constant.Status.Failed, sj.constant.Status.Cancelled,
                            sj.constant.Status.Filled, sj.constant.Status.Inactive]
        self.filling_states = [sj.constant.Status.PartFilled, sj.constant.Status.Filling]
        self.success_states = [sj.constant.Status.Filled]
        self.fail_states = [sj.constant.Status.Failed, sj.constant.Status.Cancelled]
        self.pendding_states = [sj.constant.Status.PendingSubmit, sj.constant.Status.PreSubmitted]

        self.lock_share_data = threading.Lock()
        self.max_time_to_resubmit = 10

        # order callbacks
        self.api.set_order_callback(self.order_callback)
        self.order_status_uncompleted = [sj.constant.OrderState.TFTOrder, sj.constant.OrderState.FOrder,
                                         sj.constant.OrderState.Order]
        self.order_status_completed = [sj.constant.OrderState.TFTDeal, sj.constant.OrderState.FDeal,
                                       sj.constant.OrderState.Deal]

        # status bookkeeping
        self.status_id_dict: ThreadSafeDict = ThreadSafeDict()  # status_id -> (trade_package, trade_index, trade)
        self.status_queue: queue.Queue = queue.Queue()
        self.pending_order_queue: queue.Queue = queue.Queue()
        self.partfilled_order_queue: queue.Queue = queue.Queue()

        self.timedelta_withdraw_order = datetime.timedelta(minutes=1)

        # safety: cancel any leftover trades
        self.cancel_all_trading_order()

    def cancel_all_trading_order(self):
        try:
            trades = self.api.list_trades()
            for trade in trades:
                try:
                    self.api.cancel_order(trade)
                except Exception:
                    pass
        except Exception:
            pass

    def check_trade_status(self, trade: sj.order.Trade) -> str:
        if trade.status.status not in self.done_states:
            return "wait"
        if trade.status.status in self.fail_states:
            return "fail"
        # IOC pending considered fail
        if (trade.status.status in self.pendding_states and
                getattr(trade.order, "order_type", None) == sj.constant.ORDER_TYPE_IOC):
            return "fail"
        return "filled"

    def check_status(self, token: str) -> str:
        # FIX: original had recursion bug
        trade_package = self.trading_dict.get(token)
        if not trade_package:
            return "unknown"
        status = "filled"
        for trade_info in trade_package[0]:
            trade = trade_info.get("trade")
            if trade is None:
                status = "wait"
                break
            status = self.check_trade_status(trade)
            if status != "filled":
                break
        return status

    def run(self) -> None:
        while not self.event_to_terminate.is_set():
            # submit queued packages
            self.event_to_start_trading.wait(0.2)
            if self.event_to_start_trading.is_set():
                try:
                    while not self.trading_package_queue.empty():
                        trade_package = self.trading_package_queue.get()
                        if trade_package["block"]:
                            if trade_package["status"]["waited"]:
                                idx = trade_package["status"]["waited"].pop(0)
                                self._submit_a_trade_to_server(trade_package, idx)
                        else:
                            while trade_package["status"]["waited"]:
                                idx = trade_package["status"]["waited"].pop(0)
                                self._submit_a_trade_to_server(trade_package, idx)
                finally:
                    self.event_to_start_trading.clear()

            # refresh account status and process queues
            self.api.update_status(self.api.futopt_account)
            self.api.update_status(self.api.stock_account)

            self.process_pending_trade()
            self.process_partfilled_trade()

        print("[MyTrading] shut down")

    def order_callback(self, stype: sj.order.OrderStatus, status: dict):
        self.status_queue.put((status, stype))
        self.process_order()

    def process_order(self):
        while not self.status_queue.empty():
            status, stype = self.status_queue.get()
            if g_debug:
                print(f"[order_callback] stype={stype}, status={status}")

            if stype in self.order_status_uncompleted:
                self.process_uncompleted_trade(status)
            else:
                self.process_completed_trade(status)

    def process_trading_package(self, trade_package: dict):
        # all done?
        if (len(trade_package["status"]["waited"]) == 0 and
                len(trade_package["status"]["submitted"]) == 0):
            cb = trade_package.get("callback")
            if cb:
                cb(trade_package["token"], "filled" if len(trade_package["status"]["failed"]) == 0 else "failed")
            self.share_holder.calc_risk_indicators()
            return

        # still has waited items -> schedule next submission
        if trade_package["status"]["waited"]:
            if (not trade_package["block"]) or (len(trade_package["status"]["submitted"]) == 0):
                self.trading_package_queue.put(trade_package)
                self.event_to_start_trading.set()

    def process_retry(self, trade_package: dict, trade_index: int):
        trade_seq = trade_package["trade_seq"]
        trade_seq[trade_index]["time_to_try"] += 1
        if trade_seq[trade_index]["time_to_try"] >= self.max_time_to_resubmit:
            with self.lock_share_data:
                if trade_package["block"]:
                    trade_package["status"]["waited"].clear()
                trade_package["status"]["failed"].append(trade_index)

                # if any filled occurred, reverse-close them
                if len(trade_package["status"]["submitted"]) == 0 and len(trade_package["status"]["filled"]) > 0:
                    while trade_package["status"]["filled"]:
                        idx = trade_package["status"]["filled"].pop(0)
                        if idx in trade_package["status"]["failed"]:
                            continue
                        trade_info = trade_seq[idx]
                        order = trade_info["order"]
                        trade = trade_info.get("trade")
                        if trade is None:
                            continue

                        # reverse action
                        order.action = "Buy" if order.action == "Sell" else "Sell"
                        order.price_type = "MKT"

                        # use dealt qty
                        sum_qty = 0
                        for deal in getattr(trade.status, "deals", []) or []:
                            sum_qty += int(deal.get("quantity", 0))
                        order.quantity = max(1, sum_qty)

                        trade_info.pop("trade", None)
                        trade_info["time_to_try"] = 0
                        trade_package["status"]["waited"].append(idx)
        else:
            # retry this trade first
            trade_package["status"]["waited"].insert(0, trade_index)

    def _extract_status_id(self, status: dict) -> str:
        # robust: completed states usually have trade_id, uncompleted have order->id
        if "trade_id" in status:
            return status["trade_id"]
        if "order" in status and isinstance(status["order"], dict) and "id" in status["order"]:
            return status["order"]["id"]
        if "status" in status and isinstance(status["status"], dict) and "id" in status["status"]:
            return status["status"]["id"]
        return ""

    def process_completed_trade(self, status: dict):
        status_id = self._extract_status_id(status)
        if not status_id:
            return
        if status_id not in self.status_id_dict:
            return

        trade_package, trade_index, trade = self.status_id_dict[status_id]

        if g_debug:
            print(f"[completed] {trade.contract.name}, status={trade.status.status}, status_id={status_id}")

        if trade.status.status in self.success_states:
            with self.lock_share_data:
                if trade_index in trade_package["status"]["submitted"]:
                    trade_package["status"]["submitted"].remove(trade_index)
                trade_package["status"]["filled"].append(trade_index)
            self.status_id_dict.pop(status_id, None)

        elif trade.status.status in self.filling_states:
            self.partfilled_order_queue.put(status_id)

        elif trade.status.status in self.done_states:
            with self.lock_share_data:
                if trade_index in trade_package["status"]["submitted"]:
                    trade_package["status"]["submitted"].remove(trade_index)
            self.status_id_dict.pop(status_id, None)
            self.process_retry(trade_package, trade_index)

        self.process_trading_package(trade_package)

    def process_uncompleted_trade(self, status: dict):
        # error if op_msg not empty
        op_msg = status.get("operation", {}).get("op_msg", "")
        status_id = self._extract_status_id(status)
        if not op_msg or not status_id:
            return
        if status_id not in self.status_id_dict:
            return

        trade_package, trade_index, trade = self.status_id_dict[status_id]
        self.status_id_dict.pop(status_id, None)

        with self.lock_share_data:
            if trade_index in trade_package["status"]["submitted"]:
                trade_package["status"]["submitted"].remove(trade_index)

        self.process_retry(trade_package, trade_index)
        self.process_trading_package(trade_package)

    def process_partfilled_trade(self):
        not_done_list = []
        while not self.partfilled_order_queue.empty():
            status_id = self.partfilled_order_queue.get()
            if status_id not in self.status_id_dict:
                continue
            trade_package, trade_index, trade = self.status_id_dict[status_id]

            deals = getattr(trade.status, "deals", []) or []
            remaining = int(getattr(trade.order, "quantity", 0))
            for deal in deals:
                remaining -= int(deal.get("quantity", 0))
            if remaining <= 0:
                with self.lock_share_data:
                    if trade_index in trade_package["status"]["submitted"]:
                        trade_package["status"]["submitted"].remove(trade_index)
                    trade_package["status"]["filled"].append(trade_index)
                self.status_id_dict.pop(status_id, None)
                self.process_trading_package(trade_package)
                continue

            # timeout-based accept partial filled
            tnow = _now()
            order_timestamp = getattr(trade.order, "order_datetime", None)
            if order_timestamp and (tnow - order_timestamp) > self.timedelta_withdraw_order:
                with self.lock_share_data:
                    if trade_index in trade_package["status"]["submitted"]:
                        trade_package["status"]["submitted"].remove(trade_index)
                    trade_package["status"]["filled"].append(trade_index)
                self.status_id_dict.pop(status_id, None)
                self.process_trading_package(trade_package)
            else:
                not_done_list.append(status_id)

        for sid in not_done_list:
            self.partfilled_order_queue.put(sid)

    def process_pending_trade(self):
        not_done_list = []
        while not self.pending_order_queue.empty():
            status_id = self.pending_order_queue.get()
            if status_id not in self.status_id_dict:
                continue
            trade_package, trade_index, trade = self.status_id_dict[status_id]

            if trade.status.status in self.success_states:
                with self.lock_share_data:
                    if trade_index in trade_package["status"]["submitted"]:
                        trade_package["status"]["submitted"].remove(trade_index)
                    trade_package["status"]["filled"].append(trade_index)
                self.status_id_dict.pop(status_id, None)
                self.process_trading_package(trade_package)

            elif trade.status.status in self.fail_states:
                with self.lock_share_data:
                    if trade_index in trade_package["status"]["submitted"]:
                        trade_package["status"]["submitted"].remove(trade_index)
                self.status_id_dict.pop(status_id, None)
                self.process_retry(trade_package, trade_index)
                self.process_trading_package(trade_package)

            else:
                not_done_list.append(status_id)

        for k in not_done_list:
            self.pending_order_queue.put(k)

    def non_blocking_cb2(self, trade: sj.order.Trade):
        # Called by place_order non-blocking mode
        with self.lock_share_data:
            if id(trade) not in self.trading_dict:
                return
            trade_package, trade_index = self.trading_dict[id(trade)]

            status_id = getattr(trade.status, "id", None) or getattr(trade.status, "trade_id", None)
            if status_id:
                self.status_id_dict[status_id] = (trade_package, trade_index, trade)

                # pending -> enqueue
                if trade.status.status in self.pendding_states:
                    self.pending_order_queue.put(status_id)

                # partfilled -> enqueue
                if trade.status.status in self.filling_states:
                    self.partfilled_order_queue.put(status_id)

                self.trading_dict.pop(id(trade), None)

                if g_debug:
                    print(f"[non_block_cb2] token={trade_package['token']} idx={trade_index} "
                          f"{trade.contract.name} status={trade.status.status} status_id={status_id}")
            else:
                if g_debug:
                    print(f"[non_block_cb2] status_id missing, trade.status={trade.status}")

    def prepare_a_trade(
        self,
        product_type: str,
        code: str,
        price: Union[float, Decimal, int],
        qty: int,
        action: str,
        price_type: str = "LMT",
        order_type: str = "IOC",
        order_cond: str = "Cash",
        octype: str = "Auto",
    ) -> Dict:
        contract = self.contracts_dict[code]

        if product_type == "stock":
            order = self.api.Order(
                price=price,
                quantity=qty,
                action=action,
                price_type=price_type,
                order_type=order_type,
                order_lot=sj.constant.TFTStockOrderLot.Common,
                order_cond=order_cond,
                first_sell=sj.constant.StockFirstSell.No,
                account=self.api.stock_account,
            )
        else:
            order = self.api.Order(
                action=action,
                price=price,
                quantity=qty,
                price_type=price_type,
                order_type=order_type,
                octype=octype,
                account=self.api.futopt_account,
            )
        return {"contract": contract, "order": order, "time_to_try": 0}

    def submit_a_trade_sequence(
        self,
        trade_seq: List[Dict],
        cb: Callable[[str, str], object] = None,
        block: bool = True,
    ) -> str:
        do_trading = True
        if not self.share_holder.is_available_capital():
            # keep your original guard logic
            for trade_info in trade_seq:
                contract = trade_info["contract"]
                order = trade_info["order"]

                if isinstance(contract, sj.contracts.Stock):
                    share_info = self.share_holder.get_stock_info_in_house(contract.code)
                elif isinstance(contract, sj.contracts.Future):
                    share_info = self.share_holder.get_future_info_in_house(contract.code)
                else:
                    share_info = self.share_holder.get_option_info_in_house(contract.code)

                if share_info is None or order.action in [share["action"] for share in share_info]:
                    do_trading = False
                    break

        if not do_trading:
            return ""

        trade_token = secrets.token_hex()
        trade_status = {"submitted": [], "waited": list(range(len(trade_seq))), "filled": [], "failed": []}
        trade_package = {
            "token": trade_token,
            "trade_seq": trade_seq,
            "callback": cb,
            "block": block,
            "status": trade_status,
        }
        self.trading_package_queue.put(trade_package)
        self.event_to_start_trading.set()
        return trade_token

    def _submit_a_trade_to_server(self, trade_package: Dict, index: int):
        with self.lock_share_data:
            trade_info = trade_package["trade_seq"][index]
            trade_package["status"]["submitted"].append(index)

            trade = self.api.place_order(
                trade_info["contract"], trade_info["order"], timeout=0, cb=self.non_blocking_cb2
            )
            trade_info["trade"] = trade
            if trade is not None:
                self.trading_dict[id(trade)] = (trade_package, index)

            if g_debug and trade is not None:
                print(f"[submit] {trade.contract.name}, status={trade.status.status}")

        return trade

    def _get_contracts(self):
        contracts = {
            code: contract
            for name, iter_contract in self.api.Contracts
            for code, contract in iter_contract._code2contract.items()
        }
        return contracts


# ----------------------------
# BidAskHolder: subscribe and store latest bidask
# ----------------------------
class BidAskHolder:
    def __init__(self, in_api: sj.Shioaji):
        self.api = in_api
        self.lock_update_bidask_dict = threading.Lock()
        self.broadcast_event_dict: Dict[str, threading.Event] = defaultdict(threading.Event)
        self.bidask_dict: defaultdict = defaultdict(lambda: None)

        self.api.set_context(self)

        @self.api.on_bidask_fop_v1(bind=True)
        def quote_callback_fu(self, exchange: Exchange, bidask: BidAskFOPv1):
            self.on_quote(bidask)

        @self.api.on_bidask_stk_v1(bind=True)
        def quote_callback_st(self, exchange: Exchange, bidask: BidAskSTKv1):
            self.on_quote(bidask)

    def on_quote(self, quote: Union[sj.BidAskFOPv1, sj.BidAskSTKv1]) -> None:
        code = quote.code
        with self.lock_update_bidask_dict:
            self.bidask_dict[code] = quote
        self.broadcast_event_dict[code].set()

    def add_bidask(self, contract: sj.contracts.Contract, notify_event: threading.Event):
        code = contract.code
        self.api.quote.subscribe(contract, quote_type=sj.constant.QuoteType.BidAsk, version="v1")
        self.broadcast_event_dict[code] = notify_event

    def remove_bidask(self, contract: sj.contracts.Contract):
        code = contract.code
        self.api.quote.unsubscribe(contract, quote_type=sj.constant.QuoteType.BidAsk, version="v1")
        self.broadcast_event_dict.pop(code, None)

    def get_latest_bidask(self, contract: sj.contracts.Contract):
        code = contract.code
        with self.lock_update_bidask_dict:
            return self.bidask_dict.get(code, None)


# ----------------------------
# MIOC: multi IOC runner (kept)
# ----------------------------
class MIOC(threading.Thread):
    def __init__(self, in_api: sj.Shioaji, contract: sj.contracts.Contract, order: sj.order.Order,
                 cb: Callable, event_to_terminate: threading.Event):
        super().__init__(daemon=True)
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
        while do_again and try_times < 10 and not self.event_to_terminate.is_set():
            trade = self.api.place_order(self.contract, self.order, timeout=0, cb=self.callback)
            is_wait = True
            while is_wait and not self.event_to_terminate.is_set():
                self.event_callback.wait(3)
                if trade is None or trade.status.status in [
                    sj.constant.Status.Cancelled, sj.constant.Status.Failed, sj.constant.Status.PendingSubmit
                ]:
                    is_wait = False
                    try_times += 1
                elif trade.status.status in [sj.constant.Status.Filled]:
                    self.cb(trade)
                    do_again = False
                    is_wait = False
                self.event_callback.clear()
                self.trade = trade


# ----------------------------
# ThreadSafeShioajiAPI: serialize API access + safe login
# ----------------------------
class ThreadSafeShioajiAPI(sj.Shioaji):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.lock_to_access_api = threading.Lock()
        self.fp = open(f"{_now().strftime('%Y-%m-%d-%H-%M-%S')}.csv", "at", encoding="utf-8")

        # decide simulation
        is_simulation = kwargs.get("simulation", False) or _env_bool("SHIOAJI_SIMULATION", False)

        # login with env
        if not is_simulation:
            person_id = os.getenv("Sinopack_PERSON_ID", "")
            passwd = os.getenv("Sinopack_PASSWORD", "")
            ca_path = os.getenv("Sinopack_CA_PATH", "")
            ca_passwd = os.getenv("Sinopack_CA_PASSWORD", "")

            if not person_id or not passwd or not ca_path or not ca_passwd:
                raise RuntimeError(
                    "Missing required env vars for real login: "
                    "SHIOAJI_PERSON_ID/SHIOAJI_PASSWORD/SHIOAJI_CA_PATH/SHIOAJI_CA_PASSWORD"
                )

            self.login(
                person_id=person_id,
                passwd=passwd,
                contracts_cb=print,
            )
            self.activate_ca(
                ca_path=ca_path,
                ca_passwd=ca_passwd,
                person_id=person_id,
            )
        else:
            # keep your original simulation default but also allow env override
            person_id = os.getenv("Sinopack_PERSON_ID", "PAPIUSER03")
            passwd = os.getenv("Sinopack_PASSWORD", "2222")
            self.login(
                person_id=person_id,
                passwd=passwd,
                contracts_cb=lambda security_type: print(f"{repr(security_type)} fetch done.")
            )

        # wait until option contracts ready
        while "TXO" not in self.Contracts.Options.keys() or self.Contracts.Options.TXO is None:
            time.sleep(0.2)

    def account_balance(self, timeout: int = 5000, cb: Callable = None):
        with self.lock_to_access_api:
            return super().account_balance(timeout, cb)

    def get_account_margin(self, currency="NTD", margin_type="1", account={}):
        with self.lock_to_access_api:
            return super().get_account_margin(currency, margin_type, account)

    def place_order(self, contract, order, timeout: int = 5000, cb: Callable = None) -> sj.order.Trade:
        global g_record_only, g_record
        with self.lock_to_access_api:
            if g_record_only:
                self.fp.write(f"{contract.name},{order.action},{order.quantity},{order.price},"
                              f"{getattr(order, 'order_cond', '')},{getattr(order, 'order_type', '')}\n")
                self.fp.flush()
                return None
            else:
                if g_record:
                    self.fp.write(f"{contract.name},{order.action},{order.quantity},{order.price},"
                                  f"{getattr(order, 'order_cond', '')},{getattr(order, 'order_type', '')}\n")
                    self.fp.flush()
                return super().place_order(contract, order, timeout, cb)

    def list_positions(self, account=None, unit=sj.constant.Unit.Common, timeout: int = 5000, cb: Callable = None):
        with self.lock_to_access_api:
            return super().list_positions(account, unit, timeout, cb)

    def get_account_openposition(self, product_type="0", query_type="0", account={}):
        with self.lock_to_access_api:
            return super().get_account_openposition(product_type, query_type, account)

    def update_order(self, trade, price=None, qty=None, timeout: int = 5000, cb: Callable = None):
        with self.lock_to_access_api:
            return super().update_order(trade, price, qty, timeout, cb)

    def cancel_order(self, trade, timeout: int = 5000, cb: Callable = None):
        with self.lock_to_access_api:
            return super().cancel_order(trade, timeout, cb)

    def update_status(self, account=None, timeout: int = 5000, cb: Callable = None):
        with self.lock_to_access_api:
            try:
                return super().update_status(account, timeout, cb)
            except Exception:
                return None


# ----------------------------
# Condition base
# ----------------------------
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


# ----------------------------
# OptionNeturalCondition (kept; only minor clean)
# ----------------------------
class OptionNeturalCondition(ConditionBase):
    def __init__(self, in_api: sj.Shioaji, contracts: List[sj.contracts.Contract], bidask_holder: BidAskHolder,
                 trading_holder: MyTrading, notify_event_to_terminate: threading.Event):
        self.api = in_api
        self.bidask_holder = bidask_holder
        self.trading_holder = trading_holder
        self.notify_event_to_terminate = notify_event_to_terminate
        self.contracts = contracts

    def delta_call(self, S, K, T, r, c):
        implied_vol = 1.0
        min_value = 100.0
        tts = 0.0001
        for i in range(0, 10000, 1):
            sigma = implied_vol - tts * i
            if sigma <= 0:
                break
            d1 = (np.log(S / K) + (r + sigma * sigma / 2.0) * T) / (sigma * np.sqrt(T))
            d2 = d1 - sigma * np.sqrt(T)
            call = S * stats.norm.cdf(d1) - K * np.exp(-r * T) * stats.norm.cdf(d2)
            abs_diff = abs(call - c)
            if abs_diff <= min_value:
                min_value = abs_diff
                implied_vol = sigma
        return implied_vol * 100

    def check(self) -> None:
        bidasks = [self.bidask_holder.get_latest_bidask(contr) for contr in self.contracts]
        if g_debug:
            print("[OptionNeturalCondition] bidasks fetched:", len(bidasks))

    def get_contracts(self) -> List[sj.contracts.Contract]:
        return self.contracts

    def get_codes(self) -> List[str]:
        return [k.code for k in self.contracts]

    def add_qty_in_house(self, code: str, quantity: int) -> None:
        pass

    def set_qty_for_first_sell(self, code: str, quantity: int) -> None:
        pass


# ----------------------------
# ArbitrageCondition (core)
# ----------------------------
class ArbitrageCondition(ConditionBase):
    def __init__(
        self,
        in_api: sj.Shioaji,
        bidask_holder: BidAskHolder,
        share_holder: ShareInHouse,
        trading_holder: MyTrading,
        price_change_interval: Dict,
        min_interval_by_price: Dict,
        max_order_qty: Dict,
        contract_st: sj.contracts.Contract,
        contract_fu: sj.contracts.Contract,
        notify_event_to_terminate: threading.Event,
    ):
        self.api = in_api
        self.price_change_interval = price_change_interval
        self.min_interval_by_price = min_interval_by_price

        self.contract_st = contract_st
        self.contract_fu = contract_fu
        self.bidask_holder = bidask_holder
        self.share_holder = share_holder
        self.trading_holder = trading_holder
        self.notify_event_to_terminate = notify_event_to_terminate

        self.max_amount_st = max_order_qty[contract_st.code]
        self.max_amount_fu = max_order_qty[contract_fu.code]

        self.min_required_volume_for_stock = 4
        self.min_required_volume_for_future = 2

        self.order_qty = Counter({contract_st.code: 0, contract_fu.code: 0})
        self.deal_qty = Counter({contract_st.code: 0, contract_fu.code: 0})
        self.deal_price = Counter({contract_st.code: Decimal(0), contract_fu.code: Decimal(0)})

        self.max_amount = Counter({contract_st.code: self.max_amount_st, contract_fu.code: self.max_amount_fu})

        self.default_gain = Counter({"forwardation": 0.5, "backwardation": 0.5})
        self.min_gain = Counter({"forwardation": 0.0, "backwardation": 0.0})
        self.amount_per_trading = Counter({"stock": 2, "future": 1})

        self.max_amount_for_first_sell = Counter({contract_st.code: 0, contract_fu.code: 0})

        self.trade_status: Dict[str, str] = {}
        self.submitted_trade: Dict[str, Dict[str, int]] = {}

        self.time_to_close_all_backwardation = datetime.time(hour=13, minute=10)
        self.time_to_call_auction = datetime.time(hour=13, minute=25)
        self.time_to_close_market = datetime.time(hour=13, minute=30)
        self.time_to_open_market = datetime.time(hour=9, minute=0)
        self.time_to_pause_submit_order = datetime.time(hour=8, minute=0)

        self.lock_for_sharing_data = threading.Lock()
        self.check_shares_in_house()

    def check_shares_in_house(self):
        # stock
        stock_shares = self.share_holder.get_stock_info_in_house(self.contract_st.code)
        if stock_shares:
            sum_value = Decimal(0)
            sum_amount = Decimal(0)
            for stock in stock_shares:
                sign = 1 if stock["action"] == "Buy" else -1
                sum_value += Decimal(stock["qty"]) * Decimal(stock["price"])
                sum_amount += Decimal(stock["qty"])
                self.add_qty_in_house(self.contract_st.code, stock["qty"] * sign)
            if sum_amount > 0:
                self.deal_price[self.contract_st.code] = sum_value / sum_amount

        # future
        future_shares = self.share_holder.get_future_info_in_house(self.contract_fu.code)
        if future_shares:
            sum_value = Decimal(0)
            sum_amount = Decimal(0)
            for fu in future_shares:
                sign = 1 if fu["action"] == "Buy" else -1
                sum_value += Decimal(fu["qty"]) * Decimal(fu["price"])
                sum_amount += Decimal(fu["qty"])
                self.add_qty_in_house(self.contract_fu.code, fu["qty"] * sign)
            if sum_amount > 0:
                self.deal_price[self.contract_fu.code] = sum_value / sum_amount

    def trading_callback(self, token: str, status: str):
        self.trade_status[token] = status
        with self.lock_for_sharing_data:
            if token not in self.submitted_trade:
                return
            if status == "filled":
                self.deal_qty[self.contract_st.code] += self.submitted_trade[token]["stock"]
                self.deal_qty[self.contract_fu.code] += self.submitted_trade[token]["future"]

            self.order_qty[self.contract_st.code] -= self.submitted_trade[token]["stock"]
            self.order_qty[self.contract_fu.code] -= self.submitted_trade[token]["future"]

        if g_debug:
            print(f"[Arb cb] token={token} status={status}")

    def add_qty_in_house(self, code: str, quantity: int) -> None:
        with self.lock_for_sharing_data:
            self.deal_qty[code] += quantity

    def set_qty_for_first_sell(self, code: str, quantity: int) -> None:
        with self.lock_for_sharing_data:
            self.max_amount_for_first_sell[code] = quantity

    def get_codes(self) -> List[str]:
        return [self.contract_st.code, self.contract_fu.code]

    def get_contracts(self) -> List[sj.contracts.Contract]:
        return [self.contract_st, self.contract_fu]

    def adjust_min_gain(self):
        now_time = _now().time()
        with self.lock_for_sharing_data:
            if self.time_to_close_all_backwardation < now_time < self.time_to_call_auction and self.deal_qty[
                self.contract_st.code
            ] < 0:
                self.min_gain["forwardation"] = -1
                self.min_gain["backwardation"] = 999
            elif self.time_to_pause_submit_order < now_time < self.time_to_open_market:
                self.min_gain["forwardation"] = 999
                self.min_gain["backwardation"] = 999
            elif self.time_to_call_auction < now_time <= self.time_to_close_market:
                self.min_gain["forwardation"] = 999
                self.min_gain["backwardation"] = 999
            else:
                ratio = self.deal_qty[self.contract_st.code] / float(self.max_amount[self.contract_st.code])
                self.min_gain["forwardation"] = max(0, self.default_gain["forwardation"] + math.ceil(
                    self.default_gain["forwardation"] * ratio
                ))
                self.min_gain["backwardation"] = max(0, self.default_gain["backwardation"] - math.ceil(
                    self.default_gain["backwardation"] * ratio
                ))

    def check(self) -> None:
        bidask_st = self.bidask_holder.get_latest_bidask(self.contract_st)
        bidask_fu = self.bidask_holder.get_latest_bidask(self.contract_fu)
        if bidask_st is not None and bidask_fu is not None:
            self.check_and_submit_order_for_open_a_position(bidask_st, bidask_fu)

    def check_and_submit_order_for_open_a_position(self, stock_bidask, future_bidask):
        global g_enable_trading

        contr_st = self.contract_st
        contr_fu = self.contract_fu
        st = contr_st.code
        fu = contr_fu.code

        now_time = _now().time()

        # ---------------------------
        # forwardation: buy stock ask, sell future bid
        # ---------------------------
        buy_price = stock_bidask.ask_price[0]
        buy_volume = stock_bidask.ask_volume[0]
        sell_price = future_bidask.bid_price[0]
        sell_volume = future_bidask.bid_volume[0]

        if (self.time_to_pause_submit_order <= now_time < self.time_to_open_market or
            self.time_to_call_auction <= now_time <= self.time_to_close_market or
            buy_price < 1 or sell_price < 1):
            return

        is_first_buy = self.deal_qty[st] <= 0 and self.time_to_open_market < now_time < self.time_to_call_auction
        extra_gain = 1 if is_first_buy else 0

        exist_margin = (self.deal_price[st] - self.deal_price[fu]) if (
            self.deal_qty[fu] != 0 and self.deal_qty[st] != 0 and (self.deal_qty[st] + self.order_qty[st]) != 0
        ) else 0

        min_margin = Decimal(self.min_interval_by_price[int(buy_price)][0]) - int(buy_price)
        expected_margin = Decimal(sell_price - buy_price) + Decimal(exist_margin)

        price_verify = (
            contr_st.limit_up >= buy_price >= contr_st.limit_down and
            contr_fu.limit_down <= sell_price <= contr_fu.limit_up and
            sell_price >= buy_price and
            exist_margin >= 0 and
            (((sell_price - buy_price) * 2) > (Decimal(self.price_change_interval[int(buy_price)]) * Decimal(extra_gain) + min_margin)
             or expected_margin > min_margin)
        )

        volume_verify = (
            buy_volume > self.min_required_volume_for_stock and
            sell_volume > self.min_required_volume_for_future and
            (
                abs(self.deal_qty[st] + self.order_qty[st] + self.amount_per_trading["stock"]) <= self.max_amount[st] or
                abs(self.deal_qty[st] + self.order_qty[st] + self.amount_per_trading["stock"]) < abs(self.deal_qty[st] + self.order_qty[st])
            )
        )

        # submit forwardation
        if volume_verify and price_verify:
            print(f"\n[FWD] buy {contr_st.name}@{buy_price}*{self.amount_per_trading['stock']} "
                  f"sell {contr_fu.name}@{sell_price}*{self.amount_per_trading['future']}, "
                  f"expected_margin={expected_margin}")

            if g_enable_trading:
                trades = [
                    self.trading_holder.prepare_a_trade(
                        "future", fu, future_bidask.bid_price[0], self.amount_per_trading["future"], "Sell"
                    ),
                    self.trading_holder.prepare_a_trade(
                        "stock", st, stock_bidask.ask_price[0], self.amount_per_trading["stock"], "Buy",
                        order_type="FOK"
                    ),
                ]
                with self.lock_for_sharing_data:
                    token = self.trading_holder.submit_a_trade_sequence(trades, self.trading_callback, block=False)
                    if token:
                        self.order_qty[st] += self.amount_per_trading["stock"]
                        self.order_qty[fu] -= self.amount_per_trading["future"]
                        self.submitted_trade[token] = {"stock": self.amount_per_trading["stock"],
                                                       "future": -self.amount_per_trading["future"]}

        # ---------------------------
        # backwardation: sell stock bid, buy future ask
        # ---------------------------
        buy_price = future_bidask.ask_price[0]
        buy_volume = future_bidask.ask_volume[0]
        sell_price = stock_bidask.bid_price[0]
        sell_volume = stock_bidask.bid_volume[0]

        is_first_sell = self.deal_qty[st] <= 0 and self.time_to_open_market < now_time < self.time_to_call_auction
        extra_gain = 2 if is_first_sell else 0

        exist_margin = (self.deal_price[fu] - self.deal_price[st]) if (
            self.deal_qty[fu] != 0 and self.deal_qty[st] != 0 and (self.deal_qty[st] + self.order_qty[st]) != 0
        ) else 0

        min_margin = Decimal(self.min_interval_by_price[int(buy_price)][0] - int(buy_price))
        expected_margin = Decimal(sell_price - buy_price) + Decimal(exist_margin)

        price_verify = (
            contr_st.limit_up >= buy_price >= contr_st.limit_down and
            contr_fu.limit_down <= sell_price <= contr_fu.limit_up and
            sell_price >= buy_price and
            exist_margin >= 0 and
            (((sell_price - buy_price) * 2) > (Decimal(self.price_change_interval[int(buy_price)]) * Decimal(extra_gain) + min_margin)
             or expected_margin > min_margin)
        )

        volume_verify = (
            buy_volume > self.min_required_volume_for_future and
            sell_volume > self.min_required_volume_for_stock and
            (
                abs(self.deal_qty[st] + self.order_qty[st] - self.amount_per_trading["stock"]) <= self.max_amount[st] or
                abs(self.deal_qty[st] + self.order_qty[st] - self.amount_per_trading["stock"]) < abs(self.deal_qty[st] + self.order_qty[st])
            )
        )

        print(f"{_now()}: {contr_st.name} "
              f"{stock_bidask.bid_price[0]:.2f}/{stock_bidask.bid_volume[0]} - "
              f"{stock_bidask.ask_price[0]:.2f}/{stock_bidask.ask_volume[0]}, "
              f"{contr_fu.name} "
              f"{future_bidask.bid_price[0]:.2f}/{future_bidask.bid_volume[0]} - "
              f"{future_bidask.ask_price[0]:.2f}/{future_bidask.ask_volume[0]}",
              end="\r")

        if volume_verify and price_verify and contr_st.code != "3260":
            print(f"\n[BWD] sell {contr_st.name}@{sell_price}*{self.amount_per_trading['stock']} "
                  f"buy {contr_fu.name}@{buy_price}*{self.amount_per_trading['future']}, "
                  f"expected_margin={expected_margin}")

            if g_enable_trading:
                trades = [
                    self.trading_holder.prepare_a_trade(
                        "stock", st, stock_bidask.bid_price[0], self.amount_per_trading["stock"], "Sell",
                        order_cond="ShortSelling", order_type="FOK"
                    ),
                    self.trading_holder.prepare_a_trade(
                        "future", fu, future_bidask.ask_price[0], self.amount_per_trading["future"], "Buy"
                    ),
                ]
                with self.lock_for_sharing_data:
                    token = self.trading_holder.submit_a_trade_sequence(trades, self.trading_callback, block=True)
                    if token:
                        self.order_qty[st] -= self.amount_per_trading["stock"]
                        self.order_qty[fu] += self.amount_per_trading["future"]
                        self.submitted_trade[token] = {"stock": -self.amount_per_trading["stock"],
                                                       "future": self.amount_per_trading["future"]}

        self.adjust_min_gain()


# ----------------------------
# ConditionCheck: event-driven checker thread
# ----------------------------
class ConditionCheck(threading.Thread):
    def __init__(self, terminate_event: threading.Event, condition: ConditionBase):
        super().__init__(daemon=True)
        self.terminate_event = terminate_event
        self.condition = condition
        self.notify_event = threading.Event()

    def get_notify_event(self):
        return self.notify_event

    def run(self):
        try:
            while not self.terminate_event.is_set():
                self.notify_event.wait(5)
                if self.notify_event.is_set():
                    self.condition.check()
                    self.notify_event.clear()
            print("[ConditionCheck] shut down")
        except Exception as e:
            print(f"[ConditionCheck] terminated by exception: {e}", file=sys.stderr)
            raise


# ----------------------------
# Minimal runnable scaffold (you can integrate into your launcher)
# ----------------------------
def main():
    global g_debug, g_enable_trading

    g_debug = _env_bool("DEBUG", False)
    g_enable_trading = _env_bool("ENABLE_TRADING", True)

    terminate_event = threading.Event()

    # API
    api = ThreadSafeShioajiAPI(simulation=_env_bool("SHIOAJI_SIMULATION", False))

    # holders
    share_holder = ShareInHouse(api, terminate_event)
    share_holder.start()

    bidask_holder = BidAskHolder(api)

    trading_holder = MyTrading(api, share_holder, terminate_event)
    trading_holder.start()

    # TODO: your own config mapping
    # Example placeholders
    price_change_interval = defaultdict(lambda: 1)  # you should provide real mapping
    min_interval_by_price = defaultdict(lambda: (0, 0))  # you should provide real mapping
    max_order_qty = defaultdict(lambda: 10)  # you should provide real mapping

    # Example: pick some contract codes (replace with your real pair)
    # NOTE: you must ensure these codes exist in your Contracts
    # e.g. stock_code="2330", future_code="TXF202601" etc.
    stock_code = os.getenv("ARB_STOCK_CODE", "2330")
    future_code = os.getenv("ARB_FUTURE_CODE", "TXF")

    try:
        contract_st = api.Contracts.Stocks[stock_code]
    except Exception:
        raise RuntimeError(f"Invalid stock code: {stock_code}")

    # Futures: depends on your usage; here we use api.Contracts.Futures[future_code] if exists
    # If you use delivery month codes, set ARB_FUTURE_CODE accordingly.
    try:
        contract_fu = api.Contracts.Futures[future_code]
    except Exception:
        # fallback: try Futures.<name>.<code> style not supported directly; fail loud
        raise RuntimeError(f"Invalid future code: {future_code}")

    # build condition
    cond = ArbitrageCondition(
        in_api=api,
        bidask_holder=bidask_holder,
        share_holder=share_holder,
        trading_holder=trading_holder,
        price_change_interval=price_change_interval,
        min_interval_by_price=min_interval_by_price,
        max_order_qty=max_order_qty,
        contract_st=contract_st,
        contract_fu=contract_fu,
        notify_event_to_terminate=terminate_event,
    )

    checker = ConditionCheck(terminate_event, cond)
    checker.start()

    # subscribe bidask
    bidask_holder.add_bidask(contract_st, checker.get_notify_event())
    bidask_holder.add_bidask(contract_fu, checker.get_notify_event())

    print("\n[main] running... Ctrl+C to stop")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[main] stopping...")
    finally:
        terminate_event.set()
        checker.get_notify_event().set()
        share_holder.update()
        trading_holder.event_to_start_trading.set()
        time.sleep(0.5)


if __name__ == "__main__":
    main()
