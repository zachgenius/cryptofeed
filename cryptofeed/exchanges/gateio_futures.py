'''
Copyright (C) 2017-2022 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from collections import defaultdict
import logging
from decimal import Decimal
import time
from typing import Dict, Tuple

from yapic import json

from cryptofeed.connection import AsyncConnection, RestEndpoint, Routes, WebsocketEndpoint
from cryptofeed.defines import BID, ASK, CANDLES, GATEIO, L2_BOOK, TICKER, TRADES, BUY, SELL, OPEN_INTEREST, \
    LIQUIDATIONS, ORDER_INFO, FILLS, POSITIONS, FUNDING
from cryptofeed.feed import Feed
from cryptofeed.symbols import Symbol
from cryptofeed.types import OrderBook, Trade, Ticker, Candle, Funding
from cryptofeed.util.time import timedelta_str_to_sec

LOG = logging.getLogger('feedhandler')


# perpetual futures
class GateioFutures(Feed):
    id = GATEIO
    websocket_endpoints = [
        WebsocketEndpoint('wss://fx-ws.gateio.ws/v4/ws/',
                          sandbox="wss://fx-ws-testnet.gateio.ws/v4/ws/{}",
                          options={'compression': None})]
    rest_endpoints = [RestEndpoint('https://api.gateio.ws/api/v4',
                                   sandbox="https://fx-api-testnet.gateio.ws/api/v4",
                                   routes=Routes('/futures/{}/contracts',
                                                 funding="/futures/{}/funding_rate",
                                                 l2book='/futures/{}/order_book?contract={}&limit=100&with_id=true'))]

    valid_candle_intervals = {'10s', '1m', '5m', '15m', '30m', '1h', '4h', '8h', '1d', '3d'}
    websocket_channels = {
        L2_BOOK: 'futures.order_book_update',
        TRADES: 'futures.trades',
        TICKER: 'futures.book_ticker',
        CANDLES: 'futures.candlesticks',
        LIQUIDATIONS: 'futures.liquidates',
        ORDER_INFO: 'futures.orders',
        FUNDING: 'futures.tickers',
        POSITIONS: 'futures.positions',
    }

    @classmethod
    def _parse_symbol_data(cls, data: dict) -> Tuple[Dict, Dict]:
        ret = {}
        info = {'instrument_type': {}}

        for entry in data:
            if entry["trade_status"] != "tradable":
                continue
            s = Symbol(entry['base'], entry['quote'])
            ret[s.normalized] = entry['id']
            info['instrument_type'][s.normalized] = s.type
        return ret, info

    def _reset(self):
        self._l2_book = {}
        self.last_update_id = {}
        self.forced = defaultdict(bool)

    async def _ticker(self, msg: dict, timestamp: float):
        """
        {
          "time": 1615366379,
          "channel": "futures.book_ticker",
          "event": "update",
          "error": null,
          "result": {
            "t": 1615366379123,
            "u": 2517661076,
            "s": "BTC_USD",
            "b": "54696.6",
            "B": 37000,
            "a": "54696.7",
            "A": 47061
          }
        }

        """

        t = Ticker(
            self.id,
            self.exchange_symbol_to_std_symbol(msg['result']['s']),
            Decimal(msg['result']['b']),
            Decimal(msg['result']['a']),
            float(msg['time']),
            raw=msg
        )
        await self.callback(TICKER, t, timestamp)

    async def _funding(self, msg: dict, timestamp: float):

        """
        {
          "time": 1541659086,
          "channel": "futures.tickers",
          "event": "update",
          "error": null,
          "result": [
            {
              "contract": "BTC_USD",
              "last": "118.4",
              "change_percentage": "0.77",
              "funding_rate": "-0.000114",
              "funding_rate_indicative": "0.01875", // predicted
              "mark_price": "118.35",
              "index_price": "118.36",
              "total_size": "73648",
              "volume_24h": "745487577",
              "volume_24h_btc": "117",
              "volume_24h_usd": "419950",
              "quanto_base_rate": "",
              "volume_24h_quote": "1665006",
              "volume_24h_settle": "178",
              "volume_24h_base": "5526"
            }
          ]
        }

        """

        next_time = None  # self.timestamp_normalize(msg['T']) if msg['T'] > 0 else None
        rate = Decimal(msg['funding_rate']) if msg['funding_rate'] else None
        # if next_time is None:
        #     rate = None

        f = Funding(self.id,
                    self.exchange_symbol_to_std_symbol(msg['contract']),
                    Decimal(msg['mark_price']),
                    rate,
                    next_time,
                    self.timestamp_normalize(msg['E']),
                    predicted_rate=Decimal(msg['funding_rate_indicative']) if 'funding_rate_indicative' in msg and msg['funding_rate_indicative'] is not None else None,
                    raw=msg)
        await self.callback(FUNDING, f, timestamp)

    async def _trades(self, msg: dict, timestamp: float):
        """
        {
          "channel": "futures.trades",
          "event": "update",
          "time": 1541503698,
          "result": [
            {
              "size": -108,
              "id": 27753479,
              "create_time": 1545136464,
              "create_time_ms": 1545136464123,
              "price": "96.4",
              "contract": "BTC_USD"
            }
          ]
        }

        """
        t = Trade(
            self.id,
            self.exchange_symbol_to_std_symbol(msg['result']['contract']),
            SELL if msg['result']['size'] < 0 else BUY,
            Decimal(msg['result']['size']),
            Decimal(msg['result']['price']),
            float(msg['result']['create_time_ms']) / 1000,
            id=str(msg['result']['id']),
            raw=msg
        )
        await self.callback(TRADES, t, timestamp)

    async def _snapshot(self, symbol: str):
        """
        {
          "id": 123456,
          "current": 1623898993.123,
          "update": 1623898993.121,
          "asks": [
            {
              "p": "1.52",
              "s": 100
            },
            {
              "p": "1.53",
              "s": 40
            }
          ],
          "bids": [
            {
              "p": "1.17",
              "s": 150
            },
            {
              "p": "1.16",
              "s": 203
            }
          ]
        }

        """
        ret = await self.http_conn.read(self.rest_endpoints[0].route('order_book', self.sandbox).format(symbol))
        data = json.loads(ret, parse_float=Decimal)

        symbol = self.exchange_symbol_to_std_symbol(symbol)
        self._l2_book[symbol] = OrderBook(self.id, symbol, max_depth=self.max_depth)
        self.last_update_id[symbol] = data['id']
        self._l2_book[symbol].book.bids = {Decimal(_item['p']): Decimal(_item['s']) for _item in data['bids']}
        self._l2_book[symbol].book.asks = {Decimal(_item['p']): Decimal(_item['s']) for _item in data['asks']}
        await self.book_callback(L2_BOOK, self._l2_book[symbol], time.time(), raw=data, sequence_number=data['id'])

    def _check_update_id(self, pair: str, msg: dict) -> bool:
        skip_update = False
        forced = not self.forced[pair]

        if forced and msg['u'] <= self.last_update_id[pair]:
            skip_update = True
        elif forced and msg['U'] <= self.last_update_id[pair] + 1 <= msg['u']:
            self.last_update_id[pair] = msg['u']
            self.forced[pair] = True
        elif not forced and self.last_update_id[pair] + 1 == msg['U']:
            self.last_update_id[pair] = msg['u']
        else:
            self._reset()
            LOG.warning("%s: Missing book update detected, resetting book", self.id)
            skip_update = True

        return skip_update

    async def _process_l2_book(self, msg: dict, timestamp: float):
        """
        {
          "time": 1615366381,
          "channel": "futures.order_book_update",
          "event": "update",
          "error": null,
          "result": {
            "t": 1615366381417,
            "s": "BTC_USD",
            "U": 2517661101,
            "u": 2517661113,
            "b": [
              {
                "p": "54672.1",
                "s": 0
              },
              {
                "p": "54664.5",
                "s": 58794
              }
            ],
            "a": [
              {
                "p": "54743.6",
                "s": 0
              },
              {
                "p": "54742",
                "s": 95
              }
            ]
          }
        }
        """
        symbol = self.exchange_symbol_to_std_symbol(msg['result']['s'])
        if symbol not in self._l2_book:
            await self._snapshot(msg['result']['s'])

        skip_update = self._check_update_id(symbol, msg['result'])
        if skip_update:
            return

        ts = msg['result']['t'] / 1000
        delta = {BID: [], ASK: []}

        for s, side in (('b', BID), ('a', ASK)):
            for update in msg['result'][s]:
                price = Decimal(update['p'])
                amount = Decimal(update['s'])

                if amount == 0:
                    if price in self._l2_book[symbol].book[side]:
                        del self._l2_book[symbol].book[side][price]
                        delta[side].append((price, amount))
                else:
                    self._l2_book[symbol].book[side][price] = amount
                    delta[side].append((price, amount))

        await self.book_callback(L2_BOOK, self._l2_book[symbol], timestamp, delta=delta, timestamp=ts, raw=msg)

    async def _candles(self, msg: dict, timestamp: float):
        """
        {
          "time": 1542162490,
          "channel": "futures.candlesticks",
          "event": "update",
          "error": null,
          "result": [
            {
              "t": 1545129300,
              "v": 27525555,
              "c": "95.4",
              "h": "96.9",
              "l": "89.5",
              "o": "94.3",
              "n": "1m_BTC_USD"
            },
            {
              "t": 1545129300,
              "v": 27525555,
              "c": "95.4",
              "h": "96.9",
              "l": "89.5",
              "o": "94.3",
              "n": "1m_BTC_USD"
            }
          ]
        }
        """
        result = msg['result'][0]
        interval, symbol = result['n'].split('_', 1)
        if interval == '7d':
            interval = '1w'
        c = Candle(
            self.id,
            self.exchange_symbol_to_std_symbol(symbol),
            float(result['t']),
            float(result['t']) + timedelta_str_to_sec(interval) - 0.1,
            interval,
            None,
            Decimal(result['o']),
            Decimal(result['c']),
            Decimal(result['h']),
            Decimal(result['l']),
            Decimal(result['v']),
            None,
            float(msg['time']),
            raw=msg
        )
        await self.callback(CANDLES, c, timestamp)

    async def message_handler(self, msg: str, conn, timestamp: float):
        msg = json.loads(msg, parse_float=Decimal)

        if "error" in msg:
            if msg['error'] is None:
                pass
            else:
                LOG.warning("%s: Error received from exchange - %s", self.id, msg)
        if msg['event'] == 'subscribe':
            return
        elif 'channel' in msg:
            market, channel = msg['channel'].split('.')
            if channel == 'tickers':
                await self._ticker(msg, timestamp)
            elif channel == 'trades':
                await self._trades(msg, timestamp)
            elif channel == 'order_book_update':
                await self._process_l2_book(msg, timestamp)
            elif channel == 'candlesticks':
                await self._candles(msg, timestamp)
            else:
                LOG.warning("%s: Unhandled message type %s", self.id, msg)
        else:
            LOG.warning("%s: Invalid message type %s", self.id, msg)

    async def subscribe(self, conn: AsyncConnection):
        self._reset()
        for chan in self.subscription:
            symbols = self.subscription[chan]
            nchan = self.exchange_channel_to_std(chan)
            if nchan in {L2_BOOK, CANDLES}:
                for symbol in symbols:
                    await conn.write(json.dumps(
                        {
                            "time": int(time.time()),
                            "channel": chan,
                            "event": 'subscribe',
                            "payload": [symbol, '100ms'] if nchan == L2_BOOK else [self.candle_interval, symbol],
                        }
                    ))
            else:
                await conn.write(json.dumps(
                    {
                        "time": int(time.time()),
                        "channel": chan,
                        "event": 'subscribe',
                        "payload": symbols,
                    }
                ))
