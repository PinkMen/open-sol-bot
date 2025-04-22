import asyncio
from collections.abc import Sequence

import orjson as json
from aioredis import Redis
from aioredis.exceptions import RedisError
from solana.rpc.async_api import AsyncClient as Client
from solbot_common.config import settings
from solbot_common.log import logger
from solders.pubkey import Pubkey  # type: ignore
from solders.signature import Signature  # type: ignore

from wallet_tracker import benchmark
from wallet_tracker.constants import (
    FAILED_TX_SIGNATURE_CHANNEL,
    NEW_TX_DETAIL_CHANNEL,
    NEW_TX_SIGNATURE_CHANNEL,
)
from wallet_tracker.exceptions import NotSwapTransaction, TransactionError
from wallet_tracker.wss.tx_detail_fetcher import TxDetailRawFetcher

from .account_log_monitor import AccountLogMonitor
from .pump_monitor import PumpMonitor

class TransactionDetailSubscriber:
    """
    交易详情订阅者
    """

    def __init__(
        self,
        rpc_endpoint: str,
        redis_client: Redis,
        wallets: Sequence[Pubkey],
    ):
        self.wallets = wallets
        self.rpc_endpoint = rpc_endpoint
        self.redis = redis_client
        self.rpc_client: Client | None = None
        self.is_running = False
        self.fetchers = [
            (f"Raw-{i}", TxDetailRawFetcher(endpoint).fetch)
            for i, endpoint in enumerate(settings.rpc.endpoints)
        ]
        self.lock = asyncio.Lock()
        self.account_log_monitor = AccountLogMonitor(
            self.wallets,
            settings.rpc.rpc_url,
            self.redis,
        )
        self.pump_monitor= PumpMonitor(
            settings.rpc.rpc_url,
            self.redis,
        )

    async def fetch_transaction_detail(self, tx_sig: str) -> dict | None:
        tasks = []
        for fetcher_name, fetcher in self.fetchers:
            tasks.append(self._fetch_with_name(fetcher_name, fetcher, tx_sig))

        try:
            remaining = set(tasks)
            while remaining:
                done, remaining = await asyncio.wait(remaining, return_when=asyncio.FIRST_COMPLETED)

                # 检查完成的任务结果
                for task in done:
                    try:
                        result = task.result()
                        if result is not None:
                            # 找到有效结果，取消剩余任务
                            for t in remaining:
                                t.cancel()
                            return result
                    except Exception:
                        continue

            # 所有任务都完成但没有找到有效结果
            logger.error(f"Transaction not found: {tx_sig}")
            return None

        except Exception as e:
            logger.error(f"Failed to fetch transaction: {e}")
            logger.exception(e)
            # 发生异常时取消所有未完成的任务
            for task in tasks:
                if not task.done():
                    task.cancel()
            return None

    async def _fetch_with_name(self, fetcher_name: str, fetcher, tx_sig: str) -> dict | None:
        try:
            logger.info(f"Fetching transaction from {fetcher_name}")
            tx_detail = await fetcher(Signature.from_string(tx_sig))
            if tx_detail is not None:
                logger.info(f"Successfully fetched transaction from {fetcher_name}")
                return tx_detail
            logger.warning(f"Failed to fetch transaction from {fetcher_name}")
            return None
        except Exception as e:
            logger.error(f"Error fetching from {fetcher_name}: {e}")
            logger.exception(e)
            return None

    async def push_transaction_to_redis(self, tx_detail: str):
        assert self.redis is not None
        await self.redis.lpush(NEW_TX_DETAIL_CHANNEL, tx_detail)

    async def push_failed_transaction_to_redis(self, tx_detail: str):
        assert self.redis is not None
        await self.redis.lpush(FAILED_TX_SIGNATURE_CHANNEL, tx_detail)

    async def process_transaction(self, tx_sig: str):
        """处理单个交易"""
        async with benchmark.with_fetch_tx(tx_sig):
            tx_detail = await self.fetch_transaction_detail(tx_sig)
        if tx_detail is None:
            logger.error(f"Failed to fetch transaction: {tx_sig}")
            # 加入到失败队列
            await self.push_failed_transaction_to_redis(tx_sig)
            return

        # 使用 orjson 的 dumps，它返回 bytes，需要解码为 str
        tx_detail_text = json.dumps(tx_detail).decode("utf-8")
        try:
            await self.push_transaction_to_redis(tx_detail_text)
            logger.success(f"New tx event: {tx_sig}")
        except TransactionError as e:
            logger.info(f"Transaction status is not valid, status: {e}")
        except NotSwapTransaction:
            logger.info(f"Tx is not swap transaction, details: {tx_sig}")
            return
        except Exception as e:
            logger.error(f"Failed to process transaction: {e}, details: {tx_detail_text}")
            logger.exception(e)
            # 加入到失败队列
            await self.push_failed_transaction_to_redis(tx_detail_text)
        finally:
            await benchmark.show_timeline(tx_sig)

    async def worker(self):
        """单个 worker 协程"""
        while True:
            try:
                assert self.redis is not None
                async with self.lock:
                    result = await self.redis.brpop(NEW_TX_SIGNATURE_CHANNEL, timeout=1)
                if result is None:
                    continue
                _, tx_sig = result
                logger.info(f"Received tx signature: {tx_sig}")
                await self.process_transaction(tx_sig)
            except RedisError as e:
                logger.error(f"Failed to push transaction to Redis: {e}")
                # await self.connect_redis()
                continue
            except Exception as e:
                logger.error(f"Worker error: {e}")
                logger.exception(e)
                continue

    async def start(self, num_workers: int = 2):
        """启动多个 worker 协程并行处理消息"""
        self.is_running = True

        # 启动 worker
        self.workers = [asyncio.create_task(self.worker()) for _ in range(num_workers)]

        async def _f():
            try:
                await self.pump_monitor.start()
                # 启动日志订阅
                await self.account_log_monitor.start()
                
                await asyncio.gather(*self.workers)
            except Exception as e:
                logger.error(f"Worker pool error: {e}")
                logger.exception(e)
                for worker in self.workers:
                    worker.cancel()

        monitor_task = asyncio.create_task(_f())
        # 添加任务完成回调以处理可能的异常
        monitor_task.add_done_callback(lambda t: t.exception() if t.exception() else None)

    async def stop(self) -> None:
        """Stop the wallet monitor gracefully."""
        self.is_running = False
        for worker in self.workers:
            worker.cancel()

    async def subscribe_wallet_transactions(self, wallet: Pubkey) -> None:
        """订阅钱包的交易信息。

        Args:
            wallet (Pubkey): 要订阅的钱包地址
        """
        await self.account_log_monitor.waitting_subscribe_wallet.put(wallet)

    async def unsubscribe_wallet_transactions(self, wallet: Pubkey) -> None:
        """取消订阅钱包的交易信息。

        Args:
            wallet (Pubkey): 要取消订阅的钱包地址
        """
        await self.account_log_monitor.waitting_unsubscribe_wallet.put(wallet)
