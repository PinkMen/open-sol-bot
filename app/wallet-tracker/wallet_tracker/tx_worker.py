import asyncio

import aioredis
import orjson as json
from aioredis.exceptions import RedisError
from solbot_common.cp.tx_event import TxEventProducer
from solbot_common.log import logger

from wallet_tracker import benchmark
from wallet_tracker.constants import FAILED_TX_DETAIL_CHANNEL, NEW_TX_DETAIL_CHANNEL
from wallet_tracker.exceptions import (
    NotSwapTransaction,
    TransactionError,
    UnknownTransactionType,
    ZeroChangeAmountError,
)
from wallet_tracker.parser import RawTXParser


class TransactionWorker:
    """
    交易处理工作类

    从 Redis 中获取交易签名，并根据签名查询交易详情，需要识别出交易类型
    - 建仓
    - 加仓
    - 减仓
    - 清仓
    """

    def __init__(self, redis: aioredis.Redis):
        self.redis: aioredis.Redis = redis
        self.is_running = False
        self.lock = asyncio.Lock()
        self.tx_event_producer = TxEventProducer(redis)

    async def push_parse_failed_to_redis(self, tx_event: str):
        """解析失败的交易详情放入失败队列"""
        assert self.redis is not None
        await self.redis.lpush(FAILED_TX_DETAIL_CHANNEL, tx_event)

    async def process_transaction(self, tx_detail: dict):
        """处理单个交易"""
        tx_parser = RawTXParser(tx_detail)
        tx_hash = tx_parser.get_tx_hash()

        # 使用 orjson 的 dumps，它返回 bytes，需要解码为 str
        tx_detail_text = json.dumps(tx_detail).decode("utf-8")
        try:
            block_time = tx_parser.get_block_time()
            await benchmark.record_block_time(tx_hash, block_time)

            async with benchmark.with_parse_tx(tx_hash):
                tx_event = tx_parser.parse()

            # FIXME: 解析失败，该如何处理, 后续需要对失败队列加入监控并发出警报
            if tx_event is None:
                logger.error(f"Parse tx failed, details: {tx_detail_text}")
                # 加入到失败队列
                await self.push_parse_failed_to_redis(tx_detail_text)
                return
            await self.tx_event_producer.produce(tx_event)
            logger.success(f"New tx event: {tx_hash}")
        except TransactionError as e:
            logger.info(f"Transaction status is not valid, status: {e}")
        except NotSwapTransaction:
            logger.info(f"Tx is not swap transaction, details: {tx_hash}")
            return
        except UnknownTransactionType:
            logger.info(f"Tx type is not valid, details: {tx_hash}")
            return
        except ZeroChangeAmountError:
            logger.info(f"Tx amount is zero, details: {tx_hash}")
            return
        except Exception as e:
            logger.error(f"Failed to process transaction: {e}, details: {tx_detail_text}")
            logger.exception(e)
            # 加入到失败队列
            await self.push_parse_failed_to_redis(tx_detail_text)
        # finally:
        #     await benchmark.show_timeline(tx_hash)

    async def worker(self):
        """单个 worker 协程"""
        while self.is_running:
            try:
                assert self.redis is not None
                async with self.lock:
                    result = await self.redis.brpop(NEW_TX_DETAIL_CHANNEL, timeout=1)
                    if result is None:  # timeout occurred
                        continue
                _, tx_detail = result
                json_data = json.loads(tx_detail)
                await self.process_transaction(json_data)
            except RedisError as e:
                logger.error(f"Failed to push transaction to Redis: {e}")
                continue
            except asyncio.CancelledError:
                logger.info("Worker task cancelled")
                break
            except Exception as e:
                logger.error(f"Worker error: {e}")
                logger.exception(e)
                continue

    async def start(self, num_workers: int = 2):
        """启动多个 worker 协程并行处理消息"""
        self.is_running = True
        self.workers = [asyncio.create_task(self.worker()) for _ in range(num_workers)]
        try:
            await asyncio.gather(*self.workers)
        except asyncio.CancelledError:
            logger.info("Worker pool cancelled")
        except Exception as e:
            logger.error(f"Worker pool error: {e}")
            logger.exception(e)
        finally:
            self.is_running = False
            for worker in self.workers:
                if not worker.done():
                    worker.cancel()
            await asyncio.gather(*self.workers, return_exceptions=True)

    async def stop(self) -> None:
        """Stop the wallet monitor gracefully."""
        self.is_running = False
        for worker in self.workers:
            worker.cancel()
