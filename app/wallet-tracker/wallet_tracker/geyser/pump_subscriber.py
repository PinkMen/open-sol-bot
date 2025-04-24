import asyncio
import signal
import time
from collections.abc import AsyncGenerator, Sequence

import aioredis
import base58
import orjson as json
from google.protobuf.json_format import (
    Parse,
    _Printer,  # type: ignore
)
from google.protobuf.message import Message
from grpc.aio import AioRpcError
from solbot_common.config import settings
from solbot_common.log import logger
from solbot_db.redis import RedisClient
from solders.pubkey import Pubkey  # type: ignore
from yellowstone_grpc.client import GeyserClient
from yellowstone_grpc.grpc import geyser_pb2
from yellowstone_grpc.types import (
    SubscribeRequest,
    SubscribeRequestFilterTransactions,
    SubscribeRequestPing,
    CommitmentLevel,
)

from wallet_tracker.constants import NEW_MINT_DETAIL_CHANNEL
from .tx_subscriber import TransactionDetailSubscriber

def should_convert_to_base58(value) -> bool:
    """Check if bytes should be converted to base58."""
    if not isinstance(value, bytes):
        return False
    try:
        # 尝试解码为字符串，如果成功且没有特殊字符，就用字符串
        decoded = value.decode("utf-8")
        # 检查是否包含转义字符或不可打印字符
        return "\\" in decoded or any(ord(c) < 32 or ord(c) > 126 for c in decoded)
    except UnicodeDecodeError:
        # 如果无法解码为字符串，就用 base58
        return True


class Base58Printer(_Printer):
    def __init__(self) -> None:
        super().__init__()
        self.preserve_proto_field_names = True

    def _RenderBytes(self, value):
        """Renders a bytes value as base58 or utf-8 string."""
        if should_convert_to_base58(value):
            return base58.b58encode(value).decode("utf-8")
        return value.decode("utf-8")

    def _FieldToJsonObject(self, field, value):
        """Converts field value according to its type."""
        if field.cpp_type == field.CPPTYPE_BYTES and isinstance(value, bytes):
            if should_convert_to_base58(value):
                return base58.b58encode(value).decode("utf-8")
            return value.decode("utf-8")
        return super()._FieldToJsonObject(field, value)


def proto_to_dict(message: Message) -> dict:
    """Convert protobuf message to dict with bytes fields encoded as base58 or utf-8."""
    printer = Base58Printer()
    return printer._MessageToJsonObject(message)


class NewMintSubscriber(TransactionDetailSubscriber):
    def __init__(
        self,
        endpoint: str,
        api_key: str,
        redis_client: aioredis.Redis,
        wallets: Sequence[Pubkey],
    ):
        super().__init__(endpoint, api_key, redis_client, wallets)
        self.channel = NEW_MINT_DETAIL_CHANNEL
    
    def __build_subscribe_request(self) -> SubscribeRequest:
        logger.info(f"Subscribing to accounts: {self.subscribed_wallets}")

        params = {}

        if len(self.subscribed_wallets) != 0:
            params["transactions"] = {
                "pump_subscription": SubscribeRequestFilterTransactions(
                    account_include=list(self.subscribed_wallets),
                    failed=False,
                    vote=False
                )
            }
            params['commitment'] = CommitmentLevel.PROCESSED
        else:
            params["ping"] = SubscribeRequestPing(id=1)

        subscribe_request = SubscribeRequest(**params)
        return subscribe_request

    async def _process_response_worker(self):
        """Process responses from the queue."""
        logger.info(f"Starting response worker {id(asyncio.current_task())}")
        while self.is_running:
            try:
                response = await self.response_queue.get()
                try:
                    response_dict = proto_to_dict(response)
                    if "ping" in response_dict:
                        logger.debug(f"Got ping response: {response_dict}")
                    if "filters" in response_dict and "transaction" in response_dict:
                        if any('InitializeMint2' in str(msg) for msg in response_dict.get('transaction', {}).get('transaction',{}).get('meta', {}).get('logMessages', [])):
                            logger.debug(f"Got transaction response: \n {response_dict}")
                            await self._process_transaction(response_dict['transaction'])
                        
                except Exception as e:
                    logger.error(f"Error processing response: {e}")
                    logger.exception(e)
                finally:
                    self.response_queue.task_done()
            except asyncio.CancelledError:
                logger.info(f"Worker {id(asyncio.current_task())} cancelled")
                break
            except Exception as e:
                logger.exception(f"Worker error: {e}")

 

if __name__ == "__main__":
    from solbot_db.redis import RedisClient

    async def main():
        wallets = [
            "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
        ]
        endpoint = settings.rpc.geyser.endpoint
        api_key = settings.rpc.geyser.api_key
        monitor = NewMintSubscriber(
            endpoint,
            api_key,
            RedisClient.get_instance(),
            [Pubkey.from_string(wallet) for wallet in wallets],
        )

        # 设置信号处理
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(monitor.stop()))

        try:
            await monitor.start()
        except asyncio.CancelledError:
            logger.info("Monitor cancelled, shutting down...")
            await monitor.stop()
        except Exception as e:
            logger.error(f"Error in main: {e}")
            await monitor.stop()
            raise

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Program interrupted by user")
