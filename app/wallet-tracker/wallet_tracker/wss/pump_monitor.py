"""
监听Pump新代币事件
监控Pump平台新创建的代币，并将消息推送至Redis
"""

import asyncio
from typing import Optional

import aioredis
from solana.rpc.websocket_api import connect
from solbot_common.config import settings
from solbot_common.log import logger
from solbot_common.constants import PUMP_FUN_PROGRAM
from solders.pubkey import Pubkey
from solders.rpc.responses import SubscriptionResult, ProgramNotification
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

from wallet_tracker.constants import NEW_PUMP_TOKEN_CHANNEL


class PumpMonitor:
    def __init__(
        self,
        rpc_endpoint: str,
        redis_client: aioredis.Redis,
        redis_channel: str = NEW_PUMP_TOKEN_CHANNEL,
    ):
        """
        初始化Pump监控器

        Args:
            rpc_endpoint: Solana RPC端点
            redis_client: Redis客户端
            redis_channel: Redis发布订阅频道名
        """
        logger.info(f"pump fun start...")
        self.websocket_url = rpc_endpoint.replace("https://", "wss://")
        self.redis_channel = redis_channel
        self.redis = redis_client
        self.is_running = False
        self.websocket = None
        self.subscription_id = None

    async def process_pump_event(self, message: ProgramNotification) -> None:
        """
        处理Pump新代币事件

        Args:
            message: WebSocket返回的程序通知数据
        """
        try:
            # 解析新代币事件
            account_data = message.result.value.account.data
            if account_data and hasattr(account_data, "parsed"):
                token_info = account_data.parsed.get("info", {})
                mint_address = token_info.get("mint")
                
                if mint_address:
                    token_address = str(mint_address)
                    await self.redis.lpush(self.redis_channel, token_address)
                    logger.info(f"New Pump token detected: {token_address}")
        except Exception as e:
            logger.error(f"Error processing Pump event: {e}")

    async def subscribe_pump(self) -> None:
        """订阅Pump.fun区块事件"""
        if not self.websocket:
            logger.warning("WebSocket connection is not established")
            return

        logger.debug("Subscribing to Pump.fun blocks")
        await self.websocket.block_subscribe(
            filter_="all",
            commitment=settings.rpc.commitment,
            encoding="base64",
            program=PUMP_FUN_PROGRAM  # 只监听Pump.fun程序的区块
        )

    async def process_block_event(self, message: ProgramNotification) -> None:
        """
        处理Pump.fun区块事件
        """
        try:
            # 解析区块中的Pump.fun交易
            block_data = message.result.value
            # 这里可以添加具体的区块数据处理逻辑
            logger.info(f"New Pump.fun block event: {block_data}")
            
            # 推送到Redis
            await self.redis.lpush(self.redis_channel, str(block_data))
        except Exception as e:
            logger.error(f"Error processing block event: {e}")

    async def start(self) -> None:
        """启动监控服务"""
        self.is_running = True
        retry_count = 0
        max_retries = 5
        base_delay = 5

        while self.is_running:
            try:
                async with connect(
                    self.websocket_url,
                    ping_timeout=30,
                    ping_interval=20,
                    close_timeout=20,
                ) as websocket:
                    self.websocket = websocket
                    logger.info(f"Connected to Solana WebSocket RPC: {self.websocket_url}")
                    
                    await self.subscribe_pump()

                    while self.is_running:
                        try:
                            messages = await websocket.recv()
                            for message in messages:
                                if isinstance(message, SubscriptionResult):
                                    self.subscription_id = message.result
                                    logger.info(f"Subscribed to blocks with ID: {self.subscription_id}")
                                elif isinstance(message, ProgramNotification):
                                    await self.process_block_event(message)
                        except (ConnectionClosedError, ConnectionClosedOK) as e:
                            logger.warning(f"WebSocket connection closed: {e}")
                            break
                        except Exception as e:
                            logger.error(f"Error processing message: {e}")
                            break
            except Exception as e:
                logger.error(f"Error in monitor loop: {e}")

            # 清理并准备重连
            await self.cleanup()

            # 指数退避重试
            retry_count += 1
            if retry_count > max_retries:
                logger.error(f"Maximum retries ({max_retries}) reached. Stopping monitor.")
                self.is_running = False
                break

            delay = min(base_delay * (2 ** (retry_count - 1)), 60)
            logger.info(f"Reconnecting in {delay} seconds (attempt {retry_count}/{max_retries})")
            await asyncio.sleep(delay)

    async def cleanup(self) -> None:
        """清理连接"""
        if self.websocket:
            await self.websocket.close()
            self.websocket = None
        if self.redis:
            await self.redis.close()

    async def stop(self) -> None:
        """停止监控服务"""
        self.is_running = False
        await self.cleanup()
        logger.info("Pump monitor stopped")