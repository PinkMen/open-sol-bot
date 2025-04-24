from collections.abc import Sequence
from typing import Literal

from solbot_common.config import settings
from solbot_common.cp.monitor_events import MonitorEvent, MonitorEventConsumer, MonitorEventType
from solbot_common.log import logger
from solbot_common.models.tg_bot.monitor import Monitor
from solbot_db.redis import RedisClient
from solbot_services.copytrade import CopyTradeService
from solders.pubkey import Pubkey  # type: ignore

from .geyser.tx_subscriber import TransactionDetailSubscriber as GeyserMonitor
from .wss.tx_subscriber import TransactionDetailSubscriber as RPCMonitor
from .geyser.pump_subscriber import NewMintSubscriber as PumpMonitor
from solbot_common.constants import PUMP_FUN_PROGRAM

class TxMonitor:
    def __init__(
        self,
        wallets: Sequence[Pubkey],
        mode: Literal["wss", "geyser"] = "wss",
    ):
        self.mode = mode
        redis = RedisClient.get_instance()
        self.events = MonitorEventConsumer(redis)
        if mode == "wss":
            self.monitor = RPCMonitor(
                settings.rpc.rpc_url,
                redis,
                wallets,
            )
        elif mode == "geyser":
            self.monitor = GeyserMonitor(
                settings.rpc.geyser.endpoint,
                settings.rpc.geyser.api_key,
                redis,
                wallets,
            )
            
            self.pump_monitor = PumpMonitor(
                settings.rpc.geyser.endpoint,
                settings.rpc.geyser.api_key,
                redis,
                [PUMP_FUN_PROGRAM],
            )
        else:
            raise ValueError("Invalid mode")

    async def start(self):
        """启动监听器"""
        # 注册事件处理器
        self.events.register_handler(MonitorEventType.RESUME, self._handle_resume_event)
        self.events.register_handler(MonitorEventType.PAUSE, self._handle_pause_event)

        # 订阅事件
        pubsub = await self.events.subscribe()
        logger.info("Transaction monitor started")
        logger.info(f"Mode: {self.mode}")

        # 启动监听器
        await self.monitor.start()
        await self.pump_monitor.start()

        # 从数据库中获取已激活的目标地址
        monitor_addresses = await Monitor.get_active_wallet_addresses()
        copytrade_addresses = await CopyTradeService.get_active_wallet_addresses()
        # 合并两个列表
        active_wallet_addresses = list(set(list(monitor_addresses) + list(copytrade_addresses)))
        for address in active_wallet_addresses:
            await self.monitor.subscribe_wallet_transactions(Pubkey.from_string(address))
            logger.debug(f"Subscribed to wallet: {address}")

        await self.pump_monitor.subscribe_wallet_transactions(PUMP_FUN_PROGRAM)
        # 开始处理事件
        logger.info("Start processing monitor events")
        while True:
            try:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.5)
                if message is None:
                    continue
                await self.events.process_event(message)
            except Exception as e:
                logger.error(f"Error processing monitor event: {e}")

    async def stop(self):
        """停止监听器"""
        await self.events.unsubscribe()
        await self.monitor.stop()
        await self.pump_monitor.stop()

    async def _handle_resume_event(self, event: MonitorEvent):
        """处理恢复监听事件"""
        try:
            wallet = Pubkey.from_string(event.target_wallet)
            await self.monitor.subscribe_wallet_transactions(wallet)
            await self.pump_monitor.subscribe_wallet_transactions(wallet)
            logger.info(f"Resumed monitoring wallet: {wallet}")
        except Exception as e:
            logger.error(f"Failed to resume monitoring wallet {event.target_wallet}: {e}")
            raise

    async def _handle_pause_event(self, event: MonitorEvent):
        """处理暂停监听事件"""
        try:
            wallet = Pubkey.from_string(event.target_wallet)
            await self.monitor.unsubscribe_wallet_transactions(wallet)
            await self.pump_monitor.unsubscribe_wallet_transactions(wallet)
            logger.info(f"Paused monitoring wallet: {wallet}")
        except Exception as e:
            logger.error(f"Failed to pause monitoring wallet {event.target_wallet}: {e}")
            raise
