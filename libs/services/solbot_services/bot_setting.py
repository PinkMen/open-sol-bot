from solbot_common.types.bot_setting import BotSetting
from solbot_db.redis import RedisClient
from typing_extensions import Self


class BotSettingService:
    _instance = None

    def __new__(cls) -> Self:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        self.redis = RedisClient.get_instance()
        self.channel = "setting"

    async def get(self, chat_id: int, wallet_address: str) -> BotSetting | None:
        data = await self.redis.get(f"setting:{chat_id}:{wallet_address}")
        if data is None:
            await self.create_default(chat_id, wallet_address)
            return BotSetting(
                wallet_address=wallet_address,
                chat_id=chat_id,
            )
        return BotSetting.from_json(data)

    async def set(self, setting: BotSetting):
        key = f"setting:{setting.chat_id}:{setting.wallet_address}"
        await self.redis.set(key, setting.to_json())

    async def create_default(self, chat_id: int, wallet_address: str):
        setting = BotSetting(
            wallet_address=wallet_address,
            chat_id=chat_id,
        )
        await self.set(setting)
