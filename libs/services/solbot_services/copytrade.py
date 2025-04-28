from collections.abc import Sequence

from solbot_common.models.tg_bot.copytrade import CopyTrade
from solbot_db.session import NEW_ASYNC_SESSION, provide_session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select


class CopyTradeService:
    @classmethod
    @provide_session
    async def get_by_target_wallet(
        cls, target_wallet: str, *, session: AsyncSession = NEW_ASYNC_SESSION
    ) -> list[CopyTrade]:
        """ "获取指定目标钱包的活跃跟单"""
        stmt = select(CopyTrade).where(
            CopyTrade.target_wallet == target_wallet
        ).where(CopyTrade.active == 1)
        results = await session.execute(stmt)
        return [row.model_copy() for row in results.scalars().all()]

    @classmethod
    @provide_session
    async def get_active_wallet_addresses(
        cls, *, session: AsyncSession = NEW_ASYNC_SESSION
    ) -> Sequence[str]:
        """获取所有已激活的目标钱包地址

        Returns:
            Sequence[str]: 已激活的目标钱包地址列表，去重后的结果
        """
        stmt = select(CopyTrade.target_wallet).where(CopyTrade.active == True).distinct()
        result = await session.execute(stmt)
        return result.scalars().all()
