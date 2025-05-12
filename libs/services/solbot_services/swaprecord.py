from collections.abc import Sequence

from solbot_common.models.swap_record import TransactionStatus ,SwapRecord
from solbot_db.session import NEW_ASYNC_SESSION, provide_session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select


class SwapRecordService:

    @classmethod
    @provide_session
    async def get_active_mint_addresses(
        cls, *, session: AsyncSession = NEW_ASYNC_SESSION
    ) -> Sequence[str]:
        """获取所有已激活的目标钱包地址

        Returns:
            Sequence[str]: 已激活的目标钱包地址列表，去重后的结果
        """
        stmt = select(SwapRecord.output_mint).where(SwapRecord.status == TransactionStatus.SUCCESS).distinct()
        result = await session.execute(stmt)
        return result.scalars().all()

    @classmethod
    @provide_session
    async def get_mint(
        cls, mint:str ,session :AsyncSession
    ) -> SwapRecord | None:
        stmt = select(SwapRecord).where(SwapRecord.status == TransactionStatus.SUCCESS and SwapRecord.output_mint == mint).limit(1)
        result = await session.execute(stmt)
        return result.scalar_one_or_none()
