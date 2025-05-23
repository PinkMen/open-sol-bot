from collections.abc import Sequence

from solbot_common.models.swap_record import TransactionStatus ,SwapRecord
from solbot_db.session import NEW_ASYNC_SESSION, provide_session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select
from solbot_common.log import logger
from solders.pubkey import Pubkey  # type: ignore

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
        mint = Pubkey.from_string(mint)
        mint_str = mint.__str__()
        stmt = select(SwapRecord).where(SwapRecord.status == TransactionStatus.SUCCESS, SwapRecord.output_mint == mint_str).limit(1)
        logger.info(f"mint: {mint} {mint_str} , stmt: {stmt}")
        result = await session.execute(stmt)
        return result.scalar_one_or_none()
