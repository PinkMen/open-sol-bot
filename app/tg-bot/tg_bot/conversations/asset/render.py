from aiogram.enums import ParseMode
from solbot_cache.wallet import WalletCache
from solbot_services.holding import HoldingService

from tg_bot.keyboards.asset import get_asset_keyboard
from tg_bot.templates import render_asset_message

holding_service = HoldingService()
wallet_cache = WalletCache()


async def render(wallet: str):
    # PERF: 这里后续需要优化，应该从数据库中获取，而不是调用 Shyft API
    # 在程序运行过程中，需要跟踪钱包的 Token 余额变化并及时更新到数据库中
    tokens = await holding_service.get_tokens(wallet, hidden_small_amount=True)
    sol_balance = await wallet_cache.get_sol_balance(wallet)

    text = render_asset_message(
        wallet=wallet,
        sol_balance=sol_balance,
        tokens=tokens[-50:],
    )

    return {
        "text": text,
        "parse_mode": ParseMode.HTML,
        "reply_markup": get_asset_keyboard(),
        "disable_web_page_preview": True,
    }
