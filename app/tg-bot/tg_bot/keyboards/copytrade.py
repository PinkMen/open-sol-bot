from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from solbot_common.types.copytrade import CopyTrade, CopyTradeSummary

from tg_bot.utils import short_text


def copytrade_keyboard_menu(
    copytrade_items: list[CopyTradeSummary] | None = None,
) -> InlineKeyboardMarkup:
    if copytrade_items is None:
        copytrade_items = []

    items = []
    for item in copytrade_items:
        alias = item.wallet_alias
        if alias is not None:
            show_name = short_text(alias, max_length=10)
        else:
            show_name = short_text(item.target_wallet, max_length=10)

        items.append(
            [
                InlineKeyboardButton(
                    text="{} 跟单地址：{}".format("🟢" if item.active else "🔴", show_name),
                    callback_data=f"copytrade_{item.pk}",
                )
            ]
        )

    if len(items) != 0:
        items.append(
            [
                InlineKeyboardButton(text="停止全部跟单", callback_data="stop_all_copytrade"),
            ]
        )

    buttoms = [
        InlineKeyboardButton(text="➕ 创建跟单", callback_data="create_copytrade"),
        InlineKeyboardButton(text="🔄 刷新", callback_data="refresh_copytrade"),
        InlineKeyboardButton(text="⬅️ 返回", callback_data="back_to_home"),
    ]

    return InlineKeyboardMarkup(
        inline_keyboard=[
            *items,
            buttoms,
        ]
    )


def create_copytrade_keyboard(udata: CopyTrade) -> InlineKeyboardMarkup:
    """Create keyboard for copytrade settings"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=(
                        "请输入跟单地址"
                        if udata.target_wallet is None
                        else str(udata.target_wallet)
                    ),
                    callback_data="set_address",
                ),
            ],
            [
                InlineKeyboardButton(
                    text=(
                        "请输入钱包别名（选填）"
                        if udata.wallet_alias is None
                        else f"钱包别名：{udata.wallet_alias}"
                    ),
                    callback_data="set_wallet_alias",
                )
            ],
            [
                InlineKeyboardButton(
                    text="{} 固定买入: {} SOL".format(
                        "✅" if udata.is_fixed_buy else "",
                        udata.fixed_buy_amount,
                    ),
                    callback_data="set_fixed_buy_amount",
                )
            ],
            [
                InlineKeyboardButton(
                    text="{} 自动跟买/卖".format(
                        "✅" if udata.auto_follow else "",
                    ),
                    callback_data="toggle_auto_follow",
                ),
                # InlineKeyboardButton(
                #     text="{} 止盈止损".format(
                #         "✅" if udata.stop_loss else "",
                #     ),
                #     callback_data="toggle_take_profile_and_stop_loss",
                # ),
                InlineKeyboardButton(
                    text="设置止盈比例",
                    callback_data="set_take_profit_ratio",
                ),
                InlineKeyboardButton(
                    text="设置止损比例",
                    callback_data="set_stop_loss_ratio",
                ),
                InlineKeyboardButton(
                    text="{} 只跟买入".format(
                        "✅" if udata.no_sell else "",
                    ),
                    callback_data="toggle_no_sell",
                ),
            ],
            [
                InlineKeyboardButton(
                    text=f"优先费: {udata.priority} SOL",
                    callback_data="set_priority",
                ),
                InlineKeyboardButton(
                    text="{} 防夹: {}".format(
                        "✅" if udata.anti_sandwich else "❌",
                        "开" if udata.anti_sandwich else "关",
                    ),
                    callback_data="toggle_anti_sandwich",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="{} 自动滑点".format(
                        "✅" if udata.auto_slippage else "",
                    ),
                    callback_data="toggle_auto_slippage",
                ),
                InlineKeyboardButton(
                    text="{} 自定义滑点: {}%".format(
                        "✅" if udata.auto_slippage is False else "",
                        udata.custom_slippage,
                    ),
                    callback_data="set_custom_slippage",
                ),
            ],
            [
                InlineKeyboardButton(text="⬅️ 取消", callback_data="back_to_copytrade"),
                InlineKeyboardButton(text="✅ 确认创建", callback_data="submit_copytrade"),
            ],
        ],
    )


def edit_copytrade_keyboard(udata: CopyTrade) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=(
                        "请输入跟单地址"
                        if udata.target_wallet is None
                        else str(udata.target_wallet)
                    ),
                    callback_data="null",
                ),
            ],
            [
                InlineKeyboardButton(
                    text=(
                        "请输入钱包别名（选填）"
                        if udata.wallet_alias is None
                        else f"钱包别名：{udata.wallet_alias}"
                    ),
                    callback_data="set_wallet_alias",
                )
            ],
            [
                InlineKeyboardButton(
                    text="{} 固定买入: {} SOL".format(
                        "✅" if udata.is_fixed_buy else "",
                        udata.fixed_buy_amount,
                    ),
                    callback_data="set_fixed_buy_amount",
                )
            ],
            [
                InlineKeyboardButton(
                    text="{} 自动跟卖".format(
                        "✅" if udata.auto_follow else "",
                    ),
                    callback_data="toggle_auto_follow",
                ),
                InlineKeyboardButton(
                    text="{} 止盈止损".format(
                        "✅" if udata.stop_loss else "",
                    ),
                    callback_data="toggle_take_profile_and_stop_loss",
                ),
                InlineKeyboardButton(
                    text="设置止盈比例",
                    callback_data="set_take_profit_ratio",
                ),
                InlineKeyboardButton(
                    text="设置止损比例",
                    callback_data="set_stop_loss_ratio",
                ),
                InlineKeyboardButton(
                    text="{} 只跟买入".format(
                        "✅" if udata.no_sell else "",
                    ),
                    callback_data="toggle_no_sell",
                ),
            ],
            [
                InlineKeyboardButton(
                    text=f"优先费: {udata.priority} SOL",
                    callback_data="set_priority",
                ),
                InlineKeyboardButton(
                    text="{} 防夹: {}".format(
                        "✅" if udata.anti_sandwich else "❌",
                        "开" if udata.anti_sandwich else "关",
                    ),
                    callback_data="toggle_anti_sandwich",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="{} 自动滑点".format(
                        "✅" if udata.auto_slippage else "",
                    ),
                    callback_data="toggle_auto_slippage",
                ),
                InlineKeyboardButton(
                    text="{} 自定义滑点: {}%".format(
                        "✅" if udata.auto_slippage is False else "",
                        udata.custom_slippage,
                    ),
                    callback_data="set_custom_slippage",
                ),
            ],
            [
                InlineKeyboardButton(text="删除跟单", callback_data="delete_copytrade"),
                InlineKeyboardButton(
                    text="停止跟单" if udata.active is True else "启动跟单",
                    callback_data="toggle_copytrade",
                ),
            ],
            [
                InlineKeyboardButton(text="⬅️ 返回", callback_data="back_to_copytrade"),
            ],
        ],
    )


def take_profile_and_stop_loss_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="设置止盈止损", callback_data="set_tp_sl"),
            ],
            [
                InlineKeyboardButton(text="移动止盈止损", callback_data="move_tp_sl"),
            ],
            [
                InlineKeyboardButton(text="⬅️ 返回", callback_data="back_to_copytrade"),
                InlineKeyboardButton(text="✅ 确认", callback_data="submit_copytrade"),
            ],
        ],
    )
