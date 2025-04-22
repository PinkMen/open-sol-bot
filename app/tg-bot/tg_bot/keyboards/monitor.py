from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from tg_bot.models.monitor import Monitor


def monitor_keyboard_menu(
    monitor_items: list[Monitor] | None = None,
) -> InlineKeyboardMarkup:
    if monitor_items is None:
        monitor_items = []

    # 构建监听项目的按钮矩阵
    items = []
    current_row = []

    for idx, item in enumerate(monitor_items[:10], 1):  # 限制最多显示10个
        if item.wallet_alias is None:
            assert item.target_wallet is not None
            wallet_name = item.target_wallet[:5] + "..."
        else:
            wallet_name = item.wallet_alias
        current_row.append(
            InlineKeyboardButton(
                text=f"{idx!s} - {wallet_name}",
                callback_data=f"monitor_{item.pk}",
            )
        )

        # 每5个按钮换一行，或者是最后一个按钮
        if len(current_row) == 5 or idx == len(monitor_items):
            items.append(current_row)
            current_row = []

    # 如果最后一行不满5个，也要添加进去
    if current_row:
        items.append(current_row)

    # 停止所有监听
    items.append(
        [
            InlineKeyboardButton(
                text="停止所有监听",
                callback_data="stop_all_monitor",
            )
        ]
    )

    # 底部按钮
    bottom_buttons = [
        InlineKeyboardButton(text="➕ 新增", callback_data="create_new_monitor"),
        InlineKeyboardButton(text="⬅️ 返回", callback_data="back_to_home"),
    ]

    return InlineKeyboardMarkup(
        inline_keyboard=[
            *items,  # 展开监听项目按钮矩阵
            bottom_buttons,  # 底部按钮
        ],
    )


def create_monitor_keyboard(monitor: Monitor) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=(
                        "请输入钱包地址"
                        if monitor.target_wallet is None
                        else str(monitor.target_wallet)
                    ),
                    callback_data="set_address",
                ),
            ],
            [
                InlineKeyboardButton(
                    text=(
                        "钱包别名(可选)"
                        if monitor.wallet_alias is None
                        else f"钱包别名：{monitor.wallet_alias}"
                    ),
                    callback_data="set_wallet_alias",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ 返回",
                    callback_data="back_to_monitor",
                ),
                InlineKeyboardButton(
                    text="✅ 确认",
                    callback_data="submit_monitor",
                ),
            ],
        ]
    )


def edit_monitor_keyboard(monitor: Monitor) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=(
                        "请输入钱包地址"
                        if monitor.target_wallet is None
                        else str(monitor.target_wallet)
                    ),
                    callback_data="set_address",
                ),
            ],
            [
                InlineKeyboardButton(
                    text=(
                        "钱包别名(可选)"
                        if monitor.wallet_alias is None
                        else f"钱包别名：{monitor.wallet_alias}"
                    ),
                    callback_data="set_wallet_alias",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="删除监听",
                    callback_data="delete_monitor",
                ),
                InlineKeyboardButton(
                    text="停止监听" if monitor.active else "开启监听",
                    callback_data="toggle_monitor",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ 返回",
                    callback_data="back_to_monitor",
                ),
            ],
        ]
    )
