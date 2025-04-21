from aiogram.fsm.state import State, StatesGroup


class StartStates(StatesGroup):
    WAITING_FOR_ACTIVATION_CODE = State()


class CopyTradeStates(StatesGroup):
    """Copy trade states"""

    MENU = State()  # 主菜单状态

    # 创建跟单相关状态
    CREATING = State()  # 创建跟单状态
    CREATE_WAITING_FOR_ADDRESS = State()  # 创建时等待输入钱包地址
    CREATE_WAITING_FOR_ALIAS = State()  # 创建时等待输入别名
    CREATE_WAITING_FOR_FIXED_BUY_AMOUNT = State()  # 创建时等待输入固定买入数量
    CREATE_WAITING_FOR_PRIORITY = State()  # 创建时等待输入优先费用
    CREATE_WAITING_FOR_CUSTOM_SLIPPAGE = State()  # 创建时等待输入自定义滑点

    # 编辑跟单相关状态
    EDITING = State()  # 编辑跟单状态
    EDIT_WAITING_FOR_ADDRESS = State()  # 编辑时等待输入钱包地址
    EDIT_WAITING_FOR_ALIAS = State()  # 编辑时等待输入别名
    EDIT_WAITING_FOR_FIXED_BUY_AMOUNT = State()  # 编辑时等待输入固定买入数量
    EDIT_WAITING_FOR_PRIORITY = State()  # 编辑时等待输入优先费用
    EDIT_WAITING_FOR_CUSTOM_SLIPPAGE = State()  # 编辑时等待输入自定义滑点


class MonitorStates(StatesGroup):
    MENU = State()
    CREATING = State()  # 创建跟单状态
    CREATING_PUMP = State()  # 创建跟单状态
    CREATE_WAITING_FOR_ADDRESS = State()  # 创建时等待输入钱包地址
    CREATE_WAITING_FOR_ALIAS = State()  # 创建时等待输入别名

    EDITING = State()  # 编辑跟单状态
    EDIT_WAITING_FOR_ALIAS = State()  # 编辑时等待输入别名


class SettingStates(StatesGroup):
    EDIT_QUICK_SLIPPAGE = State()
    WAITING_FOR_QUICK_SLIPPAGE = State()  # 等待输入快速滑点
    WAITING_FOR_SANDWICH_SLIPPAGE = State()  # 等待输入防夹滑点
    WAITING_FOR_BUY_PRIORITY_FEE = State()  # 等待输入买入优先费
    WAITING_FOR_SELL_PRIORITY_FEE = State()  # 等待输入卖出优先费
    WAITING_FOR_CUSTOM_BUY_AMOUNT = State()  # 等待输入自定义买入数量
    WAITING_FOR_CUSTOM_SELL_PCT = State()  # 等待输入自定义卖出


class SwapStates(StatesGroup):
    MENU = State()
    WAITING_FOR_TOKEN_MINT = State()
    WAITING_BUY_AMOUNT = State()
    WAITING_SELL_PCT = State()


class WalletStates(StatesGroup):
    MENU = State()
    WAITING_FOR_NEW_PRIVATE_KEY = State()
