from functools import cache

from solbot_common.log import logger

import orjson as json
from solbot_common.constants import SWAP_PROGRAMS, TOKEN_PROGRAM_ID, WSOL ,PUMP_FUN_PROGRAM_ID
from solbot_common.types import SolAmountChange, TokenAmountChange, TxEvent, TxType
from solbot_common.utils.utils import get_bonding_curve_account
from wallet_tracker.exceptions import (
    NotSwapTransaction,
    UnknownTransactionType,
    ZeroChangeAmountError,
)

from .protocol import TransactionParserInterface
from solbot_common.utils.utils import get_async_client
from solbot_common.constants import (
    PUMP_FUN_PROGRAM
)
from solbot_services.swaprecord import SwapRecordService
from solders.pubkey import Pubkey  # type: ignore
class PumpfunNewMintParser(TransactionParserInterface):
    def __init__(self, tx_detail: dict) -> None:
        self.tx_detail = tx_detail

    @classmethod
    def from_json(cls, tx_detail: str) -> "PumpfunNewMintParser":
        return cls(json.loads(tx_detail))

    @cache
    def get_block_time(self) -> int:
        return self.tx_detail["blockTime"]

    @cache
    def get_tx_hash(self) -> str:
        txs = self.tx_detail["transaction"]["signatures"]
        return txs[0]

    @cache
    def get_who(self) -> str:
        account_keys = self.tx_detail["transaction"]["message"]["accountKeys"]
        signer = account_keys[0]
        if isinstance(signer, str):
            return signer
        return signer["pubkey"]

    @cache
    def get_mint(self) -> str:
        token_post_balances = self.tx_detail["meta"]["postTokenBalances"]
        for token_post_balance in token_post_balances:
            if token_post_balance["owner"] != self.get_who():
                continue
            if token_post_balance["programId"] == str(TOKEN_PROGRAM_ID) and token_post_balance[
                "mint"
            ] != str(WSOL):
                return token_post_balance["mint"]
        raise ValueError("mint not found")       

    @cache
    def get_token_amount_change(self) -> TokenAmountChange:
        post_token_balances = self.tx_detail["meta"]["postTokenBalances"]
        who = self.get_who()
        mint = self.get_mint()

        pre_token_amount = 0
        post_token_amount = 0
        decimals = 6

        for post_token_balance in post_token_balances:
            if post_token_balance["mint"] == mint and post_token_balance["owner"] == who:
                post_token_amount = int(post_token_balance["uiTokenAmount"]["amount"])
                decimals = post_token_balance["uiTokenAmount"]["decimals"]
                break

        if "preTokenBalances" in self.tx_detail["meta"]:
            for pre_token_balance in self.tx_detail["meta"]["preTokenBalances"]:
                if pre_token_balance["mint"] == mint and pre_token_balance["owner"] == who:
                    pre_token_amount = int(pre_token_balance["uiTokenAmount"]["amount"])
                    break

        return {
            "change_amount": post_token_amount - pre_token_amount,
            "decimals": decimals,
            "pre_balance": pre_token_amount,
            "post_balance": post_token_amount,
        }

    @cache
    def get_sol_amount_change(self) -> SolAmountChange:
        pre_balances = self.tx_detail["meta"]["preBalances"]
        post_balances = self.tx_detail["meta"]["postBalances"]
        try:
            pre_sol_balance = int(pre_balances[0])
            post_sol_balance = int(post_balances[0])
        except IndexError:
            raise ValueError("owner index out of range")
        return {
            "change_amount": post_sol_balance - pre_sol_balance,
            "decimals": 9,
            "pre_balance": pre_sol_balance,
            "post_balance": post_sol_balance,
        }

    @cache
    def get_tx_type(self) -> TxType:
        # 检查是否是开仓或清仓交易
        logmessages = self.tx_detail["meta"]["logMessages"]
        return TxType.OPEN_POSITION if any('InitializeMint2' in str(msg) for msg in logmessages) else TxType.CLOSE_POSITION
        # change_ui_amount = token_amount_change["change_amount"] / (
        #     10 ** token_amount_change["decimals"]
        # )
        # pre_balance = token_amount_change["pre_balance"] / (10 ** token_amount_change["decimals"])
        # post_balance = token_amount_change["post_balance"] / (10 ** token_amount_change["decimals"])
        # if change_ui_amount > 0:
        #     # 加仓或开仓
        #     if pre_balance == 0 and post_balance > 0:
        #         return TxType.OPEN_POSITION
        #     elif post_balance > pre_balance:
        #         return TxType.ADD_POSITION
        #     else:
        #         raise UnknownTransactionType()
        # elif change_ui_amount < 0:
        #     if pre_balance > 0 and post_balance < 0.001:
        #         return TxType.CLOSE_POSITION
        #     elif post_balance < pre_balance:
        #         return TxType.REDUCE_POSITION
        #     else:
        #         raise UnknownTransactionType()
        # else:
        #     raise ZeroChangeAmountError(pre_balance, post_balance)
    async def get_mint_price(self, mint: str) -> float:
        # 这里可以根据mint地址查询价格
        # 这里假设mint地址为"mint_address"的价格为1.0
        mint = Pubkey.from_string(mint)
        result = await get_bonding_curve_account(get_async_client(), mint, PUMP_FUN_PROGRAM)
        if result is None:
            raise Exception("bonding curve account not found")
        bonding_curve, associated_bonding_curve, bonding_curve_account = result
        logger.info(f"get mint price {result}")
        # 价格 = SOL / 代币
        return (
            bonding_curve_account.virtual_sol_reserves
            / bonding_curve_account.virtual_token_reserves
            / 1000
        )

    def calculate_price_change(self,new_price: float, old_price: float) -> float:
        return ((new_price - old_price) / old_price) * 100

    async def needClosePotision(self) -> bool:
        # 检查是否需要关闭仓位
        # 这里假设需要关闭仓位的条件是价格变化超过10%
        mint = self.get_mint()
        new_price = await self.get_mint_price(mint) 
        createMint = await SwapRecordService().get_mint(mint)
        logger.info(f"get create mint: {createMint}")
        oldPrice = createMint.input_amount / createMint.output_amount
        price_change = self.calculate_price_change(new_price, oldPrice)
        return abs(price_change) > 10

    @cache
    def get_swap_program_id(self) -> str | None:
        log_messages = self.tx_detail["meta"]["logMessages"]
        for message in log_messages:
            for program_id in SWAP_PROGRAMS:
                if program_id in message:
                    return program_id
        return None

    @cache
    async def parse(self) -> TxEvent | None:
        # if self.tx_detail["meta"]["status"] is not None:
        #     if "Err" in self.tx_detail["meta"]["status"]:
        #         raise TransactionError(str(self.tx_detail["meta"]["status"]["Err"]))
        try:
            mint = self.get_mint()
        except ValueError:
            raise NotSwapTransaction()

        signature = self.get_tx_hash()
        timestamp = self.get_block_time()
        who = self.get_who()
        mint = self.get_mint()
        token_amount_change = self.get_token_amount_change()
        sol_amount_change = self.get_sol_amount_change()
        tx_type = self.get_tx_type()
        program_id = self.get_swap_program_id()

        if tx_type == TxType.OPEN_POSITION or tx_type == TxType.ADD_POSITION:
            from_amount = abs(sol_amount_change["change_amount"])
            from_decimals = 9
            to_amount = abs(token_amount_change["change_amount"])
            to_decimals = token_amount_change["decimals"]
            pre_token_balance = token_amount_change["pre_balance"]
            post_token_balance = token_amount_change["post_balance"]
        else:
            if not await self.needClosePotision():
                raise NotSwapTransaction()
            from_amount = abs(token_amount_change["change_amount"])
            from_decimals = token_amount_change["decimals"]
            to_amount = abs(sol_amount_change["change_amount"])
            to_decimals = 9
            pre_token_balance = token_amount_change["pre_balance"]
            post_token_balance = token_amount_change["post_balance"]

        return TxEvent(
            signature=signature,
            who=PUMP_FUN_PROGRAM_ID,
            from_amount=from_amount,
            from_decimals=from_decimals,
            to_amount=to_amount,
            to_decimals=to_decimals,
            mint=mint,
            tx_type=tx_type,
            tx_direction="buy" if tx_type  == TxType.OPEN_POSITION  else "sell",
            timestamp=timestamp,
            pre_token_amount=pre_token_balance,
            post_token_amount=post_token_balance,
            program_id=program_id,
        )
