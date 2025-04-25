# from functools import cache

# from solbot_common.constants import PUMP_FUN_PROGRAM, SOL, WSOL
# from solbot_common.types import (
#     SolAmountChange,
#     TokenAmountChange,
#     TxEvent,
#     TxType,
# )


# class PumpTxParser:
#     def __init__(self, tx_detail: dict) -> None:
#         self.tx_detail = tx_detail

#     @cache
#     def get_block_time(self) -> int:
#         return self.tx_detail["block_time"]

#     @cache
#     def get_tx_hash(self) -> str:
#         return self.tx_detail["tx_hash"]

#     @cache
#     def get_who(self) -> str:
#         return self.tx_detail["signer"][0]

#     @cache
#     def get_mint(self) -> str:
#         tokens_involved = self.tx_detail["tokens_involved"]
#         tokens_involved_len = len(tokens_involved)
#         if tokens_involved_len == 1:
#             return tokens_involved[0]
#         elif tokens_involved_len == 2:
#             if tokens_involved[0] == SOL:
#                 return tokens_involved[1]
#             elif tokens_involved[1] == SOL:
#                 return tokens_involved[0]
#             elif tokens_involved[0] == str(WSOL):
#                 return tokens_involved[1]
#             elif tokens_involved[1] == str(WSOL):
#                 return tokens_involved[0]
#         else:
#             raise ValueError(f"unknown tokens_involved, len = {tokens_involved_len}")
#         raise ValueError("unknown mint")

#     @cache
#     def get_token_amount_change(self) -> TokenAmountChange:
#         mint = self.get_mint()
#         token_bal_change = self.tx_detail["token_bal_change"]
#         for token_bal in token_bal_change:
#             if (
#                 token_bal["token_address"] == mint
#                 and token_bal["owner"] == self.get_who()
#             ):
#                 return {
#                     # "change_type": token_bal["change_type"],
#                     "change_amount": int(token_bal["change_amount"]),
#                     "decimals": int(token_bal["decimals"]),
#                     "pre_balance": int(token_bal["pre_balance"]),
#                     "post_balance": int(token_bal["post_balance"]),
#                 }
#         else:
#             raise ValueError("mint not found in token_bal_change")

#     @cache
#     def get_sol_amount_change(self) -> SolAmountChange:
#         sol_bal_change = self.tx_detail["sol_bal_change"]
#         for sol_bal in sol_bal_change:
#             if sol_bal["address"] == self.get_who():
#                 sol_bal["change_amount"] = int(sol_bal["change_amount"])
#                 return sol_bal
#         else:
#             raise ValueError("who not found in sol_bal_change")

#     @cache
#     def get_tx_type(self) -> TxType:
#         token_amount_change = self.get_token_amount_change()
#         change_ui_amount = int(token_amount_change["change_amount"]) / (
#             10 ** token_amount_change["decimals"]
#         )
#         pre_balance = int(token_amount_change["pre_balance"]) / (
#             10 ** token_amount_change["decimals"]
#         )
#         post_balance = int(token_amount_change["post_balance"]) / (
#             10 ** token_amount_change["decimals"]
#         )
#         if change_ui_amount > 0:
#             # 加仓或开仓
#             if pre_balance == 0 and post_balance > 0:
#                 return TxType.OPEN_POSITION
#             elif post_balance > pre_balance:
#                 return TxType.ADD_POSITION
#             else:
#                 assert False, "not possible"
#         elif change_ui_amount < 0:
#             if pre_balance > 0 and post_balance < 0.001:
#                 return TxType.CLOSE_POSITION
#             elif post_balance < pre_balance:
#                 return TxType.REDUCE_POSITION
#             else:
#                 assert False, "not possible"
#         else:
#             assert False, "not possible"

#     def parse(self) -> TxEvent:
#         token_amount_change = self.get_token_amount_change()
#         sol_amount_change = self.get_sol_amount_change()
#         tx_direction = "buy" if sol_amount_change["change_amount"] < 0 else "sell"
#         if tx_direction == "buy":
#             from_amount = abs(sol_amount_change["change_amount"])
#             from_decimals = 9
#             to_amount = abs(token_amount_change["change_amount"])
#             to_decimals = token_amount_change["decimals"]
#         else:
#             from_amount = abs(token_amount_change["change_amount"])
#             from_decimals = token_amount_change["decimals"]
#             to_amount = abs(sol_amount_change["change_amount"])
#             to_decimals = 9

#         return TxEvent(
#             signature=self.get_tx_hash(),
#             who=self.get_who(),
#             from_amount=from_amount,
#             from_decimals=from_decimals,
#             to_amount=to_amount,
#             to_decimals=to_decimals,
#             mint=self.get_mint(),
#             tx_type=self.get_tx_type(),
#             tx_direction=tx_direction,
#             timestamp=self.get_block_time(),
#         )


# class PumpSwapTxParser:
#     def __init__(self, tx_detail: dict) -> None:
#         self.tx_detail = tx_detail

#     @cache
#     def get_block_time(self) -> int:
#         return self.tx_detail["block_time"]

#     @cache
#     def get_tx_hash(self) -> str:
#         return self.tx_detail["tx_hash"]

#     @cache
#     def get_who(self) -> str:
#         return self.tx_detail["signer"][0]

#     def parse(self) -> TxEvent:
#         activities = self.tx_detail["activities"]
#         swap_activity = None
#         for activity in activities:
#             if activity["program_id"] == str(PUMP_FUN_PROGRAM):
#                 swap_activity = activity
#                 break
#         else:
#             raise ValueError("no pump swap activity found")

#         if swap_activity is None:
#             raise ValueError("no pump swap activity found")

#         data = swap_activity["data"]
#         token1 = data["token_1"]
#         if token1 == SOL:
#             mint = data["token_2"]
#             tx_direction = "buy"
#             from_amount = data["amount_1"]
#             from_decimals = data["token_decimal_1"]
#             to_amount = data["amount_2"]
#             to_decimals = data["token_decimal_2"]
#         else:
#             mint = data["token_1"]
#             tx_direction = "sell"
#             from_amount = data["amount_2"]
#             from_decimals = data["token_decimal_2"]
#             to_amount = data["amount_1"]
#             to_decimals = data["token_decimal_1"]

#         return TxEvent(
#             signature=self.get_tx_hash(),
#             who=self.get_who(),
#             from_amount=from_amount,
#             from_decimals=from_decimals,
#             to_amount=to_amount,
#             to_decimals=to_decimals,
#             mint=mint,
#             tx_type=TxType.OPEN_POSITION,
#             tx_direction=tx_direction,
#             timestamp=self.get_block_time(),
#         )


# def parse_tx(tx_detail: dict) -> TxEvent | None:
#     result = None
#     try:
#         result = PumpSwapTxParser(tx_detail).parse()
#     except Exception:
#         pass

#     if result is not None:
#         return result

#     try:
#         result = PumpTxParser(tx_detail).parse()
#     except Exception:
#         pass
