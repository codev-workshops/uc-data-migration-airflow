"""Regenerate data/sample/ from the ts-sas-legacy-analytics banking seed data.

    python data/sample/generate_samples.py ../ts-sas-legacy-analytics/Data/csv

Deterministic: identical inputs always produce identical fixtures.
"""
import os
import shutil
import sys

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
ORACLE_DW_TABLES = ["CUST_ACCOUNTS", "PAYMENT_HISTORY", "LOAN_DETAILS", "COLLATERAL"]


def read(path):
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def write(df, *parts):
    path = os.path.join(HERE, *parts)
    if not os.path.isdir(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))
    df.to_csv(path, index=False, lineterminator="\n")


def main(seed_dir):
    for table in ORACLE_DW_TABLES:
        src = os.path.join(seed_dir, "oracle_dw", table + ".csv")
        dst_dir = os.path.join(HERE, "oracle_dw")
        if not os.path.isdir(dst_dir):
            os.makedirs(dst_dir)
        shutil.copyfile(src, os.path.join(dst_dir, table + ".csv"))

    # %dedup_string: per-account channel history (one token per transaction, in posting order)
    txn = read(os.path.join(seed_dir, "raw_bank", "TXN_FEED_20240131.csv"))
    hist = (txn.groupby("ACCOUNT_ID", sort=True)
            .agg(TXN_COUNT=("TRANSACTION_ID", "size"),
                 CHANNEL_HISTORY=("CHANNEL", " ".join),
                 TXN_TYPES=("TRANSACTION_TYPE", " ".join))
            .reset_index())
    examples = pd.DataFrame([
        {"ACCOUNT_ID": "DOC_EXAMPLE_1", "TXN_COUNT": "", "CHANNEL_HISTORY": "C A B B A G E 3 2 1 1 2 3",
         "TXN_TYPES": ""},
        {"ACCOUNT_ID": "DOC_EXAMPLE_2", "TXN_COUNT": "", "CHANNEL_HISTORY": "C|A|B|B|A|G|E|3|2|1|1|2|3",
         "TXN_TYPES": ""},
        {"ACCOUNT_ID": "MIXED_CASE", "TXN_COUNT": "", "CHANNEL_HISTORY": "Mobile mobile  ATM atm Atm POS",
         "TXN_TYPES": ""},
        {"ACCOUNT_ID": "EMPTY", "TXN_COUNT": "", "CHANNEL_HISTORY": "", "TXN_TYPES": ""},
    ])
    write(pd.concat([hist, examples], ignore_index=True), "dedup_string", "ACCOUNT_CHANNELS.csv")

    # %compare: base = Oracle DW extract, comp = a simulated migrated copy with known drift
    base_dir = os.path.join(HERE, "compare", "base")
    for table in ORACLE_DW_TABLES:
        write(read(os.path.join(seed_dir, "oracle_dw", table + ".csv")), "compare", "base", table + ".csv")

    accounts = read(os.path.join(base_dir, "CUST_ACCOUNTS.csv"))
    accounts = accounts[accounts["ACCOUNT_ID"] != accounts["ACCOUNT_ID"].iloc[10]].copy()   # dropped row
    balances = accounts["CURRENT_BALANCE"].astype(float)
    accounts.loc[accounts.index[[0, 5]], "CURRENT_BALANCE"] = (balances.iloc[[0, 5]] + 100).map("{:.2f}".format)
    accounts.loc[accounts.index[[3]], "CURRENT_BALANCE"] = (balances.iloc[[3]] * (1 + 1e-9)).map(repr)  # float noise
    accounts.loc[accounts.index[7], "ACCOUNT_STATUS"] = "C"
    extra = accounts.iloc[[1]].copy()
    extra["ACCOUNT_ID"] = "A99999999"
    accounts = pd.concat([accounts, extra], ignore_index=True)                            # added row
    accounts = accounts.sample(frac=1, random_state=42)                                   # different order
    accounts["LOADDTTM"] = "31JAN2024:02:00:00"                                           # audit column
    write(accounts, "compare", "comp", "CUST_ACCOUNTS.csv")

    write(read(os.path.join(base_dir, "LOAN_DETAILS.csv")), "compare", "comp", "LOAN_DETAILS.csv")  # identical
    payments = read(os.path.join(base_dir, "PAYMENT_HISTORY.csv")).drop(columns=["AVG_PMT_RATIO_12MO"])
    write(payments, "compare", "comp", "PAYMENT_HISTORY.csv")                            # dropped column
    write(read(os.path.join(seed_dir, "raw_bank", "DAILY_RATES.csv")), "compare", "comp", "DAILY_RATES.csv")
    # COLLATERAL intentionally absent from comp (NO MATCH in the library report)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else os.path.join("..", "ts-sas-legacy-analytics", "Data", "csv"))
