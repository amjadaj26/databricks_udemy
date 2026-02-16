import dlt
from pyspark.sql.functions import *

@dlt.table(name = 'gold_mt940_reconciliation')
@dlt.expect('valid_amount', 'amount > 0')

def gold_mt940_reconciliation():
    ledger = spark.read.table('amjad_catalog.swift_mt940_recon.ledger_silver_layer')
    mt940 = spark.read.table('amjad_catalog.swift_mt940_recon.mt940_silver_layer')

    joined = mt940.alias("e").join(
        ledger.alias("i"),
        (col("e.account_no") == col("i.account_no")) &
        (col("e.Reference") == col("i.txn_reference")) &
        (col("e.Amount") == col("i.amount")) &
        (col("e.Currency") == col("i.currency")) &
        (col("e.Value_Date") == col("i.ledger_date")) &
        (col("e.Debit/Credit") == col("i.dr_cr")),
        "full_outer"
    )

    return joined.select(
        coalesce(col("e.account_no"), col("i.account_no")).alias("account_no"),
        coalesce(col("e.Currency"), col("i.currency")).alias("currency"),
        coalesce(col("e.Amount"), col("i.amount")).alias("amount"),
        col("e.Reference").alias("external_reference"),
        col("i.txn_reference").alias("internal_reference"),
        col("e.Value_Date").alias("value_date"),
        col("e.Debit/Credit").alias("debit_credit"),
        col("i.ledger_date").alias("ledger_date"),
        col("i.dr_cr").alias("internal_dr_cr"),
        when(col("e.Reference").isNotNull() & col("i.txn_reference").isNotNull(), "MATCHED")
            .when(col("e.Reference").isNull(), "INTERNAL_ONLY")
            .otherwise("EXTERNAL_ONLY")
            .alias("recon_status")
    )