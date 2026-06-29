import pandas as pd
from sap_conn import get_conn
from sap_rtab import rfc_read_table

from helper_functions import chunks

import time


def get_delivery_plants_df(sap_system, orders_list, chunk_size=1000, printing_frequency=2):
    vbeln_chunks = list(chunks(orders_list, chunk_size))
    vbap = []

    with get_conn(sap_system) as conn:

        for chunk_num, vbeln_chunk in enumerate(vbeln_chunks, start=1):
            is_printing = chunk_num % printing_frequency == 0
            chunk_start = time.perf_counter()

            vbeln_filter = " OR ".join(
                [f"VBELN = '{m}'" for m in vbeln_chunk]
            )

            vbap_chunk_data = rfc_read_table(
                conn=conn,
                table="VBAP",
                fields=[
                    "VBELN",  # zlecenie klienta
                    "POSNR",  # pozycja
                    "WERKS",  # zakład dostarczający
                    "SOBKZ",  # special stock indicator - "E" for special customer requirements
                ],
                where=f"""
                    {vbeln_filter}
                """,
                # rowcount=1500
            )

            vbap.extend(vbap_chunk_data)

            if is_printing:
                print(
                    f"\nVBELN chunk {chunk_num}/{len(vbeln_chunks)} "
                    f"| docs={len(vbeln_chunk)}"
                    f"\n Chunk time: {time.perf_counter() - chunk_start:.2f} s"
                )

        vbap_df = pd.DataFrame(
            vbap,
            columns=["VBELN", "POSNR", "WERKS", "SOBKZ"]
    )

    vbap_df.drop_duplicates(subset=["VBELN", "POSNR"], keep="first", inplace=True)

    return vbap_df


def get_special_stock_indicators(sap_system, orders_list, chunk_size=1000, printing_frequency=2):
    vbeln_chunks = list(chunks(orders_list, chunk_size))
    vbbe = []

    with get_conn(sap_system) as conn:

        for chunk_num, vbeln_chunk in enumerate(vbeln_chunks, start=1):
            is_printing = chunk_num % printing_frequency == 0
            chunk_start = time.perf_counter()

            vbeln_filter = " OR ".join(
                [f"VBELN = '{m}'" for m in vbeln_chunk]
            )

            vbbe_chunk_data = rfc_read_table(
                conn=conn,
                table="VBBE",
                fields=[
                    "VBELN",  # zlecenie klienta
                    "POSNR",  # pozycja
                    "SOBKZ",  # special stock indicator - "E" for special customer requirements
                ],
                where=f"""
                    {vbeln_filter}
                """,
                # rowcount=1500
            )

            vbbe.extend(vbbe_chunk_data)

            if is_printing:
                print(
                    f"\nVBELN chunk {chunk_num}/{len(vbeln_chunks)} "
                    f"| docs={len(vbeln_chunk)}"
                    f"\n Chunk time: {time.perf_counter() - chunk_start:.2f} s"
                )

        vbbe_df = pd.DataFrame(
            vbbe,
            columns=["VBELN", "POSNR", "SOBKZ"]
    )

    vbbe_df.drop_duplicates(subset=["VBELN", "POSNR"], keep="first", inplace=True)

    return vbbe_df


def get_purchase_order_sales_orders(sap_system, po_list, chunk_size=1000, printing_frequency=2):
    ebeln_chunks = list(chunks(po_list, chunk_size))
    ekkn = []

    with get_conn(sap_system) as conn:

        for chunk_num, ebeln_chunk in enumerate(ebeln_chunks, start=1):
            is_printing = chunk_num % printing_frequency == 0
            chunk_start = time.perf_counter()

            ebeln_filter = " OR ".join(
                [f"EBELN = '{po}'" for po in ebeln_chunk]
            )

            ekkn_chunk_data = rfc_read_table(
                conn=conn,
                table="EKKN",
                fields=[
                    "EBELN",  # Purchase Order
                    "EBELP",  # Purchase Order Item
                    "VBELN",  # Sales Order
                    "VBELP",  # Sales Order Item
                ],
                where=f"""
                    {ebeln_filter}
                """,
                # rowcount=1500
            )

            ekkn.extend(ekkn_chunk_data)

            if is_printing:
                print(
                    f"\nEBELN chunk {chunk_num}/{len(ebeln_chunks)} "
                    f"| docs={len(ebeln_chunk)}"
                    f"\nChunk time: {time.perf_counter() - chunk_start:.2f} s"
                )

    ekkn_df = pd.DataFrame(
        ekkn,
        columns=["EBELN", "EBELP", "VBELN", "VBELP"]
    )

    ekkn_df.drop_duplicates(
        subset=["EBELN", "EBELP", "VBELN", "VBELP"],
        keep="first",
        inplace=True
    )

    return ekkn_df