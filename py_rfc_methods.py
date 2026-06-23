import pandas as pd
from sap_conn import get_conn
from sap_rtab import rfc_read_table

from helper_functions import chunks

import time


def get_delivery_plants_df(orders_list, chunk_size=1000, printing_frequency=2):
    vbeln_chunks = list(chunks(orders_list, chunk_size))
    vbap = []

    with get_conn("P11_SSO") as conn:

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
                ],
                where=f"""
                    {vbeln_filter}
                """,
                # rowcount=1500
            )

            vbap.extend(vbap_chunk_data)

            if is_printing:
                print(
                    f"\nMKPF chunk {chunk_num}/{len(vbeln_chunks)} "
                    f"| docs={len(vbeln_chunk)}"
                    f"\n Chunk time: {time.perf_counter() - chunk_start:.2f} s"
                )

        vbap_df = pd.DataFrame(
            vbap,
            columns=["VBELN", "POSNR", "WERKS"]
    )

    return vbap_df