zsdkap_dtypes = {
    'Odbiorca materia≈Ç√≥w': 'string',
    'Materia≈Ç': 'string',
    'Nazwa': 'string',
    'Dokument sprzeda≈ºy': 'string',
    'Pozycja': 'string',
    'Kontroler MRP': 'string',
    'Ilo≈õƒá zlecenia': 'string',
    # 'WA-Datum': 'datetime64[ns]',
}

zsdkap_new_columns_names = {
    'Odbiorca materia≈Ç√≥w': 'receiver',
    'Materia≈Ç': 'mat_number',
    'Nazwa': 'mat_description',
    'Dokument sprzeda≈ºy': 'customer_order_number',
    'Pozycja': 'customer_order_position',
    'Kontroler MRP': 'mrp_controller',
    'Ilo≈õƒá zlecenia': 'orders_quantity',
    'WADAT': 'dispatch_date_original',
    # 'WADAT': 'dispatch_date',
}

zsbe_dtypes = {
    'Materiał': 'string',
    'Zakład': 'string',
    'Column': 'float',
    'Column 2': 'float',
}

zsbe_new_columns_names = {
    'Materiał': 'mat_number',
    'Zakład': 'plant',
    'Column': 'stock_quantity',
    'Kontroler MRP': 'mrp_controller',
    'Column 2': 'safety_stock',
    'Opis': 'mat_description'
}

mb5td_dtypes = {
    'Materiał': 'string',
    'Zakład': 'string',
    'Ilość': 'float',
    'Dok.zaopatrz.': 'string',
    'Pozycja': 'string',
}

mb5td_new_columns_names = {
    'Materiał': 'mat_number',
    'Zakład': 'plant',
    'Zakład dostarczający': 'supplying_plant',
    'Ilość': 'transit_quantity',
    'Zapas specjalny': 'special_stock_indicator',
    'Dok.zaopatrz.': 'purchase_order_number',
    'Pozycja': 'purchase_order_position'
}

mb52_dtypes = {
    'Materiał': 'string',
    'Nieogr. wykorz.': 'float',
    'Dokument SD': 'string',
    'Pozycja': 'string',
    'Zakład': 'string',
    'Skład': 'string'
}

mb52_new_columns_names = {
    'Materiał': 'mat_number',
    'Nieogr. wykorz.': 'stock_quantity',
    'Dokument SD': 'customer_order_number',
    'Pozycja': 'customer_order_position',
    'Zakład': 'plant',
    'Skład': 'storage_location'
}

zkbp1_dtypes = {
    'L.poj.aktiv': 'float',
    'Zawart pojemn': 'float',
}

zkbp1_new_columns_names = {
    'NrMat.': 'mat_number',
    'L.poj.aktiv': 'num_of_containers',
    'Zawart pojemn': 'container_capacity',
    'Krótki tekst mater.:': 'mat_name'
}

vbap_new_columns_names = {
    'VBELN': 'customer_order_number',
    'POSNR': 'customer_order_position',
    'WERKS': 'delivery_plant',
    'SOBKZ': 'special_stock_indicator'
}

ekkn_new_columns_names = {
    "EBELN": "purchase_order_number",
    "EBELP": 'purchase_order_position',
    'VBELN': 'customer_order_number',
    'VBELP': 'customer_order_position'
}

production_site_map = {
    # Production site 2101
    'L1K': '2101',
    'L1H': '2101',
    'L41': '2101',
    'L3H': '2101',
    'L82': '2101',
    'L2H': '2101',
    'LD1': '2101',
    'LZ1': '2101',
    'LMD': '2101',
    'LAS': '2101',
    'L2E': '2101',
    'L2V': '2101',
    'LI1': '2101',
    'LI3': '2101',
    'L2J': '2101',
    'LI5': '2101',
    'LI8': '2101',
    'L2F': '2101',
    'LI6': '2101',
    'L2I': '2101',
    'L2B': '2101',
    'L2R': '2101',
    'LI2': '2101',
    'LI4': '2101',
    'LI7': '2101',
    'L2S': '2101',
    'L11': '2101',

    # Production site 0301
    'M81': '0301',
    'M82': '0301',
    'MQ1': '0301',
    'MQ2': '0301',
    'MR1': '0301',
    'MR2': '0301',
    'MR3': '0301',
    'MR4': '0301',
    'MEB': '0301',
    'MED': '0301',
    'MEE': '0301',
    'MEI': '0301',
    'MEH': '0301',
    'MEJ': '0301',
    'MEM': '0301',
    'MEN': '0301',
    'MEX': '0301',
    'M8M': '0301',
    'M84': '0301',
    'M71': '0301',
    'M72': '0301',
    'MQ4': '0301',
    'MNW': '0301',
    'MRR': '0301'
}