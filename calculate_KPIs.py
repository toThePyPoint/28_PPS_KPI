import pandas as pd


zsdkap_dtypes = {
    'Warenempfänger': 'string',
    'Materialnummer': 'string',
    'Artikeltext': 'string',
    'Auftrag': 'string',
    'Kontroler MRP': 'string',
    'Menge': 'float',
}

zsdkap_new_columns_names={
    'Warenempfänger': 'receiver',
    'Materialnummer': 'mat_number',
    'Artikeltext': 'mat_description',
    'Auftrag': 'customer_order_number',
    'Kontroler MRP': 'mrp_controller',
    'Menge': 'orders_quantity',
}

zsbe_dtypes = {
    'Materiał': 'string',
    'Zakład': 'string',
    'dowolne użycie': 'float',
    'zapas bezpieczeństwa': 'float',
}

zsbe_new_columns_names = {
    'Materiał': 'mat_number',
    'Zakład': 'plant',
    'dowolne użycie': 'stock_quantity',
    'Kontroler MRP': 'mrp_controller',
    'zapas bezpieczeństwa': 'safety_stock',
}

mb5t_dtypes = {
    'Materiał': 'string',
    'Zakład': 'string',
    'Ilość zamówienia': 'float',
    'Pozycja': 'string',
}

mb5t_new_columns_names = {
    'Materiał': 'mat_number',
    'Zakład': 'plant',
    'Zakład dostarczający': 'supplying_plant',
    'Ilość zamówienia': 'transit_quantity',
}


def create_paths(zsdkap_report_name, zsbe_report_name, mb5t_report_name):
    global ZSDKAP_FILE_PATH, ZSBE_FILE_PATH, MB5T_FROM_2101_TO_ALL_PLANTS_FILE_PATH
    ZSDKAP_FILE_PATH = f'excel_files/{zsdkap_report_name}.xlsx'
    ZSBE_FILE_PATH = f'excel_files/{zsbe_report_name}.xlsx'
    MB5T_FROM_2101_TO_ALL_PLANTS_FILE_PATH = f'excel_files/{mb5t_report_name}.xlsx'


def get_zsdkap_df(mrp_controller):
    zsdkap_df = pd.read_excel(ZSDKAP_FILE_PATH, sheet_name='Sheet1', dtype=zsdkap_dtypes)
    zsdkap_df = zsdkap_df.rename(columns=zsdkap_new_columns_names)
    zsdkap_df = zsdkap_df[(zsdkap_df['mrp_controller'] == mrp_controller)]
    zsdkap_df = zsdkap_df[['mat_number', 'orders_quantity']]
    zsdkap_df = zsdkap_df.groupby('mat_number', as_index=False).sum()

    return zsdkap_df


def get_zsbe_df(mrp_controller):
    zsbe_df = pd.read_excel(ZSBE_FILE_PATH, sheet_name='Sheet1', dtype=zsbe_dtypes)
    zsbe_df = zsbe_df.rename(columns=zsbe_new_columns_names)
    zsbe_df = zsbe_df[(zsbe_df['mrp_controller'] == mrp_controller)]
    zsbe_df = zsbe_df[['mat_number', 'stock_quantity', 'safety_stock']]
    zsbe_df = zsbe_df.groupby('mat_number', as_index=False).sum()

    return zsbe_df


def get_mb5t_df():
    mb5t_df = pd.read_excel(MB5T_FROM_2101_TO_ALL_PLANTS_FILE_PATH, sheet_name='Sheet1', dtype=mb5t_dtypes)
    mb5t_df = mb5t_df.rename(columns=mb5t_new_columns_names)
    mb5t_df = mb5t_df[['mat_number', 'transit_quantity']]
    mb5t_df = mb5t_df.groupby('mat_number', as_index=False).sum()

    return mb5t_df


def calculate_order_level_KPI(zsdkap_report_name="zsdkap",
                              zsbe_report_name="ZSBE_L1K",
                              mb5t_report_name="MB5T_from_2101_to_all_plants",
                              mrp_controller='L1K'):

    def calculate_to_be_produced_all(row):
        stock_quantity = row['stock_quantity'] + row['transit_quantity']
        if (stock_quantity - row['orders_quantity'] >= row['safety_stock']) and row['safety_stock'] > 0:
            return 0
        else:
            if row['orders_quantity'] + row['safety_stock'] - stock_quantity > 0:
                return row['orders_quantity'] + row['safety_stock'] - stock_quantity
            else:
                return 0

    def calculate_to_be_produced_gr_c(row):
        stock_quantity = row['stock_quantity'] + row['transit_quantity']
        if stock_quantity < row['orders_quantity']:
            return row['orders_quantity'] - stock_quantity
        else:
            return 0

    create_paths(zsdkap_report_name, zsbe_report_name, mb5t_report_name)
    zsdkap_df = get_zsdkap_df(mrp_controller)
    zsbe_df = get_zsbe_df(mrp_controller)
    mb5t_df = get_mb5t_df()

    zsdkap_zsbe_merged_df = pd.merge(zsdkap_df, zsbe_df, on='mat_number', how='outer')
    zsdkap_zsbe_merged_df.fillna(0, inplace=True)

    zsdkap_zsbe_mb5t_merged_df = pd.merge(zsdkap_zsbe_merged_df, mb5t_df, on='mat_number', how='left')
    zsdkap_zsbe_mb5t_merged_df = zsdkap_zsbe_mb5t_merged_df.rename(columns=mb5t_new_columns_names)
    zsdkap_zsbe_mb5t_merged_df.fillna(0, inplace=True)
    zsdkap_zsbe_mb5t_merged_df['to_be_produced_all'] = zsdkap_zsbe_mb5t_merged_df.apply(calculate_to_be_produced_all,
                                                                                        axis=1)
    zsdkap_zsbe_mb5t_merged_df['to_be_produced_gr_c'] = zsdkap_zsbe_mb5t_merged_df.apply(calculate_to_be_produced_gr_c,
                                                                                         axis=1)

    to_be_produced_all_total = zsdkap_zsbe_mb5t_merged_df['to_be_produced_all'].sum()
    to_be_produced_gr_c_total = zsdkap_zsbe_mb5t_merged_df['to_be_produced_gr_c'].sum()

    return to_be_produced_all_total, to_be_produced_gr_c_total


if __name__ == "__main__":
    kpi_all, kpi_gr_c = calculate_order_level_KPI()
    print(f"KPI_ALL: {kpi_all}")
    print(f"KPI GR C: {kpi_gr_c}")



