from datetime import datetime
import pandas as pd

from helper_functions import append_data_to_excel, get_nth_working_day

KPIS_FILE_PATH = r"P:\Technisch\PLANY PRODUKCJI\PLANIŚCI\PP_TOOLS_TEMP_FILES\07_PPS_KPIs\KPIs_source_data.xlsx"
ERROR_PATH = r"P:\Technisch\PLANY PRODUKCJI\PLANIŚCI\PP_TOOLS_TEMP_FILES\07_PPS_KPIs\error.log"


zsdkap_dtypes = {
    'Warenempfänger': 'string',
    'Materialnummer': 'string',
    'Artikeltext': 'string',
    'Auftrag': 'string',
    'Kontroler MRP': 'string',
    'Menge': 'float',
    'WA-Datum': 'datetime64[ns]',
}

zsdkap_new_columns_names = {
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
    global ZSDKAP_FILE_PATH, ZSBE_FILE_PATH, MB5T_FROM_2101_TO_ALL_PLANTS_FILE_PATH, KPIS_FILE_PATH
    ZSDKAP_FILE_PATH = f'excel_files/{zsdkap_report_name}.xlsx'
    ZSBE_FILE_PATH = f'excel_files/{zsbe_report_name}.xlsx'
    MB5T_FROM_2101_TO_ALL_PLANTS_FILE_PATH = f'excel_files/{mb5t_report_name}.xlsx'


def get_zsdkap_df(mrp_controller, df, date_limit=None):
    tmp = df.copy()
    if date_limit is not None:
        tmp = tmp[tmp['WA-Datum'] <= date_limit]

    tmp = tmp[tmp['mrp_controller'].isin(mrp_controller)]
    tmp = tmp[['mat_number', 'orders_quantity']]
    return tmp.groupby('mat_number', as_index=False).sum()


def get_zsdkap_merged_df(horizons, mrp_controller):
    # 1. Load raw data once
    raw_df = pd.read_excel(ZSDKAP_FILE_PATH, sheet_name='Sheet1', dtype=zsdkap_dtypes)
    raw_df = raw_df.rename(columns=zsdkap_new_columns_names)

    # 2. Base (total) dataframe
    zsdkap_total_df = get_zsdkap_df(mrp_controller, raw_df)

    # 3. Horizons
    horizons = horizons
    dfs = []

    for h in horizons:
        df_h = get_zsdkap_df(mrp_controller, raw_df, date_limit=get_nth_working_day(h))
        df_h = df_h.rename(columns={'orders_quantity': f'orders_quantity_{h}_days'})
        dfs.append(df_h)

    # 4. Merge everything
    zsdkap_merged_df = zsdkap_total_df
    for df_h in dfs:
        zsdkap_merged_df = zsdkap_merged_df.merge(df_h, on='mat_number', how='left')

    return zsdkap_merged_df


def get_zsbe_df(mrp_controller):
    zsbe_df = pd.read_excel(ZSBE_FILE_PATH, sheet_name='Sheet1', dtype=zsbe_dtypes)
    zsbe_df = zsbe_df.rename(columns=zsbe_new_columns_names)
    zsbe_df = zsbe_df[(zsbe_df['mrp_controller'].isin(mrp_controller))]
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

    def calculate_to_be_produced_gr_c(row, col_suffix=""):
        orders_quantity_column_name = f'orders_quantity{col_suffix}'
        stock_quantity = row['stock_quantity'] + row['transit_quantity']
        if stock_quantity < row[orders_quantity_column_name]:
            return row[orders_quantity_column_name] - stock_quantity
        else:
            return 0

    # Ensure mrp_controller is always a list
    if not isinstance(mrp_controller, (list, tuple, set, pd.Series)):
        mrp_controller = [mrp_controller]

    create_paths(zsdkap_report_name, zsbe_report_name, mb5t_report_name)

    horizons = [3, 5, 10]
    zsdkap_merged_df = get_zsdkap_merged_df(horizons, mrp_controller)

    zsbe_df = get_zsbe_df(mrp_controller)
    mb5t_df = get_mb5t_df()

    zsdkap_zsbe_merged_df = pd.merge(zsdkap_merged_df, zsbe_df, on='mat_number', how='outer')
    zsdkap_zsbe_merged_df.fillna(0, inplace=True)

    zsdkap_zsbe_mb5t_merged_df = pd.merge(zsdkap_zsbe_merged_df, mb5t_df, on='mat_number', how='left')
    zsdkap_zsbe_mb5t_merged_df = zsdkap_zsbe_mb5t_merged_df.rename(columns=mb5t_new_columns_names)
    zsdkap_zsbe_mb5t_merged_df.fillna(0, inplace=True)
    zsdkap_zsbe_mb5t_merged_df['to_be_produced_all'] = zsdkap_zsbe_mb5t_merged_df.apply(calculate_to_be_produced_all,
                                                                                        axis=1)

    zsdkap_zsbe_mb5t_merged_df['to_be_produced_gr_c'] = zsdkap_zsbe_mb5t_merged_df.apply(calculate_to_be_produced_gr_c,
                                                                                         axis=1)
    for h in horizons:
        zsdkap_zsbe_mb5t_merged_df[f'to_be_produced_gr_c_{h}_days'] = zsdkap_zsbe_mb5t_merged_df.apply(
            calculate_to_be_produced_gr_c, col_suffix=f"_{h}_days", axis=1)

    kpis = {"ORDERS LEVEL (ALL)": zsdkap_zsbe_mb5t_merged_df['to_be_produced_all'].sum(),
            "ORDERS LEVEL (GR C)": zsdkap_zsbe_mb5t_merged_df['to_be_produced_gr_c'].sum()}

    for h in horizons:
        kpis[f'ORDERS LEVEL (GR C - {h})'] = zsdkap_zsbe_mb5t_merged_df[f'to_be_produced_gr_c_{h}_days'].sum()

    zsdkap_zsbe_mb5t_merged_df.to_excel(f"excel_files/output/output_{'_'.join(mrp_controller)}.xlsx")
    return kpis


if __name__ == "__main__":
    today = datetime.today()
    today_str = today.strftime('%Y-%m-%d')

    zsdkap = 'zsdkap2'
    zsbe = 'ZSBE_r4_r7'

    lines = ["P100", "M200"]
    mrp_controllers = ['L1K', ('L1H', 'L41', 'L3H')]

    for line, mrp in zip(lines, mrp_controllers):
        kpis_result = calculate_order_level_KPI(zsdkap_report_name=zsdkap, zsbe_report_name=zsbe, mrp_controller=mrp)
        kpis_result["LINE"] = line

        append_data_to_excel(
            status_file=KPIS_FILE_PATH,
            data_dict=kpis_result,
            error_path=ERROR_PATH,
            sheet_name="LUB"
        )



