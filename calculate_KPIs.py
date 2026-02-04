import sys
import traceback
from datetime import datetime
import pandas as pd

from helper_functions import append_data_to_excel, get_nth_working_day, clean_number, generate_zsdkap_filename

KPIS_FILE_PATH = r"P:\Technisch\PLANY PRODUKCJI\PLANIŚCI\PP_TOOLS_TEMP_FILES\07_PPS_KPIs\KPIs_source_data.xlsx"
ERROR_PATH = r"P:\Technisch\PLANY PRODUKCJI\PLANIŚCI\PP_TOOLS_TEMP_FILES\07_PPS_KPIs\error.log"


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
    'WADAT': 'dispatch_date'
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
}

mb5td_dtypes = {
    'Materiał': 'string',
    'Zakład': 'string',
    'Ilość': 'float',
    'Pozycja': 'string',
}

mb5td_new_columns_names = {
    'Materiał': 'mat_number',
    'Zakład': 'plant',
    'Zakład dostarczający': 'supplying_plant',
    'Ilość': 'transit_quantity',
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


def create_paths(zsdkap_report_name, zsbe_report_name, mb5t_report_name, mb52_report_name, zkbp1_report_name):
    global ZSDKAP_FILE_PATH, ZSBE_FILE_PATH, MB5TD_2101, KPIS_FILE_PATH, MB52_FILE_PATH, ZKBP1_FILE_PATH
    # ZSDKAP_FILE_PATH = fr'C:\Temp\Kamil\Prywatne\07_Programowanie\99_Moje_projekty\28_PPS_KPI\excel_files/job/{zsdkap_report_name}.csv'
    # ZSBE_FILE_PATH = fr'C:\Temp\Kamil\Prywatne\07_Programowanie\99_Moje_projekty\28_PPS_KPI\excel_files/job/{zsbe_report_name}.xlsx'
    # MB5TD_2101 = fr'C:\Temp\Kamil\Prywatne\07_Programowanie\99_Moje_projekty/28_PPS_KPI\excel_files/job/{mb5t_report_name}.xlsx'
    # MB52_FILE_PATH = f'C:/Temp/Kamil/Prywatne/07_Programowanie/99_Moje_projekty/28_PPS_KPI/excel_files/job/{mb52_report_name}.xlsx'

    ZSDKAP_FILE_PATH = fr'\\rfmesrv5\connect\DST_SAP_Transfer\P11\PPS_LUB\02_MID_TERM_PLANNING_ALIGNMENT/{zsdkap_report_name}.csv'
    ZSBE_FILE_PATH = fr'\\rfmesrv5\connect\DST_SAP_Transfer\P11\PPS_LUB\02_MID_TERM_PLANNING_ALIGNMENT/{zsbe_report_name}.xlsx'
    MB5TD_2101 = fr'\\rfmesrv5\connect\DST_SAP_Transfer\P11\PPS_LUB\02_MID_TERM_PLANNING_ALIGNMENT/{mb5t_report_name}.xlsx'
    MB52_FILE_PATH = fr'\\rfmesrv5\connect\DST_SAP_Transfer\P11\PPS_LUB\02_MID_TERM_PLANNING_ALIGNMENT/{mb52_report_name}.xlsx'
    ZKBP1_FILE_PATH = fr'\\rfmesrv5\connect\DST_SAP_Transfer\P11\PPS_LUB\02_MID_TERM_PLANNING_ALIGNMENT/{zkbp1_report_name}.xlsx'


def get_zsdkap_df(mrp_controller, mat_name, df, date_limit=None):
    tmp = df.copy()
    if date_limit is not None:
        tmp = tmp[tmp['dispatch_date'] <= date_limit]

    tmp = tmp[(tmp['mrp_controller'].isin(mrp_controller)) & (tmp['mat_description'].str.startswith(mat_name))]
    tmp = tmp[['mat_number', 'orders_quantity']]
    return tmp.groupby('mat_number', as_index=False).sum()


def get_zsdkap_customer_orders_numbers(mrp_controller, mat_name, df, date_limit=None):
    tmp = df.copy()
    if date_limit is not None:
        tmp = tmp[tmp['dispatch_date'] <= date_limit]

    tmp = tmp[(tmp['mrp_controller'].isin(mrp_controller)) & (tmp['mat_description'].str.startswith(mat_name))]
    tmp = tmp[['mat_number', 'customer_order_number', 'customer_order_position', 'orders_quantity']]
    return tmp


def get_zsdkap_merged_df(horizons, mrp_controller, mat_name):
    # 1. Load raw data once
    # raw_df = pd.read_excel(ZSDKAP_FILE_PATH, sheet_name='Sheet1', dtype=zsdkap_dtypes)
    # raw_df = pd.read_csv(ZSDKAP_FILE_PATH, dtype=zsdkap_dtypes)
    # # Convert WA-Datum correctly to datetime
    # raw_df = raw_df.rename(columns=zsdkap_new_columns_names)
    # raw_df['dispatch_date'] = pd.to_datetime(raw_df['dispatch_date'], dayfirst=True, errors='coerce')

    raw_df = pd.read_csv(ZSDKAP_FILE_PATH, dtype=zsdkap_dtypes, sep=';', encoding='MacRoman')
    raw_df = raw_df.rename(columns=zsdkap_new_columns_names)
    raw_df['dispatch_date'] = pd.to_datetime(raw_df['dispatch_date'], dayfirst=True, errors='coerce')
    raw_df['orders_quantity'] = raw_df['orders_quantity'].apply(clean_number)

    # 2. Base (total) dataframe
    zsdkap_total_df = get_zsdkap_df(mrp_controller, mat_name, raw_df)

    # 3. Horizons
    horizons = horizons
    dfs = []

    for h in horizons:
        df_h = get_zsdkap_df(mrp_controller, mat_name, raw_df, date_limit=get_nth_working_day(h))
        df_h = df_h.rename(columns={'orders_quantity': f'orders_quantity_{h}_days'})
        dfs.append(df_h)

    # 4. Merge everything
    zsdkap_merged_df = zsdkap_total_df
    for df_h in dfs:
        zsdkap_merged_df = zsdkap_merged_df.merge(df_h, on='mat_number', how='left')

    return zsdkap_merged_df


def get_zsbe_df(mrp_controller, include_zkbp1_sb, mat_name):
    zsbe_df = pd.read_excel(ZSBE_FILE_PATH, sheet_name='Exported data', dtype=zsbe_dtypes)
    zsbe_df = zsbe_df.rename(columns=zsbe_new_columns_names)
    zsbe_df = zsbe_df[(zsbe_df['mrp_controller'].isin(mrp_controller)) & (~zsbe_df['mat_number'].str.startswith('99'))
                      & (zsbe_df['Opis'].str.startswith(mat_name))]
    zsbe_df = zsbe_df[['mat_number', 'Opis', 'stock_quantity', 'safety_stock', 'plant']]

    # Include safety stocks from ZKBP1 transaction
    if include_zkbp1_sb:
        zkbp1_df = pd.read_excel(ZKBP1_FILE_PATH, sheet_name='Exported data', dtype=zkbp1_dtypes)
        zkbp1_df = zkbp1_df.rename(columns=zkbp1_new_columns_names)
        zkbp1_df['mat_number'] = zkbp1_df['mat_number'].astype(str)
        zkbp1_df['safety_stock_kanban'] = zkbp1_df['num_of_containers'] * zkbp1_df['container_capacity']
        zkbp1_df['plant'] = "0301"
        zkbp1_df = zkbp1_df[[
            'mat_number',
            'safety_stock_kanban',
            'plant', ]]

        zsbe_zkbp1_merged = pd.merge(zsbe_df, zkbp1_df, on=['mat_number', 'plant'], how='left')
        zsbe_zkbp1_merged['safety_stock_kanban'] = zsbe_zkbp1_merged['safety_stock_kanban'].fillna(0)
        zsbe_zkbp1_merged['safety_stock'] = zsbe_zkbp1_merged['safety_stock'] + zsbe_zkbp1_merged['safety_stock_kanban']
        zsbe_df = zsbe_zkbp1_merged

    zsbe_df = zsbe_df[['mat_number', 'Opis', 'stock_quantity', 'safety_stock']]
    zsbe_df = zsbe_df.groupby('mat_number', as_index=False).agg({
        'Opis': 'first',  # Wybierz pierwszą wartość
        'stock_quantity': 'sum',
        'safety_stock': 'sum'
    })

    return zsbe_df


def get_mb5t_df():
    mb5t_df = pd.read_excel(MB5TD_2101, sheet_name='Exported data', dtype=mb5td_dtypes)
    mb5t_df = mb5t_df.rename(columns=mb5td_new_columns_names)
    mb5t_df = mb5t_df[['mat_number', 'transit_quantity']]
    mb5t_df = mb5t_df.groupby('mat_number', as_index=False).sum()

    return mb5t_df


def get_mb52_df():
    mb52_df = pd.read_excel(MB52_FILE_PATH, sheet_name='Exported data', dtype=mb52_dtypes)
    mb52_df = mb52_df.rename(columns=mb52_new_columns_names)

    mb52_df["customer_order_number"] = (
        mb52_df["customer_order_number"].str.zfill(10)
    )

    return mb52_df


def calculate_order_level_KPI(zsdkap_report_name="zsdkap",
                              zsbe_report_name="ZSBE_L1K",
                              mb5t_report_name="MB5T_from_2101_to_all_plants",
                              mb52_report_name="mb52",
                              horizons=None,
                              mrp_controller='L1K',
                              mat_name='R4',
                              ready_goods_storage_locs=('0004', '0005', 'FSC'),
                              include_zkbp1_sb=False,
                              zkbp1_report_name='ZKBP1_SB_0301',):

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
        stock_quantity = row[f'stock_quantity{col_suffix}'] + row['transit_quantity']
        if stock_quantity < row[orders_quantity_column_name]:
            return row[orders_quantity_column_name] - stock_quantity
        else:
            return 0

    # Ensure mrp_controller is always a tuple
    if not isinstance(mrp_controller, (list, tuple, set, pd.Series)):
        mrp_controller = mrp_controller,

    # Ensure mat_name is always a tuple
    if not isinstance(mat_name, (list, tuple, set, pd.Series)):
        mat_name = mat_name,

    create_paths(zsdkap_report_name, zsbe_report_name, mb5t_report_name, mb52_report_name, zkbp1_report_name)

    horizons = horizons
    zsdkap_merged_df = get_zsdkap_merged_df(horizons, mrp_controller, mat_name)

    zsbe_df = get_zsbe_df(mrp_controller, include_zkbp1_sb, mat_name)
    mb5t_df = get_mb5t_df()
    mb52_df = get_mb52_df()

    # ===== Start of MB52 stocks implementation ====
    # This snippet of code ensures that the stock_quantities data is taken from MB52 transaction instead of ZSBE
    # I excluded confi ('99') items, because there is separate logic for them further, which I don't want to change
    mb52_df_all_stocks = mb52_df.copy()
    mb52_df_all_stocks = mb52_df_all_stocks[(mb52_df_all_stocks['storage_location'].isin(ready_goods_storage_locs))
                                            & (~mb52_df_all_stocks['mat_number'].str.startswith('99'))]
    mb52_df_all_stocks = mb52_df_all_stocks[['mat_number', 'stock_quantity']]
    mb52_df_all_stocks = mb52_df_all_stocks.groupby('mat_number', as_index=False).sum()

    # Step 1: Merge the two DataFrames on 'mat_number' (left join to retain zsbe_df's rows)
    merged_df = pd.merge(zsbe_df, mb52_df_all_stocks, on='mat_number', how='left', suffixes=('_old', '_new'))

    # Step 2: Replace the stock_quantity in zsbe_df with the new stock_quantity
    # Fill missing values (NaNs) with 0 where no match is found
    merged_df['stock_quantity'] = merged_df['stock_quantity_new'].fillna(0)
    merged_df.drop(columns=['stock_quantity_old', 'stock_quantity_new'], inplace=True)

    # Result: zsbe_df now contains updated stock quantities (stocks from MB52 transaction)
    zsbe_df = merged_df

    # Here I ensure that mb52_df looks exactly as it's needed for confi stocks implementation
    mb52_df = mb52_df[mb52_df['mat_number'].str.startswith('99')].drop(columns=['plant'])
    # ===== End of MB52 stocks implementation ====

    # zsdkap_df = pd.read_excel(ZSDKAP_FILE_PATH, sheet_name='Sheet1', dtype=zsdkap_dtypes)
    # zsdkap_df = zsdkap_df.rename(columns=zsdkap_new_columns_names)
    # zsdkap_df['dispatch_date'] = pd.to_datetime(zsdkap_df['dispatch_date'], dayfirst=True, errors='coerce')
    zsdkap_df = pd.read_csv(ZSDKAP_FILE_PATH, dtype=zsdkap_dtypes, sep=';', encoding='MacRoman')
    zsdkap_df = zsdkap_df.rename(columns=zsdkap_new_columns_names)
    zsdkap_df['dispatch_date'] = pd.to_datetime(zsdkap_df['dispatch_date'], dayfirst=True, errors='coerce')

    # Przetwarzanie konkretnej kolumny
    zsdkap_df['orders_quantity'] = zsdkap_df['orders_quantity'].apply(clean_number)


    zsdkap_zsbe_merged_df = pd.merge(zsdkap_merged_df, zsbe_df, on='mat_number', how='outer')
    zsdkap_zsbe_merged_df.fillna(0, inplace=True)

    zsdkap_zsbe_mb5t_merged_df = pd.merge(zsdkap_zsbe_merged_df, mb5t_df, on='mat_number', how='left')
    zsdkap_zsbe_mb5t_merged_df = zsdkap_zsbe_mb5t_merged_df.rename(columns=mb5td_new_columns_names)
    zsdkap_zsbe_mb5t_merged_df.fillna(0, inplace=True)

    # Ensure stock quantities for confi items
    zsdkap_customer_orders_numbers_df = get_zsdkap_customer_orders_numbers(mrp_controller, mat_name, zsdkap_df)
    mb52_zsdkap_merged_df = pd.merge(zsdkap_customer_orders_numbers_df, mb52_df, on=('mat_number', 'customer_order_number', 'customer_order_position'), how='inner')
    mb52_zsdkap_merged_df = mb52_zsdkap_merged_df.groupby('mat_number', as_index=False).sum()
    mb52_zsdkap_merged_df = mb52_zsdkap_merged_df[['mat_number', 'stock_quantity', 'Opis']]

    merged = pd.merge(zsdkap_zsbe_mb5t_merged_df, mb52_zsdkap_merged_df, on='mat_number', how='left', suffixes=('_zsbe', '_mb52'))
    merged['stock_quantity'] = merged['stock_quantity_zsbe'].fillna(0) + merged['stock_quantity_mb52'].fillna(0)

    # Drop the temporary columns
    merged = merged.drop(columns=['stock_quantity_mb52'])

    for h in horizons:
        zsdkap_customer_orders_numbers_df = get_zsdkap_customer_orders_numbers(mrp_controller, mat_name, zsdkap_df,
                                                                               date_limit=get_nth_working_day(h))
        mb52_zsdkap_merged_df = pd.merge(zsdkap_customer_orders_numbers_df, mb52_df,
                                         on=('mat_number', 'customer_order_number', 'customer_order_position'),
                                         how='inner')
        mb52_zsdkap_merged_df = mb52_zsdkap_merged_df.groupby('mat_number', as_index=False).sum()
        mb52_zsdkap_merged_df = mb52_zsdkap_merged_df[['mat_number', 'stock_quantity']]
        mb52_zsdkap_merged_df = mb52_zsdkap_merged_df.rename(columns={'stock_quantity': 'stock_quantity_mb52'})

        merged = pd.merge(merged, mb52_zsdkap_merged_df, on='mat_number', how='left')

        # try:
        merged[f'stock_quantity_{h}_days'] = merged['stock_quantity_zsbe'].fillna(0) + merged['stock_quantity_mb52'].fillna(0)
        # except KeyError:
        #     merged[f'stock_quantity_{h}_days'] = merged[f'stock_quantity_{h}_days_df1'].fillna(0) + merged[f'stock_quantity'].fillna(0)
        # try:
            # Drop the temporary columns
        merged = merged.drop(columns=[f'stock_quantity_mb52'])
        # except KeyError:
        #     # Drop the temporary columns
        #     merged = merged.drop(columns=[f'stock_quantity_{h}_days_df1', f'stock_quantity'])

    zsdkap_zsbe_mb5t_merged_df = merged
    zsdkap_zsbe_mb5t_merged_df['to_be_produced_all'] = zsdkap_zsbe_mb5t_merged_df.apply(calculate_to_be_produced_all,
                                                                                        axis=1)

    zsdkap_zsbe_mb5t_merged_df['to_be_produced_gr_c'] = zsdkap_zsbe_mb5t_merged_df.apply(calculate_to_be_produced_gr_c,
                                                                                         axis=1)
    for h in horizons:
        zsdkap_zsbe_mb5t_merged_df[f'to_be_produced_gr_c_{h}_days'] = zsdkap_zsbe_mb5t_merged_df.apply(
            calculate_to_be_produced_gr_c, col_suffix=f"_{h}_days", axis=1)

    kpis = {"ORDERS LEVEL (ALL)": int(zsdkap_zsbe_mb5t_merged_df['to_be_produced_all'].sum()),
            "ORDERS LEVEL (GR C)": int(zsdkap_zsbe_mb5t_merged_df['to_be_produced_gr_c'].sum())}

    for h in horizons:
        kpis[f'ORDERS LEVEL (GR C - {h})'] = int(zsdkap_zsbe_mb5t_merged_df[f'to_be_produced_gr_c_{h}_days'].sum())

    zsdkap_zsbe_mb5t_merged_df.to_excel(f"C:/Temp/Kamil/Prywatne/07_Programowanie/99_Moje_projekty/28_PPS_KPI/excel_files/output/output_{'_'.join(mrp_controller)}.xlsx")
    return kpis


def kpis_loop(lines, mrp_controllers, product_names, zsdkap, zsbe, mb52, mb5t, horizons, storage_locs, result_file_sheet, include_zkbp1_sb=False, zkbp1_report_name="ZKBP1_SB_0301"):
    try:
        for line, mrp, prd_name in zip(lines, mrp_controllers, product_names):
            kpis_result = calculate_order_level_KPI(zsdkap_report_name=zsdkap, zsbe_report_name=zsbe, mb52_report_name=mb52, mb5t_report_name=mb5t,
                                                    horizons=horizons, mrp_controller=mrp, mat_name=prd_name, ready_goods_storage_locs=storage_locs,
                                                    include_zkbp1_sb= include_zkbp1_sb, zkbp1_report_name=zkbp1_report_name)
            kpis_result["LINE"] = line

            append_data_to_excel(
                status_file=KPIS_FILE_PATH,
                data_dict=kpis_result,
                error_path=ERROR_PATH,
                sheet_name=result_file_sheet
            )
    except Exception as e:
        print("Błąd: ", e)
        error_details = traceback.format_exc()
        print("Szczegóły błędu:\n", error_details)
        input("Press Enter...")


def wmo_kpis():
    today = datetime.today()
    today_str = today.strftime('%Y-%m-%d')

    zsdkap = generate_zsdkap_filename()
    zsbe = 'zsbe_wmo'
    mb52 = 'mb52'
    mb5t = "MB5TD_2101"
    result_sheet = "LUB"

    storage_locs = ('0004', '0005', 'FSC')

    horizons = [3, 5, 10]

    lines = ["P100", "M200", "M300", "M320", "M500", "M600"]
    mrp_controllers = ['L1K', ('L1H', 'L41', 'L3H', 'L82'), ('L3H', 'L82'), 'L2H', 'LD1', 'LZ1']
    product_names = [('R4', 'R7', 'R3', 'R5', 'EFL_R4', 'EFL_R7'), ('R4', 'R7', 'R3', 'R5', 'EFL_R4', 'EFL_R7', 'EFL 4', 'EFL 7'), ('R6', 'R8', 'EFL_R6', 'EFL_R8', 'EFL 6', 'EFL 8'), ('Q4', 'EFL_Q'), 'R2', ('ZI', 'KO', 'Li')]  # Product names starts with...

    # lines = ["P100"]
    # mrp_controllers = ['L1K']
    # product_names = [('R4', 'R7', 'R3', 'R5')]  # Product names starts with...
    kpis_loop(lines, mrp_controllers, product_names, zsdkap, zsbe, mb52, mb5t, horizons, storage_locs, result_sheet, False)

def wmr_kpis():
    today = datetime.today()
    today_str = today.strftime('%Y-%m-%d')

    zsdkap = generate_zsdkap_filename()
    zsbe = 'zsbe_wmr'
    mb52 = 'mb52'
    mb5t = "MB5TD_2101"
    result_sheet = "LUB"

    storage_locs = ('0004', '0005', 'FSC', '0003')

    horizons = [3, 5, 10]

    lines = ["ZRV", "ZJA", "ZFA", "ZRI", "ZAR"]
    mrp_controllers = [('L2E', 'L2V', 'LI1', 'LI3'), ('L2J', 'LI5', 'LI8'), ('L2F', 'LI6'), 'L2I', ('L2B', 'L2R', 'LI2', 'LI4', 'LI7')]
    product_names = [('ZRE_M', 'ZRE M', 'ZRV_M', 'ZRV M'), ('ZJA', 'ZRE_E', 'ZRE E', 'ZRV_E', 'ZRV E'), 'ZFA', 'ZRI', ('ZAR', 'Auss', 'BHG', 'ZRS')]  # Product names starts with...

    kpis_loop(lines, mrp_controllers, product_names, zsdkap, zsbe, mb52, mb5t, horizons, storage_locs, result_sheet, False)

def mont_kpis():
    '''
    BMH KPIs
    '''
    today = datetime.today()
    today_str = today.strftime('%Y-%m-%d')

    zsdkap = generate_zsdkap_filename()
    zsbe = 'zsbe_mont'
    mb52 = 'mb52'
    mb5t = "MB5TD_0301"
    zkbp1_report_name = "ZKBP1_SB_0301"
    result_sheet = "LUB"

    storage_locs = ('0004', '0005', 'FSC', '0003', '0007')

    horizons = [3, 5, 10]

    lines = ["WDF68K", "WDFQK", "ZRO", "QR1", "EDR"]
    mrp_controllers = [
        ('M81', 'M82'),
        ('MQ1', 'MQ2'),
        ('MR1', 'MR2', 'MR3'),
        "MR4",
        ('MEB', 'MED', 'MEE', 'MEI', 'MEH', 'MEJ', 'MEM', 'MEN', 'MEX')
    ]

    product_names = [
        ('R6', 'R8', 'I8', 'EFL', 'ABR'),
        ('Q4', 'QRA', 'Qt4', 'EFL', 'ABR'),
        ('ZRO', 'ZMA'),
        "ZRO",
        ('ED', 'EF', 'EA')
    ]
    # Product names starts with...

    kpis_loop(lines, mrp_controllers, product_names, zsdkap, zsbe, mb52, mb5t, horizons, storage_locs, result_sheet,
              True, zkbp1_report_name)

if __name__ == "__main__":

    department = sys.argv[1]
    if department == 'wmo':
        wmo_kpis()
    elif department == 'wmr':
        wmr_kpis()
    elif department == 'mont':
        mont_kpis()
    # mont_kpis()
    # wmo_kpis()
    # wmr_kpis()
