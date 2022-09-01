import datetime

from M01getdata_S01_global_download import *
from M01getdata_S02_db_connect import *
import akshare as ak
import tushare as ts
import pandas as pd

from os import path
import time
import sys, os
import configparser

# pip install akshare  --upgrade

def cntimezone_global_syn():
    # 期货文件追加更新
    functime = datetime.datetime.now()
    cn_global_dic = {'sh000001': 1, 'sz399001': 2}
    for key, value in cn_global_dic.items():
        tbl = global_stock_tbname_dict[key]
        read_sql = """SELECT Date from {} where Code = '{}'""".format(tbl, key)
        org_sh000001_df = pd.read_sql_query(read_sql, global_sqlite_cnn)
        org_sh000001_date = pd.to_datetime(org_sh000001_df['Date'], format='%Y-%m-%d')
        t = org_sh000001_date.max()
        org_sh000001_lastday = datetime.date(2010, 1, 1) if pd.isnull(t) else t
        if org_sh000001_lastday < runday_cn:
            org_sh000001_nextday = org_sh000001_lastday + datetime.timedelta(days=1)
            down_sh000001_df00 = ak.stock_zh_index_daily_em(symbol=key)
            down_sh000001_df01 = dfrename(down_sh000001_df00)
            down_sh000001_df = dfreduce(down_sh000001_df01, start_day=org_sh000001_nextday, end_day=runday_cn)
            down_sh000001_df['Code'] = key
            down_sh000001_df['timestamp'] = functime
            if down_sh000001_df.shape[0] > 0:
                down_sh000001_df.to_sql(tbl, global_sqlite_cnn, index=False, if_exists='append')

    # fxcm_tbl = global_stock_tbname_dict['USDCNH.FXCM']
    # read_fxcm_sql = """SELECT Date from {} where Code = '{}'""".format(fxcm_tbl, 'USDCNH.FXCM')
    # org_fxcm_df = pd.read_sql_query(read_fxcm_sql, global_sqlite_cnn)
    # org_fxcm_date = pd.to_datetime(org_fxcm_df['Date'], format='%Y-%m-%d')
    # t = org_fxcm_date.max()
    # org_fxcm_lastday = datetime.date(2010, 1, 1) if pd.isnull(t) else t
    # if org_fxcm_lastday < runday_cn:
    #     org_nextday_str = tdshtstr(org_fxcm_lastday + datetime.timedelta(days=1))
    #     # 获取美元人民币交易对的日线行情
    #     down_fxcm_df00 = pro.fx_daily(ts_code='USDCNH.FXCM', start_date=org_nextday_str, end_date=nowdate_cn_str)
    #     if down_fxcm_df00.shape[0] > 0:
    #         down_fxcm_df = dfrename(down_fxcm_df00)
    #         down_fxcm_df['Code'] = 'USDCNH.FXCM'
    #         df2 = down_fxcm_df[['Code', 'Date', 'Open', 'Close', 'High', 'Low']]
    #         df2['timestamp'] = functime
    #         df2.to_sql(fxcm_tbl, global_sqlite_cnn, index=False, if_exists='append')

    sge_tbl = global_stock_tbname_dict['sge_gold']
    read_sge_sql = """SELECT Date from {} where Code = '{}'""".format(sge_tbl, 'sge_gold')
    org_sge_df = pd.read_sql_query(read_sge_sql, global_sqlite_cnn)
    org_sge_date = pd.to_datetime(org_sge_df['Date'], format='%Y/%m/%d')
    t = org_sge_date.max()
    org_sge_lastday = datetime.date(2010, 1, 1) if pd.isnull(t) else t
    if org_sge_lastday < runday_cn:
        org_sge_nextday = org_sge_lastday + datetime.timedelta(days=1)
        down_sge_df00 = ak.spot_golden_benchmark_sge()
        # "spot_golden_benchmark_sge" # 上海黄金交易所-上海金基准价
        down_sge_df01 = dfrename(down_sge_df00)
        down_sge_df = dfreduce(down_sge_df01, start_day=org_sge_nextday, end_day=runday_cn)
        if down_sge_df.shape[0] > 0:
            down_sge_df['Code'] = 'sge-gold'
            down_sge_df['timestamp'] = functime
            down_sge_df.to_sql(sge_tbl, global_sqlite_cnn, index=False, if_exists='append')

    brnoil_tbl = global_stock_tbname_dict['BRN0Y']
    read_brnoil_sql = """SELECT Date from {} where Code = '{}'""".format(brnoil_tbl, 'BRN0Y')
    org_brnoil_df = pd.read_sql_query(read_brnoil_sql, global_sqlite_cnn)
    org_brnoil_date = pd.to_datetime(org_brnoil_df['Date'], format='%Y/%m/%d')
    t = org_brnoil_date.max()
    org_brnoil_lastday = datetime.date(2010, 1, 1) if pd.isnull(t) else t
    if org_brnoil_lastday < runday_cn:
        org_brnoil_nextday = org_brnoil_lastday + datetime.timedelta(days=1)
        down_brnoil_df00 = ak.futures_foreign_hist(symbol="OIL")
        down_brnoil_df01 = dfrename(down_brnoil_df00)
        down_brnoil_df02 = dfreduce(down_brnoil_df01, start_day=org_brnoil_nextday, end_day=runday_cn)
        down_brnoil_df = down_brnoil_df02[['Date', 'Open', 'High', 'Low', 'Close', 'Vol']]
        if down_brnoil_df.shape[0] > 0:
            down_brnoil_df['Code'] = 'BRN0Y'
            down_brnoil_df['timestamp'] = functime
            down_brnoil_df.to_sql(brnoil_tbl, global_sqlite_cnn, index=False, if_exists='append')

    print('global futures sync finished')


def ustimezone_global_syn():
    functime = datetime.datetime.now()
    # 美股大盘文件追加更新
    us_global_dic = {'IXIC': 1, 'DJI': 2}
    for key, value in us_global_dic.items():
        tbl = global_stock_tbname_dict[key]
        read_sql = """SELECT Date from {} where Code = '{}'""".format(tbl, key)
        org_ixic_df = pd.read_sql_query(read_sql, global_sqlite_cnn)
        org_ixic_date = pd.to_datetime(org_ixic_df['Date'], format='%Y-%m-%d')
        t = org_ixic_date.max()
        db_ixic_lastday = datetime.date(2010, 1, 1) if pd.isnull(t) else t
        if db_ixic_lastday < runday_cn:
            db_ixic_nextday = db_ixic_lastday + datetime.timedelta(days=1)
            down_ixic_df00 = pro.index_global(ts_code=key, start_date=tdshtstr(db_ixic_nextday),
                                              end_date=e_date_us_str)
            if down_ixic_df00.shape[0] > 0:
                down_ixic_df = dfrename(down_ixic_df00)
                down_ixic_df['timestamp'] = functime
                down_ixic_df.to_sql(tbl, global_sqlite_cnn, index=False, if_exists='append')


def cn_singl_syn(part):
    if part not in 'STOCKS_CN00,STOCKS_CN30,STOCKS_CN60':
        return
    # 个股文件追加更新
    cn_stock_data_dict00 = getstockdic(cndic_file, 'TblName')
    cn_stock_tbl_dict = {k: v for k, v in cn_stock_data_dict00.items() if v == part}
    singl_tbl = part
    read_db_sql = """SELECT Code,Date from {} """.format(singl_tbl )
    org_db_df = pd.read_sql_query(read_db_sql, cnboard_sqlite_cnn)
    org_db_df['Date'] = pd.to_datetime(org_db_df['Date'], format='%Y-%m-%d')

    functime = datetime.datetime.now()
    down_all_df=pd.DataFrame()
    for key, value in cn_stock_tbl_dict.items():
        org_singu_df = org_db_df[org_db_df["Code"] == key]
        org_singu_date = org_singu_df['Date']
        t = org_singu_date.max()
        org_singu_lastday = datetime.date(2010, 1, 1) if pd.isnull(t) else t
        if org_singu_lastday < pd.to_datetime(runday_cn):
            org_singu_nextday = org_singu_lastday + datetime.timedelta(days=1)
            down_singu_df00 = singl_stock_byday(key[0:6], tdshtstr(org_singu_nextday), e_date_cn_str)
            if down_singu_df00.shape[0] > 0:
                down_singu_df00["Code"] = key
                down_singu_df01 = down_singu_df00[
                    ["Code", "Date", "Open", "Close", "High", "Low", "Vol", "Amount", "Amplitude", "PricePercent",
                     "Turnover_rate"]]
                down_singu_df = dfrename(down_singu_df01)
                down_singu_df['timestamp'] = functime
                down_all_df = pd.concat([down_all_df,down_singu_df])

    down_all_df.to_sql(singl_tbl, cnboard_sqlite_cnn, index=False, if_exists='append')
    print('China singl stocks sync finished')


def cn_daily_down_syn(tbl):
    functime = datetime.datetime.now()
    # 大盘单日下载 ！
    read_sql = """SELECT Date from {} """.format(tbl)
    org_cnday_df = pd.read_sql_query(read_sql, cnboard_sqlite_cnn)
    org_cnday_date = pd.to_datetime(org_cnday_df['Date'], format='%Y-%m-%d')
    t = org_cnday_date.max()
    db_cnday_lastday = datetime.date(2015, 1, 1) if pd.isnull(t) else t
    # endday = datetime.date(2019, 12, 31)
    while db_cnday_lastday < runday_cn:
        db_cnday_lastday = db_cnday_lastday + datetime.timedelta(days=1)
        down_cnday_df00 = pro.daily_basic(ts_code='', trade_date=tdshtstr(db_cnday_lastday), fields='')
        if down_cnday_df00.shape[0] > 0:
            down_cnday_df = dfrename(down_cnday_df00)
            down_cnday_df.sort_values(by=["Code"], ascending=True, inplace=True)
            down_cnday_df.to_sql(tbl, cnboard_sqlite_cnn, index=False, if_exists='append')


def convertCloseGlobal():
    # temporary table
    read_date_sql = """SELECT Date from {} """.format('Global')
    org_date_df = pd.read_sql_query(read_date_sql, global_sqlite_cnn)
    org_sge_date = pd.to_datetime(org_date_df['Date'], format='%Y/%m/%d')
    tstart = org_sge_date.min()
    tend = org_sge_date.max()
    end = datetime.date(2021, 1, 1) if pd.isnull(tend) else tend
    # end = datetime.date.today()

    merge_fill_df = pd.DataFrame()
    merge_fill_df['Date'] = pd.date_range(tstart, end)
    # merge_fill_df['Date'] = pd.to_datetime(merge_fill_df['Date'], format='%Y-%m-%d').dt.date
    merge_fill_df['Date'] = pd.to_datetime(merge_fill_df['Date'], format='%Y-%m-%d')

    for key, value in stock_displ_dict.items():
        tbl = global_stock_tbname_dict[key]
        read_sql = """SELECT Date,Close as '{}' from {} where Code = '{}' order by Date""".format(key, tbl, key)
        one_df = pd.read_sql_query(read_sql, global_sqlite_cnn)
        one_df['Date'] = pd.to_datetime(one_df['Date'], format='%Y-%m-%d')
        merge_fill_df = pd.merge(one_df, merge_fill_df, how='outer', on='Date')
        merge_fill_df.sort_values(by=["Date"], ascending=True, inplace=True)
        merge_fill_df = merge_fill_df.fillna(method='ffill')

    xdata00 = merge_fill_df['Date']
    # Convert datetime.datetime xdata to milliseconds since the epoch
    merge_fill_df['ms_epoch'] = [time.mktime(s.timetuple()) * 1000 for s in xdata00]

    purge_sql = """delete from Close_Global """
    cnboard_sqlite_cnn.connect().execute(purge_sql)

    merge_fill_df.to_sql('Close_Global', cnboard_sqlite_cnn, index=False, if_exists='append')
    print('convert Close Global')


if __name__ == '__main__':

    config = configparser.ConfigParser()
    config.read("config.ini", encoding="utf-8")
    config.sections()  # 获取section节点
    config.options('myquant')  # 获取指定section 的options即该节点的所有键
    project_folder = config.get("myquant", "project_folder")
    synlog_file = config.get("myquant", "synlog_path")
    dicfile = config.get("myquant", "globaldict_path")  # 获取指定section下的options
    cndic_file = config.get("myquant", "cndict_path")
    db_path = config.get("myquant", "db_path")
    cnday_tbl = config.get("dbtbl", "cnday")


    df_dict = pd.read_csv(dicfile, encoding='gbk')

    syn_part = sys.argv[1]
    # syn_part = 'STOCKS_CN00'

    stock_displ_dict = dict(zip(df_dict['Code'], df_dict['displ']))
    global_stock_tbname_dict = getstockdic(dicfile, 'TblName')
    ts.set_token('76aadb6dd21f96547fe4a877f07248a25d31503f4b728eb59e1476b4')
    pro = ts.pro_api()

    global_sqlite_cnn = create_engine('sqlite:///'+ db_path)
    cnboard_sqlite_cnn = create_engine('sqlite:///'+ db_path)

    runday_cn = datetime.date.today()  # cn 1201
    yd_cn = runday_cn + datetime.timedelta(days=-1)  # cn 1130
    td_us = runday_cn + datetime.timedelta(days=-1)  # us 1130
    nowdate_cn_str = tdshtstr(runday_cn)
    e_date_cn_str = tdshtstr(yd_cn)
    e_date_us_str = tdshtstr(td_us)

    if syn_part in 'cnday,xxzx':
        cn_daily_down_syn(cnday_tbl)

    if syn_part in 'global,yyy':
        ustimezone_global_syn()
        cntimezone_global_syn()
        convertCloseGlobal()

    if syn_part in 'STOCKS_CN00,STOCKS_CN30,STOCKS_CN60':
        cn_singl_syn(syn_part)
        # global_sqlite_cnn.close()
        # cnboard_sqlite_cnn.close()
    if syn_part in 'STOCKS_CN00,STOCKS_CN60,STOCKS_CN30,cnday,global':

        with open(synlog_file, 'a+') as f:
        # with open('d:\\run.log', 'a+') as f:
            f.write("""'{}' {} dbsyn done  """.format(datetime.datetime.now(),syn_part))
            f.write('\n')
            f.close()
        exit()


