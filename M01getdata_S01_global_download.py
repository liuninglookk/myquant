import akshare as ak
import tushare as ts
import pandas as pd
import datetime
import  os, arrow
from sqlalchemy import create_engine

# https://www.akshare.xyz/tutorial.html?highlight=%E7%BE%8E%E8%82%A1#id1
def tdshtstr(dayv):
    return '{0}{1}{2}'.format(dayv.strftime('%Y'), dayv.strftime('%m'), dayv.strftime('%d'))


def getstockdic(dic_file, datapath):

    df_dict = pd.read_csv(dic_file, encoding='gbk')

    for i in df_dict.index:
        # .copy
        df_dict[datapath][i] = df_dict[datapath][i]
        data_dict = dict(zip(df_dict['Code'], df_dict[datapath]))

    return data_dict


def dfrename(df00):
    # df列名批量转化为小写：
    # org_project.columns=org_project.columns.map(lambda x:x.lower())
    rename_map01 = {'今开': 'Open', '最新价': 'Close', '最高': 'High', '最低': 'Low', '成交量': 'Vol', '成交额': 'Amount',
                    '昨收': 'Pre_Close'}
    rename_map02 = {'trade_date': 'Date', 'date': 'Date', 'open': 'Open', 'close': 'Close', 'high': 'High',
                    'low': 'Low', 'vol': 'Vol', 'volume': 'Vol', 'amount': 'Amount'}
    rename_map03 = {'日期': 'Date', '交易时间': 'Date', '晚盘价': 'Close', '收盘': 'Close', '早盘价': 'Open', '开盘': 'Open'}
    rename_map04 = {'ts_code': 'Code', 'bid_open': 'Open', 'bid_close': 'Close', 'bid_high': 'High', 'bid_low': 'Low'}
    rename_map05 = {'振幅': 'Amplitude', '涨跌幅': 'PricePercent', '涨跌额': 'PriceDiff', '换手率': 'Turnover_rate'}
    rename_map06 = {'ts_code': 'Code', 'total_mv': 'TotalMV', 'circ_mv': 'CircMV', 'turnover_rate': 'Turnover_rate'}

    df01 = df00.rename(columns=rename_map01)
    df02 = df01.rename(columns=rename_map02)
    df03 = df02.rename(columns=rename_map03)
    df04 = df03.rename(columns=rename_map04)
    df05 = df04.rename(columns=rename_map05)
    df06 = df05.rename(columns=rename_map06)

    df06["Date"] = pd.to_datetime(df06['Date']).dt.date

    return df06


def dfreduce(df00, start_day, end_day):
    df00['Date'] = pd.to_datetime(df00['Date']).dt.date
    df01 = df00[df00['Date'] >= start_day]

    df02 = df01[df00['Date'] <= end_day]

    return df02


def cn_stock_now_minute():
    # 分钟级别
    # 限量: 实时行情数据-东财,单次返回所有沪深京 A 股上市公司的实时行情数据
    # ,序号,代码,名称,最新价,涨跌幅,涨跌额,成交量,成交额,振幅,最高,最低,今开,昨收,量比,换手率,市盈率-动态,市净率
    nowstr = arrow.now().format('YYYY-MM-DD HHmmss')
    stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
    df_new = dfrename(stock_zh_a_spot_em_df)
    fname = 'S00-download/chn-stock-M-{0}.csv'.format(nowstr)
    df_new.to_csv(fname, encoding='utf_8_sig', index=False)


def singl_stock_byday(code, startdate, enddate):
    # 日期,开盘,收盘,最高,最低,成交量,成交额,振幅,涨跌幅,涨跌额,换手率,start_date="20100101"
    stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=startdate, end_date=enddate,
                                            adjust="")

    if stock_zh_a_hist_df.shape[1] > 0:
        df_new = dfrename(stock_zh_a_hist_df)

    if stock_zh_a_hist_df.shape[1] == 0:
        df_new = pd.DataFrame()

    return df_new


def batch_singl_stock_byday(sday, eday):
    cnstock_data_dict = getstockdic('cn', 'TblName')
    table_name = 'STOCKS'
    for key, value in cnstock_data_dict.items():
        df = singl_stock_byday(key[0:6], sday, eday)
        df["Code"] = key
        df_new0 = dfrename(df)
        df_new = df_new0[['Code', 'Date', 'Open', 'Close', 'High', 'Low', 'Vol', 'Amount', 'Amplitude', 'PricePercent',
                          'Turnover_rate']]
        df_new.sort_values(by=["Date"], ascending=True, inplace=True)
        df_new.to_sql(table_name, sqlite_conn, index=False, if_exists='append')


def batch_cnstocks_byday(s_date, e_date):
    ts.set_token('76aadb6dd21f96547fe4a877f07248a25d31503f4b728eb59e1476b4')
    pro = ts.pro_api()
    df_opendate = pro.trade_cal(exchange='SSE', is_open='1',
                                start_date=s_date, end_date=e_date,
                                fields='cal_date')
    table_name = 'CNDAY'
    for date in df_opendate['cal_date'].values:
        df = pro.daily_basic(ts_code='', trade_date=date, fields='')
        # TS股票代码,交易日期,当日收盘价,换手率（%）,换手率（自由流通股）,量比,市盈率（总市值/净利润， 亏损的PE为空）,市盈率（TTM，亏损的PE为空）,市净率（总市值/净资产）,市销率,市销率（TTM）,股息率 （%）,股息率（TTM）（%）,  总股本 （万股）,流通股本 （万股）,自由流通股本 （万）,总市值 （万元）,流通市值（万元）
        # ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,   total_share,float_share,free_share,total_mv,circ_mv
        if df.shape[0] > 0:
            df_01 = dfrename(df)
            df_01.sort_values(by=["Code"], ascending=True, inplace=True)
            combine_df = combine_df.append(df_01, ignore_index=True)

    combine_df.to_sql(table_name, sqlite_conn, index=False, if_exists='append')


def globaldownload():
    # stock_data_dict = getstockdic('global', 'download-data')

    endstr = tdshtstr(datetime.date.today())
    ts.set_token('76aadb6dd21f96547fe4a877f07248a25d31503f4b728eb59e1476b4')
    pro = ts.pro_api()
    table_name = 'GLOBAL'

    orgdf_ixic = pro.index_global(ts_code='IXIC', start_date='20100201', end_date=endstr)
    df_ixic = dfrename(orgdf_ixic)
    df_ixic.to_sql(table_name, sqlite_conn, index=False, if_exists='append')

    orgdf_dji = pro.index_global(ts_code='DJI', start_date='20100201', end_date=endstr)
    df_dji = dfrename(orgdf_dji)
    df_dji['Date'] = pd.to_datetime(df_dji['Date']).dt.date
    table_name = 'global'
    df_dji.to_sql(table_name, sqlite_conn, index=False, if_exists='append')

    # 目标地址: http://quote.eastmoney.com/center/hszs.html
    orgdf_sh000001 = ak.stock_zh_index_daily_em(symbol="sh000001")
    t1_df_sh000001 = dfrename(orgdf_sh000001)
    df_sh000001 = dfreduce(t1_df_sh000001, start_day=datetime.date(2010, 1, 1), end_day=datetime.date.today())
    df_sh000001['Code'] = 'sh000001'
    df_sh000001.to_sql(table_name, sqlite_conn, index=False, if_exists='append')

    orgdf_sz399001 = ak.stock_zh_index_daily_em(symbol="sz399001")
    t1_df_sz399001 = dfrename(orgdf_sz399001)
    df_sz399001 = dfreduce(t1_df_sz399001, start_day=datetime.date(2010, 1, 1), end_day=datetime.date.today())
    df_sz399001['Code'] = 'sz399001'
    df_sz399001.to_sql(table_name, sqlite_conn, index=False, if_exists='append')

    "spot_golden_benchmark_sge"  # 上海黄金交易所-上海金基准价
    orgdf_sge = ak.spot_golden_benchmark_sge()
    df_sge = dfrename(orgdf_sge)
    df_sge['Code'] = 'sge-gold'
    df_sge.to_sql(table_name, sqlite_conn, index=False, if_exists='append')
    #
    orgdf_brnoil = ak.futures_foreign_hist(symbol="OIL")
    df_brnoil0 = dfrename(orgdf_brnoil)
    df_brnoil0['Code'] = 'BRN0Y'
    df_brnoil = df_brnoil0[['Code', 'Date', 'Open', 'High', 'Low', 'Vol']]
    df_brnoil.to_sql(table_name, sqlite_conn, index=False, if_exists='append')

    # 获取美元人民币交易对的日线行情
    orgdf_fxcm = pro.fx_daily(ts_code='USDCNH.FXCM', start_date='20100101', end_date=endstr)
    df_fxcm0 = dfrename(orgdf_fxcm)
    df_fxcm0['Code'] = 'USDCNH.FXCM'
    df_fxcm = df_fxcm0[['Code', 'Date', 'Open', 'Close', 'High', 'Low']]
    df_fxcm.to_sql(table_name, sqlite_conn, index=False, if_exists='append')


if __name__ == '__main__':
    # engine = create_engine(r'sqlite:///D:\PythonSpace\MyQuant\00_database\MyQuantGlobal.db')
    engine = create_engine(r'sqlite:///D:\PythonSpace\MyQuant\00_database\MyQuantCN_Day.db')
    # engine = create_engine(r'sqlite:///D:\PythonSpace\MyQuant\00_database\MyQuantCN_Stock.db')
    sqlite_conn = engine.connect()
    if False:
        sqlite_conn.execute('''CREATE TABLE CNDAILY
            (
            Code   TEXT,
            Date   DATE,
            Open   FLOAT,
            Close  FLOAT,
            High   FLOAT,
            Low    FLOAT,
            Vol    BIGINT,
            Amount BIGINT,
            pre_close   Float, 
            change   Float, 
            pct_chg   Float, 
            swing   Float
            );''')

    if True:
        sqlite_conn.execute('''CREATE TABLE CNDAY
            (
            Code   TEXT,
            Date   DATE,
            Open   FLOAT,
            Close  FLOAT,
            High   FLOAT,
            Low    FLOAT,
            Vol   FLOAT,
            Amount   FLOAT,
            Turnover_rate  FLOAT,	
            turnover_rate_f  FLOAT,	
            volume_ratio  FLOAT,	
            pe  FLOAT,	
            pe_ttm  FLOAT,
            pb  FLOAT,
            ps  FLOAT,	
            ps_ttm  FLOAT,	
            dv_ratio  FLOAT,
            dv_ttm  FLOAT,	
            total_share   FLOAT,
            float_share   FLOAT,
            free_share   FLOAT,
            TotalMV   FLOAT,
            CircMV   FLOAT
            );''')

    if False:
        sqlite_conn.execute('''CREATE TABLE STOCKS
            (
            Code   TEXT,
            Date   DATE,
            Open   FLOAT,
            Close  FLOAT,
            High   FLOAT,
            Low    FLOAT,
            Vol    FLOAT,
            Amount    FLOAT,
            Amplitude   FLOAT,
            PricePercent   FLOAT,
            Turnover_rate   FLOAT
            );''')
    # globaldownload()
    batch_cnstocks_byday('20120101', '20131231')
    # batch_singl_stock_byday("20120101", "20131231")
    sqlite_conn.close()
