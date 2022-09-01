from sqlalchemy import create_engine
from backtrader.feeds import PandasData  # 用于扩展DataFeed
import pandas as pd
from backtrader.feeds import PandasData  # 用于扩展DataFeed
# from MyTT import *

class PandasDataExtend(PandasData):
    # 增加pe线
    lines = ('pe', 'PricePer', 'VolPer', 'PricePositiveCount', 'CircMV', 'TotalMV')

    params = (('pe', 2), ('PricePer', 2), ('VolPer', 2), ('PricePositiveCount', 2),
              ('CircMV', 2), ('TotalMV', 2))


def get_btPandasDataExtend(key, tblname, s_date,e_date):
    cnboard_sqlite_cnn = DB_CONNECT_OPEN('cn_board_engine')
    global_sqlite_cnn = DB_CONNECT_OPEN('global_engine')

    read_sing_sql = """SELECT * from {} singtbl where Code = '{}'""".format(tblname, key)
    read_day_sql = """SELECT * from {} daytbl where Code = '{}'""".format('CNDAY_Y2020', key)
    org_singu_df00 = pd.read_sql_query(read_sing_sql, cnboard_sqlite_cnn)
    org_day_df00 = pd.read_sql_query(read_day_sql, cnboard_sqlite_cnn)

    org_singu_df01 = pd.merge(org_singu_df00, org_day_df00[['Date', 'Code', 'CircMV', 'TotalMV']], on=['Date', 'Code'],
                              how='left', suffixes=('', '_'))
    org_singu_df01['Date'] = pd.to_datetime(org_singu_df01['Date'])
    org_singu_df02 = org_singu_df01.sort_values(by='Date', ascending=True)
    org_singu_df = org_singu_df02.dropna(subset=['Close'])
    org_singu_df['PricePer'] = np.round(DIFF(org_singu_df['Close'], N=1) / REF(org_singu_df['Close'], N=1), 3)
    org_singu_df['VolPer'] = np.round(DIFF(org_singu_df['Vol'], N=1) / REF(org_singu_df['Vol'], N=1), 3)
    # org_singu_df['PricePositiveCount'] = COUNT(org_singu_df['PricePer'] > PriceGrowPer['low'], last_n_days)

    btdata = PandasDataExtend(
        dataname=org_singu_df,
        datetime='Date',  # 日期行所在列
        open='Open',  # 开盘价所在列
        high='High',  # 最高价所在列-1 : autodetect position or case-wise equal name
        low='Low',  # 最低价所在列
        close='Close',  # 收盘价价所在列
        volume='Vol',  # 成交量所在列
        PricePer='PricePer',
        VolPer='VolPer',
        TotalMV='TotalMV',
        CircMV='CircMV',
        openinterest=-1,  # 无未平仓量列.(openinterest是期货交易使用的)
        fromdate=s_date,  # 起始日
        todate=e_date,  # 结束日
        # plot= singu_plot_flag,
        # timeframe=bt.TimeFrame.Days  # 月线数据
    )
    return btdata


def DB_CONNECT_OPEN(str):
    numbers = {
        'global_engine': create_engine(r'sqlite:///D:\PythonSpace\database\MyQuantDB.db'),
        'cn_board_engine': create_engine(r'sqlite:///D:\PythonSpace\database\MyQuantDB.db'),
        # 'cn_daily_engine' : create_engine(r'sqlite:///D:\PythonSpace\MyQuant\00_database\MyQuantCN_Day.db')
    }

    return numbers.get(str, None)


def DB_CONNECT_CLOSE(global_sqlite_cnn, cnboard_sqlite_cnn, cndaily_sqlite_cnn):
    global_sqlite_cnn.close()
    cnboard_sqlite_cnn.close()
    cndaily_sqlite_cnn.close()


if __name__ == '__main__':
    DB_CONNECT_OPEN()
