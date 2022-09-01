import streamlit as st

st.markdown("# Page 2 ❄️")
st.sidebar.markdown("# Page 2 ❄️")

import streamlit as st
import plotly.figure_factory as ff
import numpy as np
import plotly.express as px
from datetime import datetime

from M01getdata_S01_global_download import *
from M01getdata_S02_db_connect import *
import configparser



def cn_singl_syn(part):
    if part not in 'STOCKS_CN00,STOCKS_CN30,STOCKS_CN60':
        return
    # 个股文件追加更新
    cn_stock_data_dict00 = getstockdic('cn', 'TblName')
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

config = configparser.ConfigParser()
config.read("config.ini", encoding="utf-8")
config.sections()  # 获取section节点
config.options('myquant')  # 获取指定section 的options即该节点的所有键
dicfile = config.get("myquant", "globaldict_path")  # 获取指定section下的options
db_path = config.get("myquant", "db_path")
cnboard_sqlite_cnn = create_engine('sqlite:///'+ db_path)

# Add histogram data
x1 = np.random.randn(200) - 2
x2 = np.random.randn(200)
x3 = np.random.randn(200) + 2

# Group data together
hist_data = [x1, x2, x3]

group_labels = ['Group 1', 'Group 2', 'Group 3']

# Create distplot with custom bin_size
fig = ff.create_distplot(
         hist_data, group_labels, bin_size=[.1, .25, .5])

# Plot!
st.plotly_chart(fig, use_container_width=True)



df = px.data.gapminder()
st.write(df)


fig2 = px.scatter(df.query("year==2007"), x="gdpPercap", y="lifeExp",size="pop",
                  color="continent",hover_name="country", log_x=True, size_max=60)

st.plotly_chart(fig2, use_container_width=True)



df02 = px.data.gapminder()
fig02 = px.scatter(df02.query("year==2007"), x="gdpPercap", y="lifeExp",size="pop",
                  color="continent",hover_name="country", log_x=True, size_max=60)

st.plotly_chart(fig02, use_container_width=True)