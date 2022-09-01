"""
读取本地html形式
"""

import streamlit as st
from nvd3 import cumulativeLineChart

from M01getdata_S01_global_download import *
from M01getdata_S02_db_connect import *
import configparser


def growth_chart(mkt_str, start_date, end_date):
    read_sql = """SELECT Date, ms_epoch, {} from {}  order by Date""".format(mkt_str, 'Close_Global')
    org_df = pd.read_sql_query(read_sql, global_sqlite_cnn)
    org_df = dfreduce(org_df, start_day=start_date, end_day=end_date)
    org_df['Date'] = pd.to_datetime(org_df['Date'], format='%Y-%m-%d')

    xdata = org_df['ms_epoch'].tolist()

    # Open File to write the D3 Graph
    output_file = open('global_grow.html', 'w')

    chart = cumulativeLineChart(name='CumulativeChart', heigh=600, width=800, x_is_date=True,
                                use_interactive_guideline=True,
                                # x_axis_format='%b %Y'
                                )
    chart.set_containerheader("\n\n<h2>" + "Global Market Grow" + "</h2>\n\n")

    df_dict = pd.read_csv(dicfile, encoding='gbk')
    stock_displ_dict = dict(zip(df_dict['Code'], df_dict['displ']))
    #
    for key, value in stock_displ_dict.items():
        if key in mkt_str:
            ydata20 = org_df[key].tolist()
            extra_serie = {"tooltip": {"y_start": "There are ", "y_end": " calls"}}
            chart.add_serie(name=key, y=ydata20, x=xdata, extra=extra_serie)

    chart.buildhtml()
    output_file.write(chart.htmlcontent)
    output_file.close()


config = configparser.ConfigParser()
config.read("config.ini", encoding="utf-8")
config.sections()  # 获取section节点
config.options('myquant')  # 获取指定section 的options即该节点的所有键
dicfile = config.get("myquant", "globaldict_path")  # 获取指定section下的options
db_path = config.get("myquant", "db_path")
global_sqlite_cnn = create_engine('sqlite:///' + db_path)

st.sidebar.markdown("# Global panel 🎈")

start_date = st.sidebar.date_input(
    "Start date",
    datetime.date(2013, 7, 6))
end_date = st.sidebar.date_input(
    "End date",
    datetime.date.today())

mkt = st.sidebar.multiselect('select market', ('BRN0Y', 'sge_gold', 'sz399001', 'sh000001', 'DJI', 'IXIC'))
mkt_str = ','.join(mkt)
if len(mkt) > 0:
    growth_chart(mkt_str, start_date, end_date)

    with open("global_grow.html") as f:
        html = f.read()

    st.markdown("# Global Main Market Growth 🎈")

    st.components.v1.html(html, width=800, height=600, scrolling=True)
