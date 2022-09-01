

import streamlit as st
import configparser

if __name__ == "__main__":

    config = configparser.ConfigParser()
    config.read("config.ini", encoding="utf-8")
    config.sections()  # 获取section节点
    config.options('myquant')  # 获取指定section 的options即该节点的所有键
    synlog_file = config.get("myquant", "synlog_path")

    f = open(synlog_file , 'rb')  # ‘r’的话会有两个\n\n
    t = f.read()
    f.close()
    st.markdown(t)