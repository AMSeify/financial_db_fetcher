import streamlit as st
import logging
from config.logger_setup import daily_task_queue, weekly_task_queue, daily_fetch_queue

st.title("Async Tasks Monitoring")

st.subheader("Daily Task Logs")
daily_logs = []
while not daily_task_queue.empty():
    record = daily_task_queue.get_nowait()
    daily_logs.append(f"{record.levelname}: {record.msg}")
for log in daily_logs:
    st.write(log)

st.subheader("Weekly Task Logs")
weekly_logs = []
while not weekly_task_queue.empty():
    record = weekly_task_queue.get_nowait()
    weekly_logs.append(f"{record.levelname}: {record.msg}")
for log in weekly_logs:
    st.write(log)

st.subheader("Daily Fetch Data Logs")
daily_fetch_logs = []
while not daily_fetch_queue.empty():
    record = daily_fetch_queue.get_nowait()
    daily_fetch_logs.append(f"{record.levelname}: {record.msg}")
for log in daily_fetch_logs:
    st.write(log)
