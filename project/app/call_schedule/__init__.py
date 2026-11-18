"""独立业务呼叫排期；入口与状态归属见本目录 README.md。

阅读顺序：worker → service 的 run_once/prepare/finish → callbacks。
queue 只管理可重建索引，control 只管理暂停；状态超时监控使用原有独立队列。
"""
