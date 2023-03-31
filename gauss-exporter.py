#!/usr/local/sbin/python3
# -*-coding:UTF-8-*-
# @Time: 10/8/22 9:50 AM
#脚本执行，依赖gauss独有的库，需要安装对应的库libzeclient.so和pyzenith.so

import base64
import pyzenith
import json
import os
import shutil
import threading
import time
from apscheduler.schedulers.blocking import BlockingScheduler


def timer(n):
    while True:
        time.sleep(n)


def escape_specific_symbol(string, quote=True):
    string = string.replace("\\r", " ")
    string = string.replace("\\n", "")
    string = string.replace("\\t", " ")
    string = string.replace("\"", "")
    string = string.replace("\\", "")
    return string


def generate_prom(prom_type, prom_help, prom_param, alert_name, node_name):
    log_dir = "/data01/node-exporter/logs/"
    alert_file = f'{alert_name}_{node_name}.prom'
    print(f'generate file {alert_file}')
    f = open(alert_file, 'w')
    try:
        f.write(f'# HELP {alert_name} {prom_help}\n')
        f.write(f'# TYPE {alert_name} {prom_type}\n')
        f.write(prom_param + '\n')
        f.flush()
    finally:
        f.close()

    src = os.path.join(os.getcwd(), alert_file)
    dst = os.path.join(log_dir, alert_file)
    shutil.copyfile(src, dst)
    os.remove(alert_file)


class SqlSchedule(object):
    @staticmethod
    def get_sql_result(line):
        passwd = bytes.decode(base64.b64decode(base64.b64decode(line['password'])), encoding="utf-8")
        conn = pyzenith.connect(line['host'], line['username'], passwd, '15400')
        cursor = conn.cursor()
        try:
            cursor.execute(line['sql'])
            result_check = cursor.fetchall()
            # print(result_check)
            desc = ",".join([item[0] for item in cursor.description])
        except Exception:
            # print("SQL:%s exec error" % line['sql'])
            print(f"SQL:{line['sql']} exec error")
            prom_param = f"{line['alert_name']}{{description=\"SQL 执行异常！\", " \
                         f"sql=\"{line['sql']}\", " \
                         f"exported_nodename=\"{line['nodename']}\", " \
                         f"sql_result=\"None\", " \
                         f"alert_db=\"{line['host']}\"}} 1"
            generate_prom(line['type'], line['help'], prom_param, line['alert_name'], line['nodename'])
            return
        finally:
            cursor.close()
            conn.close()

        sql_result = None
        prom_value = 0
        alert_value = 0
        # no result, ok
        try:
            line['alert_value']
        except KeyError:
            pass
        else:
            alert_value = int(line['alert_value'])

        if len(result_check) == 0:
            pass
        # 查询结果是number类型
        elif line['result_type'] == 'num':
            if int(result_check[0][0]) >= alert_value:
                prom_value = 1
            else:
                prom_value = 0
            sql_result = desc + "\n" + "=" * 50 + "\n"
            sql_result = (sql_result + str(result_check[0][0])).replace("\n", "<br/>")
        # 查询结果是string类型
        elif line['result_type'] == 'str':
            if len(result_check) > alert_value:
                prom_value = 1
            else:
                prom_value = 0
            sql_result = desc + "\n" + "=" * 50 + "\n"
            fmt_result = ""
            for result_check_sub in result_check:
                result_check_sub = escape_specific_symbol(str(result_check_sub))
                fmt_result += str(result_check_sub) + "\n"
            sql_result = (sql_result + fmt_result[0:5000]).replace("\n", "<br/>")

        #sql查询结果为number统计时，添加额外的alert_name_val，显示具体的查询结果
        if line['result_type'] == 'num':
            alert_name_val = f"{line['alert_name']}_val"
            prom_param = f"{alert_name_val}{{alert_db=\"{line['host']}\"}} {str(result_check[0][0])}"
            generate_prom(line['type'], line['help'], prom_param, alert_name_val, line['nodename'])
        else:
            prom_param = f"{line['alert_name']}{{description=\"{escape_specific_symbol(line['alert_msg'])}\", " \
                         f"exported_nodename=\"{line['nodename']}\", " \
                         f"sql=\"{line['sql']}\", " \
                         f"sql_result=\"{sql_result}\", " \
                         f"alert_db=\"{line['host']}\"}} {prom_value}"
            generate_prom(line['type'], line['help'], prom_param, line['alert_name'], line['nodename'])

    def start_jobs(self, line):
        scheduler = BlockingScheduler()
        scheduler.add_job(func=self.get_sql_result, args=[line], trigger='interval', seconds=int(line['interval']),
                          misfire_grace_time=3600, max_instances=50)
        scheduler.start()

    def update_thread(self, spark):
        thread = threading.Thread(target=self.start_jobs, args=[spark])
        thread.start()


if __name__ == '__main__':
    List = []
    for root, dirs, files in os.walk("prod-sql-dir"):
        for file in files:
            for obj in json.load(open(os.path.join(root, file), "r")):
                List.append(obj)
    for obj in List:
        print(f"line: {obj}")
        app = SqlSchedule()
        app.update_thread(obj)
    timer(1)