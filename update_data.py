#每天定时运行程序，对于每一只股票，获取其最后一条记录至今的新纪录并更新
import pymysql
import requests
import time

database_name = "usa_stocks"
Cookie = "B=cs31qqldfi9tn&b=3&s=nt; GUC=AQEBAQFbXn1cKkIhAwTX&s=AQAAAC8MBhS2&"\
    "g=W10xGw; PRF=t%3DCSCO%252B%255EGSPC%252BT%26fin-trd-cue%3D1; GUCS=ATQz5ue-"
request1 = "https://query1.finance.yahoo.com/v7/finance/download/"
request2 = "?period1="

#要写股票名称......
request3 = "&period2="
request4 = "&interval=1d&events=history&crumb=Do.1dwB.rFe"

#之后要统一添加三大指数
#时间的具体含义之后也要弄清楚

database = pymysql.connect("127.0.0.1", "root", "950321", database_name)
cursor = database.cursor()

N = cursor.execute("show tables")
tables = cursor.fetchall()
count = 1

for n in range(N):
    try:
        table_name = tables[n][0]
        temp_num = cursor.execute("select * from {0} order by date_t desc limit 1".format(table_name))
        if (temp_num == 0):
            continue
        record = cursor.fetchall()
        GMT_time = time.mktime(record[0][4].timetuple())
        GMT_time = int(GMT_time)
        s_ave = record[0][9]
        stock_code = record[0][0]
        stock_name = record[0][1]
        category = record[0][2]
        start_time = str(GMT_time)
        end_time = str(int(time.time()))
        request_url = request1 + stock_code + request2 + start_time + request3 + end_time + request4
        heads = {}
        heads['Cookie'] = Cookie
        temp_result = requests.get(request_url, headers=heads)
        if (temp_result.status_code != 200):
            print("股票{0}更新数据数据请求失败".format(table_name))
            continue

        # 统计历史交易总手数
        s_amount = 0
        cursor.execute("select volume from {0}".format(table_name))
        volumes = cursor.fetchall()
        for m in range(len(volumes)):
            s_amount += volumes[m][0]

        # 插入更新数据
        results = temp_result.text.split('\n')
        del results[0];
        del results[len(results) - 1]
        for m in range(len(results)):
            results[m] = results[m].split(',')
            try:
                s_ave += s_ave * s_amount + int(results[m][6]) * (float(results[m][1]) + float(results[m][5])) / 2
                s_amount += int(results[m][6])
                s_ave /= s_amount
                cursor.execute(
                    "insert into {0} values('{1}', '{2}', '{3}', default, '{4}', {5}, {6}, {7}, {8}, {9}, {10})".format(
                        table_name, stock_code, stock_name, category, results[m][0], float(results[m][1]),
                        float(results[m][5]), float(results[m][3]),
                        float(results[m][2]), s_ave, int(results[m][6])
                    ))
                database.commit()
            except BaseException as e:
                print("更新股票{0}时出现错误".format(table_name))
                print(e)
                continue
        print("股票{0}数据更新成功".format(table_name))
    except BaseException as e:
        print(e)
        continue


print("股票数据更新完毕")
