import requests
import re
import pymysql
import string

#Adam_20180729
#从yahoo finance上爬取2013年1月1日至2018年7月27日全部可以爬取的美股数据，并存入数据库

#首先设置一些格式参数
database_name = "USA_stocks"
Cookie = "B=cs31qqldfi9tn&b=3&s=nt; GUC=AQEBAQFbXn1cKkIhAwTX&s=AQAAAC8MBhS2&"\
    "g=W10xGw; PRF=t%3DCSCO%252B%255EGSPC%252BT%26fin-trd-cue%3D1; GUCS=ATQz5ue-"

request1 = "https://query1.finance.yahoo.com/v7/finance/download/"
request2 = "?period1=1356969600&period2=18789988400&interval=1d&events=history&crumb=Do.1dwB.rFe"

all_stocks = []

file = open("全部美股.txt")
#要保证读取的文本文件在当前目录下

for line in file:
    line.strip('\n')
    all_stocks.append(line)

file.close()
#加上三大指数，它们可能要从文件中读取
all_stocks.append("标准普尔(.INX)")
all_stocks.append("道琼斯(.DJI)")
all_stocks.append("纳斯达克(.IXIC)")


chinese_companies = []
file = open("chinese_companies.txt")
for line in file:
    line.strip('\n')
    chinese_companies.append(line)
file.close()

finance_companies = []
file = open("finance_companies.txt")
for line in file:
    line.strip('\n')
    finance_companies.append(line)
file.close()

food_medical_companies = []
file = open("food&medical_companies.txt")
for line in file:
    line.strip('\n')
    food_medical_companies.append(line)
file.close()

media_companies = []
file = open("media_companies.txt")
for line in file:
    line.strip('\n')
    media_companies.append(line)
file.close()

motor_energy_companies = []
file = open("motor&energy_companies.txt")
for line in file:
    line.strip('\n')
    motor_energy_companies.append(line)
file.close()

retail_manu_companies = []
file = open("retail&manu_companies.txt")
for line in file:
    line.strip('\n')
    retail_manu_companies.append(line)
file.close()

technology_companies = []
file = open("technology_companies.txt")
for line in file:
    line.strip('\n')
    technology_companies.append(line)
file.close()

#连接数据库
database = pymysql.connect("127.0.0.1", "root", "950321", database_name)
cursor = database.cursor()
#对all_stocks中的每个股票代码，试图请求其股票历史数据
#如果根据状态码判断请求成功，则建表并存入相应数据
count = 0
for n in range(len(all_stocks)):
    #提取出中文名和股票代码
    temp_result = re.findall(r'[(](.*?)[)]', all_stocks[n])
    stock_code = temp_result[0]
    #如果股票代码中含有.在建表时表名的时候转化为0
    stock_code_edited = stock_code.replace('.', '0')
    stock_name = all_stocks[n].split('(')[0]
    request_url = request1 + stock_code + request2;
    heads = {}
    heads['Cookie'] = Cookie
    temp_result = requests.get(request_url, headers=heads)
    if(temp_result.status_code != 200):
        print("股票{0}数据请求失败".format(stock_code))
        continue
    #确定股票所属类别
    category = "company"
    if(all_stocks[n] in chinese_companies):
        category = "chinese_company"
    elif(all_stocks[n] in finance_companies):
        category = "finance_comapny"
    elif(all_stocks[n] in food_medical_companies):
        category = "food&medical_comapny"
    elif(all_stocks[n] in media_companies):
        category = "media_comapny"
    elif(all_stocks[n] in motor_energy_companies):
        category = "motor&energy_company"
    elif(all_stocks[n] in retail_manu_companies):
        category = "retail&manu_company"
    elif(all_stocks[n] in technology_companies):
        category = "technology_company"

    results = temp_result.text.split('\n')
    del results[0];
    del results[len(results) - 1]
    for m in range(len(results)):
        results[m] = results[m].split(',')

    #建表，目前进行到GLU_A连接无响应
    try:
        #需要解决数据库名称以及新增字段的问题
        cursor.execute("create table if not exists {0}(\
        stock_code varchar(64) not null,\
        stock_name varchar(64) not null,\
        stock_category varchar(64) not null,\
        index_in varchar(64) default null comment '保留字段，标识所属股票指数',\
        date_t date primary key not null,\
        open double default null,\
        close double default null comment '经调整收市价',\
        low double default null,\
        high double default null,\
        s_ave double default null comment '考虑交易量的滑动平均',\
        volume bigint(20) default null\
        )engine=myisam default charset = utf8;".format(stock_code_edited))
        database.commit()
        s_amount = 0
        s_ave = 0
        for m in range(len(results)):
            try:
                #计算累计股票交易量
                s_ave += s_ave * s_amount + int(results[m][6]) * (float(results[m][1]) + float(results[m][5]))/2
                s_amount += int(results[m][6])
                s_ave /= s_amount
                #计算滑动平均股票价格
                cursor.execute(
                    "insert into {0} values('{1}', '{2}', '{3}', default, '{4}', {5}, {6}, {7}, {8}, {9}, {10})".format(
                        stock_code_edited, stock_code, stock_name, category, results[m][0], float(results[m][1]),
                        float(results[m][5]), float(results[m][3]),
                        float(results[m][2]), s_ave, int(results[m][6])
                    ))
                database.commit()
            except BaseException as e:
                print(e)
                print("该异常被忽略")
            #print("插入第{0}条数据".format(m))
        count += 1
        print("成功导入了股票{0}的数据，共成功导入{1}支股票".format(stock_code, count))
    except BaseException as e:
        print("在建立股票{0}时发生数据库错误".format(stock_code))
        print(e)
        continue

print("全部数据导入完毕！")
