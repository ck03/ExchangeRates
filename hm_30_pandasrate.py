import pandas as pd
import datetime as dt
import time
from datetime import datetime
from copy import deepcopy
from pymongo import MongoClient
import json


class ExchangeRates():

    def chagedate(self, sdate):
        """
        日期轉換成int型態
        :param sdate:
        :return:
        """
        sdate = int(sdate.replace("-", ""))
        return sdate

    def dateaddone(self, sdate):
        """
         日期加 1 天,並轉換
        :param sdate:
        :return:
        """
        delta = dt.timedelta(days=1)
        next_date = datetime.strptime(sdate, '%Y-%m-%d')
        n_days = next_date + delta
        n_days = n_days.strftime('%Y-%m-%d')
        n_days_int = self.chagedate(n_days)
        return n_days, n_days_int

    def dbexits(self, collection):
        if collection.count() > 0:
            collection.drop()
            print("外匯牌告匯率表exrates已刪除")
        else:
            print("尚無外匯牌告匯率表exrates")

    def save_dict(self, result_list, next_temp_date, end_date_int):
        self.result_dict["Result"] = result_list
        with open("外匯牌告匯率表({}-{}).json".format(next_temp_date, end_date_int), "w", encoding="utf-8") as f:
            f.write(json.dumps(self.result_dict, ensure_ascii=False, indent=2))

    def dblast(self, collection):
        if collection.count() > 0:
            strdate = collection.find().sort([("date", -1), ("_id", -1)]).limit(1)
            for i in strdate:
                sd = i["date"]
                sd = sd[:4] + "-" + sd[4:6] + "-" + sd[-2:]
                return sd, int(i["_id"])
        else:
                return "", 0

    def __init__(self):
        self.client = MongoClient(host="127.0.0.1", port=27017)
        self.collection = self.client["exrates"]["info"]
        # 首次爬取時要用到,因為尚屬測試階段,若測試OK後,db已有資料,則要mark起來
        # self.dbexits(self.collection)
        self.next_date = "2019-01-01"
        self.next_date_int = self.chagedate(self.next_date)
        self.end_date = "2019-07-31"
        self.end_date_int = self.chagedate(self.end_date)
        # today = dt.date.today()
        # print(today)
        # t = datetime.strptime("2018-01-01", '%Y-%m-%d')
        # print(type(t))

        self.result_list = []
        self.result_dict = {}

    def run(self):
        # mongodb已有資料,找出最近(即日期最大)一筆的日期,然後加1的日期就是要開始insert的日期
        lastdate, pid = self.dblast(self.collection)
        if lastdate != "":
            self.next_date, self.next_date_int = self.dateaddone(lastdate)
            pid += 1
        else:
            pid += 1
        # print(pid)
        # return
        next_temp_date = ""
        if self.next_date_int <= self.end_date_int:
            while True:
                dfs = pd.read_html("https://rate.bot.com.tw/xrt/all/{}".format(self.next_date))
                print(self.next_date)
                print(type(dfs), len(dfs), dfs[0].iloc[0, 0])
                if dfs[0].iloc[0, 0] != "很抱歉，本次查詢找不到任何一筆資料！":
                    currency = dfs[0].iloc[:, 0:5]
                    # print(len(currency))
                    currency.columns = [u"幣別", u"現金匯率-本行買入", u"現金匯率-本行賣出", u"即期匯率-本行買入", u"即期匯率-本行賣出"]
                    row_dict = {}
                    # "現金匯率-本行買入"=>cerbb, "現金匯率-本行賣出"=>cerbs, "即期匯率-本行買入"=>serbb, "即期匯率-本行賣出"=>sersb
                    for i in range(len(currency[u"幣別"])):
                        row_dict_temp = {}
                        row_dict_temp2 = {}
                        t = currency.iloc[i, 0]
                        t = t.split(" ")
                        t = "".join(t[:2])
                        currency.iloc[i, 0] = t
                        row_dict["currency"] = currency.iloc[i, 0]
                        row_dict["cerbb"] = currency.iloc[i, 1]
                        row_dict["cerbs"] = currency.iloc[i, 2]
                        row_dict["serbb"] = currency.iloc[i, 3]
                        row_dict["serbs"] = currency.iloc[i, 4]
                        row_dict["date"] = str(self.next_date_int)
                        row_dict["_id"] = pid
                        row_dict_temp = deepcopy(row_dict)
                        row_dict_temp2 = deepcopy(row_dict)
                        # 保存到mongodb,抓一筆寫一筆
                        self.collection.insert(row_dict_temp)
                        self.result_list.append(row_dict_temp2)
                        pid += 1
                        # print(row_dict)
                    # print(currency)
                    print("爬取{}資料結束.....".format(self.next_date))
                if next_temp_date == "":
                    next_temp_date = self.next_date_int
                self.next_date, self.next_date_int = self.dateaddone(self.next_date)
                if self.next_date_int > self.end_date_int:
                    # 保存到json檔
                    self.save_dict(self.result_list, next_temp_date, self.end_date_int)
                    print("爬取外匯牌告匯率表結束.....")
                    break


if __name__ == "__main__":
    print("爬取外匯牌告匯率表開始.....")
    exchangerates = ExchangeRates()
    exchangerates.run()
