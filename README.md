# ExchangeRates
外匯牌告匯率表爬蟲<br/>
本例因為剛好資料量不大,所以用pandas.read_html(url)方式來獲取資料,並且從中解析所需的欄位,
最後寫入Mongodb及保存為json檔
