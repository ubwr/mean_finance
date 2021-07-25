import pandas as pd
import pandas_datareader.data as web
from datetime import datetime
import math
import csv

# green = 0, red = 1
def run(color):
    
    title = ""
    if color == 0:
        title = "green"
    else:
        title = "red"
        
    column = ["Symbol"]

    exchange = "nasdaq"
    company_cap = "micro"

    stock_tickers_df = pd.read_csv(exchange + "_screener_" + company_cap + "_cap.csv", usecols=column)
    dictionary = {"Symbol":[],
                        "Date":[],
                        "Percent_Change":[],
                        "Z_Score":[],
                        "One_Day_Percent_Change":[],
                        "One_Week_Percent_Change":[],
                        "One_Month_Percent_Change":[]}
    final_dict = {"Symbol":[],
                        "Date":[],
                        "Percent_Change":[],
                        "Z_Score":[],
                        "One_Day_Percent_Change":[],
                        "One_Week_Percent_Change":[],
                        "One_Month_Percent_Change":[]}
    averages = {"Symbol":[], "Average_Initial_Change":[], "Average_One_Day":[], "Average_Week":[], "Average_Month":[], "Rows":[]}
    column_names = ["Symbol", "Date", "Percent_Change", "Z_Score", "One_Day_Percent_Change", "One_Week_Percent_Change", "One_Month_Percent_Change"]
    average_columns = ["Symbol", "Average_Initial_Change", "Average_One_Day", "Average_Week", "Average_Month", "Rows"]
    average_csv = pd.DataFrame(averages)
    csv = pd.DataFrame(final_dict)
    tick_count = 1

    big_move_list = []
    honorable_mentions = []
    for j, r in stock_tickers_df.iterrows():
##        if j == 10:
##            break
        while True:
            try:
                ticker = stock_tickers_df.iloc[j]['Symbol']
                try:
                    today_start = datetime(2021, 7, 22)
                    today_end = datetime(2021, 7, 23)
                    today_df = web.DataReader(ticker, 'yahoo', today_start, today_end)
                    today_df = today_df.reset_index()

                    start = datetime(1970, 1, 1)
                    end = datetime(2021, 7, 21)
                    df = web.DataReader(ticker, 'yahoo', start, end)
                    df = df.reset_index()

                    df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d')

                    if pd.DatetimeIndex(df['Date']).year[0] < 2020:
                        
                        count = 0
                        percent = 0
                        average = 0
                        percent_array = []
                        data_rows = []
                        one_day_change = 0
                        one_week_change = 0
                        one_month_change = 0
                        print(ticker, " : ", tick_count)
                        tick_count += 1
                        
                        for index, row in df.iterrows():
                            if index < len(df)-20:
                                try:
                                    if (index > 0):
                                        string = 'Close'
                                        num = 1
                                    else:
                                        string = 'Open'
                                        num = 0
                                    if df.iloc[index-num][string] > 0:
                                        percent_change = round((((df.iloc[index]['Close'] - df.iloc[index-num][string]) / df.iloc[index-num][string]) * 100), 2)
                                        if color == 0:
                                            if (df.iloc[index]['Close'] > df.iloc[index-num][string]):
                                                count += 1
                                                percent += percent_change
                                                percent_array.append(percent_change)
                                                data_rows.append(("DATE: ", df.iloc[index]['Date'], "PREVIOUS PRICE: ", round(df.iloc[index-num][string], 2),
                                                      "CLOSE: ", round(df.iloc[index]['Close'], 2), "%CHANGE: ",
                                                      percent_change, "%"))
                                                dictionary["Symbol"].append(ticker)
                                                dictionary["Date"].append(str(df.iloc[index]['Date'].date()))
                                                dictionary["Percent_Change"].append(percent_change)
                                                one_day_change = round((((df.iloc[index+1]['Close'] - df.iloc[index][string]) / df.iloc[index][string]) * 100), 2)
                                                one_week_change = round((((df.iloc[index+5]['Close'] - df.iloc[index][string]) / df.iloc[index][string]) * 100), 2)
                                                one_month_change = round((((df.iloc[index+20]['Close'] - df.iloc[index][string]) / df.iloc[index][string]) * 100), 2)
                                                dictionary["One_Day_Percent_Change"].append(one_day_change)
                                                dictionary["One_Week_Percent_Change"].append(one_week_change)
                                                dictionary["One_Month_Percent_Change"].append(one_month_change)
                                        if color == 1:
                                            if (df.iloc[index]['Close'] < df.iloc[index-num][string]):
                                                count += 1
                                                percent += percent_change
                                                percent_array.append(percent_change)
                                                data_rows.append(("DATE: ", df.iloc[index]['Date'], "PREVIOUS PRICE: ", round(df.iloc[index-num][string], 2),
                                                      "CLOSE: ", round(df.iloc[index]['Close'], 2), "%CHANGE: ",
                                                      percent_change, "%"))
                                                dictionary["Symbol"].append(ticker)
                                                dictionary["Date"].append(str(df.iloc[index]['Date'].date()))
                                                dictionary["Percent_Change"].append(percent_change)
                                                one_day_change = round((((df.iloc[index+1]['Close'] - df.iloc[index][string]) / df.iloc[index][string]) * 100), 2)
                                                one_week_change = round((((df.iloc[index+5]['Close'] - df.iloc[index][string]) / df.iloc[index][string]) * 100), 2)
                                                one_month_change = round((((df.iloc[index+20]['Close'] - df.iloc[index][string]) / df.iloc[index][string]) * 100), 2)
                                                dictionary["One_Day_Percent_Change"].append(one_day_change)
                                                dictionary["One_Week_Percent_Change"].append(one_week_change)
                                                dictionary["One_Month_Percent_Change"].append(one_month_change)
                                except (ZeroDivisionError):
                                    pass
                        try:
                            average = percent / count
                            # list of data, average, number of samples
                            def standard_deviation_calculator(ls, avg, ct):
                                divisible_total = 0
                                for perc in ls:
                                    divisible_total += (perc - avg) ** 2
                                return (math.sqrt(divisible_total/(ct)))

                            # list of data, number of samples
                            def calc_sample_mean(ls, ct):
                                total = 0
                                for perc in ls:
                                    total += perc
                                return (total / ct)

                            z_dict = {}

                            # population mean, population standard deviation, list to update, list to get data from, rows of stock daily data
                            def z_calc(avg, std_dev, send_ls, receive_dict, data_rows):
                                c = 0
                                for i in send_ls:
                                    receive_dict[data_rows[c]] = ((i-avg)/std_dev)
                                    c += 1

                            standard_deviation = standard_deviation_calculator(percent_array, average, count)
                            z_score = z_calc(average, standard_deviation, percent_array, z_dict, data_rows)
                            new_dict = {}

                            for i in z_dict:
                                dictionary["Z_Score"].append(round(z_dict[i], 3))
                                if z_dict[i] >= 3 or z_dict[i] <= -3:
                                    # round z 
                                    new_dict[i] = round(z_dict[i], 3)

                            new_dict = dict(sorted(new_dict.items(), reverse=True, key=lambda item: item[1]))

                            initial = 0
                            one_day = 0
                            one_week = 0
                            one_month = 0
                            for index, z in enumerate(dictionary["Z_Score"]):
                                if z >= 3 or z <= -3:
                                    final_dict["Symbol"].append(dictionary["Symbol"][index])
                                    final_dict["Date"].append(dictionary["Date"][index])
                                    final_dict["Percent_Change"].append(dictionary["Percent_Change"][index])
                                    initial += dictionary["Percent_Change"][index]
                                    final_dict["Z_Score"].append(z)
                                    final_dict["One_Day_Percent_Change"].append(dictionary["One_Day_Percent_Change"][index])
                                    one_day += dictionary["One_Day_Percent_Change"][index]
                                    final_dict["One_Week_Percent_Change"].append(dictionary["One_Week_Percent_Change"][index])
                                    one_week += dictionary["One_Week_Percent_Change"][index]
                                    final_dict["One_Month_Percent_Change"].append(dictionary["One_Month_Percent_Change"][index])
                                    one_month += dictionary["One_Month_Percent_Change"][index]

                            averages["Symbol"] = []
                            averages["Symbol"].append(ticker)
                            averages["Average_Initial_Change"].append(round((initial/len(final_dict["Percent_Change"])), 2))
                            averages["Average_One_Day"].append(round((one_day/len(final_dict["One_Day_Percent_Change"])), 2))
                            averages["Average_Week"].append(round((one_week/len(final_dict["One_Week_Percent_Change"])), 2))
                            averages["Average_Month"].append(round((one_month/len(final_dict["One_Month_Percent_Change"])), 2))
                            averages["Rows"].append(len(final_dict["Percent_Change"]))
                            dictionary = {"Symbol":[],
                                "Date":[],
                                "Percent_Change":[],
                                "Z_Score":[],
                                "One_Day_Percent_Change":[],
                                "One_Week_Percent_Change":[],
                                "One_Month_Percent_Change":[]}

                            new_df = pd.DataFrame(final_dict)
                            if color == 0:
                                new_df = new_df.sort_values(by=["Z_Score"], ascending=False)
                            else:
                                new_df = new_df.sort_values(by=["Z_Score"], ascending=True)

                            new_avg_df = pd.DataFrame(averages)

                            averages = {"Symbol":[], "Average_Initial_Change":[], "Average_One_Day":[], "Average_Week":[], "Average_Month":[], "Rows":[]}

                            average_csv = average_csv.append(new_avg_df)
                            csv = csv.append(new_df)

                            final_dict = {"Symbol":[],
                                "Date":[],
                                "Percent_Change":[],
                                "Z_Score":[],
                                "One_Day_Percent_Change":[],
                                "One_Week_Percent_Change":[],
                                "One_Month_Percent_Change":[]}
                            today_percent_change = round((((today_df.iloc[1]['Close'] - today_df.iloc[0]['Close']) / today_df.iloc[0]['Close']) * 100), 2)
                            solo_z = (today_percent_change - average) / standard_deviation
##                            print(str(today_percent_change)+'%')
##                            print(solo_z)
                            if color == 0:
                                if today_percent_change > 0:
                                    if solo_z >= 3 or solo_z <= -3:
                                         big_move_list.append(ticker)
                                    if solo_z < 3 and solo_z >= 2.5:
                                        honorable_mentions.append(ticker)
                            else:
                                if today_percent_change < 0:
                                    if solo_z >= 3 or solo_z <= -3:
                                         big_move_list.append(ticker)
                                    if solo_z < 3 and solo_z >= 2.5:
                                        honorable_mentions.append(ticker)

                            print("HM: ", honorable_mentions)
                            print("BM: ", big_move_list)
                        except Exception as e:
                            print(e)
                except (KeyError):
                    pass
            except Exception as e:
                print(e)
            break
    csv.to_csv(title + '_' + exchange + '_screener_' + company_cap + '_cap_data.csv', index=False, header=column_names)
    average_csv.to_csv(title + '_' + exchange + '_' + company_cap + '_averages.csv', index=False, header=average_columns)

run(0)
run(1)
