import pymongo as pymongo
from pymongo import MongoClient
import csv
from collections import defaultdict 
import pandas as pd
import numpy as np

maxSevSelDelay = 1000
client = pymongo.MongoClient("mongodb+srv://akrishna:J3WfhZuNfkcNx9uC@coviddata.drkjr.mongodb.net/coviddata?retryWrites=true&w=majority", serverSelectionTimeoutMS=maxSevSelDelay)
db = client.test
testData = db.testData
metaData = db.metaData
cityData = db.cityData

last7days_csv_link = "https://raw.githubusercontent.com/nychealth/coronavirus-data/master/latest/last7days-by-modzcta.csv"
pp_csv_link = "https://raw.githubusercontent.com/nychealth/coronavirus-data/master/latest/pp-by-modzcta.csv"
total_csv_link = "https://raw.githubusercontent.com/nychealth/coronavirus-data/master/totals/data-by-modzcta.csv"
city_data_link = "https://raw.githubusercontent.com/nychealth/coronavirus-data/master/latest/now-data-by-day.csv"
city_test_link = "https://raw.githubusercontent.com/nychealth/coronavirus-data/master/latest/now-tests.csv"
city_summary_link = "https://raw.githubusercontent.com/nychealth/coronavirus-data/master/latest/now-summary.csv"


def insert1Day():
    latest_day = pd.read_csv(pp_csv_link).iloc[[-1]].melt(id_vars=['End date'], var_name="zipCode", value_name="percentpositivity_1day")[['zipCode', 'percentpositivity_1day']]
    latest_day = latest_day.drop(range(0, 6))
    latest_day['zipCode'] = pd.to_numeric(latest_day['zipCode'])
    data2 = latest_day.to_dict(orient='records')
    testData.insert_many(data2)

def update7DaysAndTotal():
    total = pd.read_csv(total_csv_link).iloc[:, np.r_[0, 3:10]].rename(columns={'MODIFIED_ZCTA': 'zipCode'})
    total['zipCode'] = pd.to_numeric(total['zipCode'])
    total.set_index("zipCode", drop=True, inplace=True)
    totalData = total.to_dict(orient='index') 

    last7days = pd.read_csv(last7days_csv_link).iloc[: , 0:5].rename(columns={'modzcta': 'zipCode', 'modzcta_name': 'name'})
    last7days['zipCode'] = pd.to_numeric(last7days['zipCode'])
    last7days.set_index("zipCode", drop=True, inplace=True)
    last7data = last7days.to_dict(orient='index') 

    for doc in testData.find():
        people_positive_7day = last7data[doc["zipCode"]]["people_positive"]
        total_population = totalData[doc["zipCode"]]["POP_DENOMINATOR"]
        infection_rate = people_positive_7day * 100000 / (total_population * 7)
        testData.update_one({"zipCode": doc["zipCode"]}, {"$set": 
            {"percentpositivity_7day":  last7data[doc["zipCode"]]["percentpositivity_7day"],
            "people_tested_7day":       last7data[doc["zipCode"]]["people_tested"],
            "people_positive_7day":     people_positive_7day,
            "total_cases":           totalData[doc["zipCode"]]["COVID_CASE_COUNT"],
            "total_deaths":             totalData[doc["zipCode"]]["COVID_DEATH_COUNT"],
            "total_tests":              totalData[doc["zipCode"]]["TOTAL_COVID_TESTS"],
            "infection_rate_7day":           infection_rate
        }})

def updateCity():
    city_data = pd.read_csv(city_data_link).iloc[[-1], :]
    city_summary = pd.read_csv(city_summary_link)
    last7days = pd.read_csv(last7days_csv_link)
    total = pd.read_csv(total_csv_link).iloc[:, [-1]]

    last7days_tests = int(sum([zipCode['people_tested'] for index, zipCode in last7days.iterrows()]))
    last7days_positive = int(sum([zipCode['people_positive'] for index, zipCode in last7days.iterrows()]))
    total_tests = int(sum([zipCode['TOTAL_COVID_TESTS'] for index, zipCode in total.iterrows()]))
    infection_rate = last7days_positive * 100000 / (8419000 * 7)

    # cityData.insert_one(
    #     {
    #     # "date":                     city_data["date_of_interest"],
    #     # "cases_1day":               city_data["CASE_COUNT"],
    #     # "deaths_1day":              city_data["DEATH_COUNT"],
    #     # "tests_1day":               city_test["TOTAL_TESTS"],
    #     # "positive_tests_1day":     city_test["POSITIVE_TESTS"],
    #     # "cases_7day_avg":           city_data["CASE_COUNT_7DAY_AVG"],
    #     # "deaths_7day_avg":          city_data["DEATH_COUNT_7DAY_AVG"], 	
    #     "percentpositivity_7day":   city_summary.iloc[0]["LAST_7_DAY"],
    #     "cases_7day":               city_summary.iloc[1]["LAST_7_DAY"],
    #     "deaths_7day":              city_summary.iloc[5]["LAST_7_DAY"],
    #     "tests_7day":               last7days_tests,
    #     "positive_7day":            last7days_positive,
    #     "infection_rate_7day":      infection_rate,
    #     "total_cases":              city_summary.iloc[1]["TOTAL"],
    #     "total_deaths":             city_summary.iloc[5]["TOTAL"],
    #     "total_tests":              total_tests,
    # })
    cityData.update_one({}, {"$set": 
        {"percentpositivity_7day":   city_summary.iloc[0]["LAST_7_DAY"],
        "cases_7day":               city_summary.iloc[1]["LAST_7_DAY"],
        "deaths_7day":              city_summary.iloc[5]["LAST_7_DAY"],
        "tests_7day":               last7days_tests,
        "positive_7day":            last7days_positive,
        "infection_rate_7day":      infection_rate,
        "total_cases":              city_summary.iloc[1]["TOTAL"],
        "total_deaths":             city_summary.iloc[5]["TOTAL"],
        "total_tests":              total_tests,
    }})

def updateMetadata():
    date = str(pd.read_csv(city_data_link).iloc[-1]["date_of_interest"])
    dateRange = pd.read_csv(last7days_csv_link).iloc[[0], [-1]]["daterange"].values[0]
    metaData.update_one({}, {"$set": {"dateRange": dateRange, "date": date}}, upsert=True)

def removeExtraFields():
    testData.update_many({},{"$unset": {"total_positive":1}},upsert=True)

# insert1Day()
update7DaysAndTotal()
updateCity()
updateMetadata()
# removeExtraFields()

