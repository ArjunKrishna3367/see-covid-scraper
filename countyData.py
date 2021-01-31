# from urllib.request import Request, urlopen
# import json
# import pymongo

# maxSevSelDelay = 1000
# client = pymongo.MongoClient("mongodb+srv://akrishna:J3WfhZuNfkcNx9uC@coviddata.drkjr.mongodb.net/coviddata?retryWrites=true&w=majority", serverSelectionTimeoutMS=maxSevSelDelay)
# db = client.test

# req = Request('https://disease.sh/v3/covid-19/jhucsse/counties', headers={'User-Agent': 'Mozilla/5.0'})
# JHUdata = json.loads(urlopen(req).read())
# JHU_NYData = [county for county in JHUdata if county["province"] == "New York"]
# print(JHU_NYData)

# statePositiveTotoal = sum([county["stats"]["confirmed"] for county in JHU_NYData])
# stateDeathsTotal = sum([county["stats"]["deaths"] for county in JHU_NYData])

# req = Request('https://disease.sh/v3/covid-19/states?yesterday=true', headers={'User-Agent': 'Mozilla/5.0'})
# World_data = json.loads(urlopen(req).read())
# World_NYData = [stateData for stateData in World_data if stateData["state"] == "New York"]

