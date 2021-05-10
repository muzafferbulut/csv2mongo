# -*- coding: utf-8 -*-
"""
Created on Sat Apr 17 15:41:55 2021

@author: Muzaffer
"""

import pandas as pd
from pymongo import MongoClient
from os import listdir
import json

with open("info.json","r") as file:
    JsonFile = json.load(file)

client = MongoClient()
database = client[JsonFile["database"]]
collection = database[JsonFile["collection"]]

files = listdir(JsonFile["filePath"])

for file in range(len(files)):
    fileName = JsonFile["filePath"]+"\\"+files[file]
    csvFile = pd.read_csv(fileName)
    
    [x,y] = csvFile.shape
    columns = list(csvFile.columns)
    data = csvFile.values
    
    for row in range(x):
        
        dataRow = data[row]
        
        DataDict = dict(zip(columns, dataRow))
        
        collection.insert(DataDict)

print("Data has been inserted")
