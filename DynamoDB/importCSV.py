# Simplified Python Script to insert csv records in dynamodb table.
# By Ryan Gamo 2021
from __future__ import print_function  # Python 2/3 compatibility
from __future__ import division  # Python 2/3 compatiblity for integer division
import argparse
import boto3
from csv import reader
import time

csvFile = 'YOURFILENAME.csv'
headerRow = 1 # 1 = True, 0 = False
delimiter = ','
writeRate = 100
region = 'us-west-2'
table = 'YOURTABLENAME'

# dynamodb and table initialization
endpointUrl = "https://dynamodb.us-west-2.amazonaws.com"
dynamodb = boto3.resource('dynamodb', region_name=region, endpoint_url=endpointUrl)
table = dynamodb.Table(table)

# open the csv filename specified above
with open(csvFile,encoding='utf-8-sig',newline='\n') as csv_file:
    # Read the csv file, specify the delimiter -commas in this case
    tokens = reader(csv_file, delimiter=delimiter)
    
    # Get the header from the first line, and also advance the row index
    # Else, you will insert one record of the header names - you don't want that
    header = next(tokens)
    
    # Get the length of the header, used in the for loops 
    headerLength = len(header)
    
    # Start looping through lines
    for token in tokens:
        # Zero out the column
        column = 0
        # Dictionary/JSON for each row we are going to insert
        row = {}
        for x in range(headerLength): # Using that headerLength
            # Assemble the item, consisting of 'header/key':'value' in JSON format
            row[header[column]] = token[column]
            column += 1
        
        print(row) # For console feedback
        # Insert the assembled row
        table.put_item(Item=row)
        
        # time.sleep(1 / writeRate)  # to accomodate max write provisioned capacity for table
    
