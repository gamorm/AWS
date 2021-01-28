# Smartsheet Ingestion into DynamoDB
# v1.0
# By Ryan Gamo
import os
import boto3
from csv import reader
from dotenv import load_dotenv

load_dotenv() # loads the .env file which contains secrets
DEV_API = os.environ.get('SMARTSHEET_DEV_API') # Can be passed to smartsheet(API_TOKEN)
PROD_API = os.environ.get('SMARTSHEET_PROD_API') # Can be passed to smartsheet(API_TOKEN)
sheetID = os.environ.get('TDR_SCHED_SHEET') # smartsheet(thisSheet)
altFileName = str(sheetID) + '.csv' # smartsheet(thisCSV) and uploadDynamo(thisCSV)
download_folder_path = os.environ['PWD'] # smartsheet(thisPath)
table = '' # uploadDynamo(awsTable)
region = 'us-west-2' # uploadDynamo(awsRegion)


def smartsheet(thisSheet, thisCSV, thisPath, API_TOKEN):
    import smartsheet
    from datetime import datetime
    import pandas as pd
    smartsheet_client = smartsheet.Smartsheet(API_TOKEN) # Authenticate
    smartsheet_client.Sheets.get_sheet_as_csv(
        thisSheet,       # sheet ID
        thisPath, 
        alternate_file_name = thisCSV
    )
    # create dataframe
    df = pd.read_csv(altFileName)
    # modify dates in dataframe
    df['Date'] = pd.to_datetime(df.Date)
    # write changes to csv
    df.to_csv(thisCSV, index=False,encoding='utf-8')

def uploadDynamo(thisCSV, awsTable, awsRegion):
    # dynamodb and table initialization
    headerRow = 1 # 1 = True, 0 = False
    writeRate = 100
    endpointUrl = "https://dynamodb.us-west-2.amazonaws.com"
    dynamodb = boto3.resource('dynamodb', region_name=awsRegion, endpoint_url=endpointUrl)
    table = dynamodb.Table(awsTable)

    # open the csv filename specified above
    with open(thisCSV,encoding='utf-8-sig',newline='\n') as csv_file:
        tokens = reader(csv_file, delimiter=',') # Read the csv file
        # Get the header from the first line, and also advance the row index
        # Else, you will insert one record of the header names - you don't want that
        header = next(tokens)

        headerLength = len(header)  # Get the length of the header, used in the for loops 

        # Start looping through lines
        for token in tokens:
            column = 0  # Zero out the column 
            row = {}  # Dictionary/JSON for each row we are going to insert
            for x in range(headerLength): # Using that headerLength
                row[header[column]] = token[column] # Assemble the item, consisting of 'header/key':'value' in JSON format
                column += 1

            print(row) # For console feedback
            table.put_item(Item=row) # Inserts the assembled row
            time.sleep(1 / writeRate)  # to accomodate max write provisioned capacity for table

    
smartsheet(sheetID,altFileName,download_folder_path,PROD_API)
uploadDynamo(altFileName, table, region)
os.remove(altFileName) # clean-up
