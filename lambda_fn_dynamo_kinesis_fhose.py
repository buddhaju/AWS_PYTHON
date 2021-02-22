#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author Buddhadev Choudhury : 2021 / Feb #

import json
import logging
import time
import boto3
from botocore.exceptions import ClientError

# Assign these values before running the program
# If the specified IAM role does not exist, it will be created
    
firehose_name = 'buddhadynamotos3test'
firehose_client = boto3.client('firehose',region_name='us-west-2')

def lambda_handler(event, context):
    
    print(event)
    
    for record in event['Records']:
        print("Player is " , record['dynamodb']['Keys']['playerid']['N'])
        
        print ("-------------------------------------")
        
        if record['eventName'] == 'INSERT':
            insert_details(record)
        elif record['eventName'] == 'REMOVE':
            remove_details(record)
        elif record['eventName'] == 'MODIFY':
            modify_details(record)
        
    print ("-------------------------------------")
        
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


def insert_details(record):
    print ("Details of new player inserted are following :\n")
    
    newimage = record['dynamodb']['NewImage']
    
    print ("New player Added is %s , Game is %s, and score is %s" %(newimage['playerid']['N'], newimage['game']['S'], newimage['score']['N']))
    print ("Completed insert_details of new player :\n")

    write_kinesis_firehose(str(newimage))
    
    
    
def remove_details(record):
    print ("Details of new player removed are following :\n")
    
    oldimage = record['dynamodb']['OldImage']
    
    print ("player Removed is %s , Game is %s, and score is %s" %(oldimage['playerid']['N'], oldimage['game']['S'], oldimage['score']['N'])) 
    print ("Completed remove_details of the player :\n")
    write_kinesis_firehose(str(oldimage))
    
    
def modify_details(record):
    print ("Details of modification for player are following :\n")
    
    oldimage = record['dynamodb']['OldImage']
    newimage = record['dynamodb']['NewImage']
    
    print ("Old details \n")
    
    print ("-------------------")
    print ("Previous details are  : player is %s , Game is %s, and score is %s" %(oldimage['playerid']['N'], oldimage['game']['S'], oldimage['score']['N']))
    
    print ("-------------------")
    print ("New details \n")
    print ("-------------------")
    print ("New player updated is %s , Game is %s, and score is %s" %(newimage['playerid']['N'], newimage['game']['S'], newimage['score']['N']))
    print ("-------------------")
    print ("Completed insert_details of new player :\n")

    write_kinesis_firehose(str(oldimage))
    write_kinesis_firehose(str(newimage))
    


def write_kinesis_firehose(msgdata):
    """Exercise Kinesis Firehose methods"""

    # Set up logging
    logging.basicConfig(level=logging.DEBUG,format='%(levelname)s: %(asctime)s: %(message)s')

    # Put records into the Firehose stream
    
    logging.info('Putting  records into the Firehose one at a time')
    # Put the record into the Firehose stream
    try:
        firehose_client.put_record(DeliveryStreamName=firehose_name,Record={'Data': msgdata})
    except ClientError as e:
        logging.error(e)
        exit(1)

    logging.info('Test data sent to Firehose stream')
