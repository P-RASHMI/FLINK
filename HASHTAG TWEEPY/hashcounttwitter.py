'''
@Author: Rashmi
@Date: 2021-11-29 21:20
@Last Modified by: Rashmi
@Last Modified time: 2021-11-30  01:42
@Title :Write a Python program for performing hashtag wordcount by taking 
trending hashtags into a list and print into file 
and taking that file as input and generating output 
file with the words,count using pyflink
'''
import os
import random
import tweepy
import requests
import csv
import time
from pyflink.table import (EnvironmentSettings, TableEnvironment, TableDescriptor, Schema,
                           DataTypes)
from pyflink.table.expressions import lit

settings = EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build()
t_env = TableEnvironment.create(settings)

def hashtag_count():
        
    try:
        input_file = '/home/lenovo/Desktop/Python_work/hadoop/FLINK/HASHTAG TWEEPY/inputhashtags.txt'
        output_file = '/home/lenovo/Desktop/Python_work/hadoop/FLINK/HASHTAG TWEEPY/outputhashcount.csv'  
        # remove the output file, if there is one there already
        if os.path.isfile(output_file):
                os.remove(output_file)
#get the hashtags from trends24 website and count that trends,store into output        
        hashtags = ["#jackdorsey","Parag Agrawal","Twitter CEO","DOGEorTesla"
                    "#Binance","IIT Bombay","Microsoft","BallonDor","CEOs",
                    "Sushant Humesha Amar Hai","#jackdorsey","Parag Agrawal","Twitter CEO","DOGEorTesla"
                    "#Binance","IIT Bombay","Microsoft","BallonDor","CEOs",
                   ]
    
        with open(input_file, 'w') as f:
                for tweet in hashtags:
                        f.write('%s\n' % ((tweet)))

        # write all the data to one file
        t_env.get_config().get_configuration().set_string("parallelism.default", "1")

        t_env.create_temporary_table(
                'source',
        TableDescriptor.for_connector('filesystem')
                .schema(Schema.new_builder()
                .column('word', DataTypes.STRING())
                .build())
                .option('path', input_file)
                .format('csv')
                .build())
        tab = t_env.from_path('source')

        # doing transformation
        t_env.create_temporary_table(
                'sink',
        TableDescriptor.for_connector('filesystem')
                .schema(Schema.new_builder()
                .column('word', DataTypes.STRING())
                .column('count', DataTypes.BIGINT())
                .build())
                .option('path', output_file)
                .format('csv')          
                .build())
                
        tab = t_env.from_path('source')
        tab.group_by(tab.word) \
        .select(tab.word, lit(1).count) \
        .execute_insert('sink').wait()

    except Exception as e:
        print(e)

hashtag_count()