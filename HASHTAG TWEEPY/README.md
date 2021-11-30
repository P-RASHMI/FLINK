TO PERFORM HASHTAG COUNT BY TAKING TRENDING TWEETS USING PYFLINK,tweepy BY STORE IT INTO INPUT FILE AND GET OUTPUT FILE WITH WORD,COUNT:

Python Version:(python3 --version):3.8.10

Flink Version:(flink --version):1.13.1

Install flink and pyflink libraries in terminal(is already installed ignore):

~$ pip3 install apache-flink

~$ pip3 install pyflink

install tweepy

comment FLINK_HOME path in .bashrc file and save changes with source .bashrc

VISIT website of trends24 and get the trending hashtags:

	https://trends24.in/india/

Import packages from pyflink ,Give TRENDING HASHTAG WORDS as list,writes into input file and generate output file with count


Run in vs code and you can see words generated in input file

The word and its count in output file.

