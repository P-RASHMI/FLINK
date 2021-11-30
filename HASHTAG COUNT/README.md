TO PERFORM HASHTAG COUNT  USING PYFLINK BY GENERATING HASHTAG WORDS AND STORE IT INTO INPUT FILE AND GET OUTPUT FILE WITH WORD,COUNT:

Python Version:(python3 --version):3.8.10

Flink Version:(flink --version):1.13.1

Install flink and pyflink libraries in terminal:

 	~$ pip3 install apache-flink
 
	~$ pip3 install pyflink
	
comment FLINK_HOME path in .bashrc file and save changes with source .bashrc	

Import packages from pyflink and give hashtag word, write into input file 

Read from that file and have the word count for generated hashtags as output file.

Run in vs code and you can see words generated in input file 

THe word and its count in output file.

Stop the clusters :stop-cluster.sh


