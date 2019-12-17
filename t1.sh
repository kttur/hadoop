echo "Removing old data..."
rm -rf result_local
rm -rf result_local_trunc
hadoop fs -rm -r -f /user/root/tmp
hadoop fs -rm -r -f /user/root/result_hadoop
hadoop fs -rm -r -f /user/root/result_hadoop_trunc
echo "Done removing."
echo ".\n.\n.\nMRJob wiki.txt\n.\n"
time python3 job.py wiki.txt -o result_local
echo ".\n.\n.\nHadoop wiki.txt\n.\n"
time python3 job.py -r hadoop hdfs:///user/root/wiki.txt -o hdfs:///user/root/result_hadoop
echo ".\n.\n.\nMRJob wiki_trunc.txt\n.\n"
time python3 job.py wiki_trunc.txt -o result_local_trunc
echo ".\n.\n.\nHadoop wiki_trunc.txt\n.\n"
time python3 job.py -r hadoop hdfs:///user/root/wiki_trunc.txt -o hdfs:///user/root/result_hadoop_trunc
