import os
import sys
import subprocess
import numpy as np

def initial_centroid():

    Input = sys.argv[1]
    Output = sys.argv[2]
    k = int(sys.argv[3])
    tolerance = float(sys.argv[4])
    
    Sum = sys.maxint

    cat = subprocess.Popen(["hadoop", "fs", "-cat", Input+'/part-m-00000'], stdout=subprocess.PIPE)
    # max_min = subprocess.Popen(["hadoop", "fs", "-cat", "max_min"], stdout=subprocess.PIPE)

    count = 0
    centroid = []
    for line in cat.stdout:
        if count >= k: break
        #line = ' '.join(line)
        centroid.append(line) #add gaussian noise

        count += 1
        
    print('writing data...')
    f = open('centroid.txt','wb')
    for line in centroid:
        f.write(line)
    f.close()
    os.system('hdfs dfs -rm -R ' + Output)
    os.system('hdfs dfs -rm -R out')
    os.system('hdfs dfs -put centroid.txt ' + Output) 
    #os.system('rm centroid.txt')
    return Input, Output, k, tolerance, Sum

def run_job(Input, Output, k):
    os.system('yarn jar target/mr-app-1.0-SNAPSHOT.jar Kmeans ' + Input + ' out ' + Output + ' ' + str(k))
    os.system('hdfs dfs -rm -R ' + Output)
    os.system('hdfs dfs -cat out/part-* |hdfs dfs -put - ' + Output)
    os.system('hdfs dfs -rm -R out')


def main():
    Max_iter = 20
    Input, Output, k, tolerance, Sum = initial_centroid()

    print('start running mapreduce job')
    count = 0
    while Sum > tolerance and count < Max_iter-1:
        print('iteration: ' + str(count))
        run_job(Input, Output, k)
        count += 1
    run_job()    
        

if __name__ == '__main__':
    main()

