"""
It is faster not to use pickle
creating hash 0.754999876022 seconds
saving into plain text file 1.49800014496 seconds
loading from plain text file 5.93899989128 seconds
799999
serializing and saving to file: 12.9160001278 seconds
799999
loading serialized from file: 9.27999997139 seconds
"""

import time
import pickle

hash  = dict()

def create_hash(num=100000):
    for i in range(1,num):
        key= "/var/opt/nokia/oss/global/bsspmm/work/client//%s" % i
        hash[key] = i+1
        
        
def save_file():
    f = open("text_file", 'w')
    for file in hash.keys():
        ts = hash[file]
        f.write("%s|%s\n" % (file,ts))
    f.close()
    
def load_file():
    hash1= dict() 
    f = open("text_file", 'r')
    totallines = 0
    founditems = 0
    while 1:
        lines = f.readline(100000)
        if not lines:
            break
        line = lines.rstrip()
        totallines=totallines+1
        items = line.split('|')
        if len(items)>=1:
            founditems=founditems+1
            hash1[items[0]] = items[1]                
    f.close()


if __name__ == '__main__':
    #create hash with 100 000 elements
    start = time.time()
    create_hash(800000)
    end = time.time()-start
    print "creating hash %s seconds" % end
    
    
    start = time.time()
    save_file()
    end = time.time()-start
    print "saving into plain text file %s seconds" % end


    start = time.time()
    load_file()
    end = time.time()-start
    print "loading from plain text file %s seconds" % end

        
    #save hash to dump
    start = time.time()
    output = open('hash.pkl','wb')
    print len(hash)
    pickle.dump(hash,output)
    output.close()
    end = time.time()-start
    print "serializing and saving to file: %s seconds" % end
    
    #load hash from dump
    start = time.time()
    input_file = open('hash.pkl','r')
    hash1 = pickle.load(input_file)
    input_file.close()
    end = time.time()-start
    print len(hash1)
    print "loading serialized from file: %s seconds" % end    