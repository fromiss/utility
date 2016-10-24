#list folder, find same size, md5 these files, {md5:[file_1, file_2 ...]}
import sys
import os
import hashlib
import signal
from multiprocessing import Pool

def build_file_list(file_list, dirname, names):
    for file_name in names:
        full_file_name=os.path.join(os.path.abspath(dirname), file_name)
        if (os.path.isfile(full_file_name) and not os.path.islink(full_file_name)):
            file_list.append(full_file_name)
            
    return

BLOCK_SIZE=65536
def file_hash(afile):
    hasher = hashlib.md5()

    with open(afile, 'rb') as afile:
        buf = afile.read(BLOCK_SIZE)
        while len(buf)>0:
            hasher.update(buf)
            buf = afile.read(BLOCK_SIZE)
    file_hash = hasher.hexdigest()
    return file_hash

def build_file_hash_dict(file_name):
    key = file_hash(file_name)
#    print "Hash " + key + ":" +file_name
    return {key : file_name}

def build_file_size_dict(file_name):
    key = os.path.getsize(file_name)
#    print "Size " + str(key) + ":" +file_name
    return {key : file_name}

#dict, list process function, file_list
def build_dict(adict, build_dic, file_list):
    print "build adict"
    pool = Pool(processes = 4)
    dict_list = pool.map(build_dic, file_list)
    pool.close()
    pool.join()
#    print str(len(file_list)) + " files"
    for i in dict_list:
        assert(len(i.keys()) ==1 )
        key = i.keys()[0]
        value = i.values()[0]
        if key not in adict:
            adict.update({key : list([value])})
        else:
            adict[key].append(value)
    #build a list with same key                     
    return

def find_duplicate_files(file_hash_dict):
    file_count=1
    for file_names in file_hash_dict.values():
        if(len(file_names)) > 1:
            print "Find same file No."+str(file_count)
            file_count = 1 + file_count
            for file_name in file_names:
                print "\t"+file_name 
    return

#if __name__ == '__main__':
try:
    if len(sys.argv) < 2:
        root_dir = "./"
    else:
        root_dir = sys.argv[1]
    # {file_size:file_name}
    # build a list with same size, then md5 these files
    file_list = []
    # {file_size : [file1, file2...]}
    file_size_dict = {}
    # {file_hash : [file1, file2...]}
    file_hash_dict = {}
    os.path.walk(root_dir, build_file_list, file_list)
    print "Total "+str(len(file_list))+" files"

    print "Build file size dict"
    build_dict(file_size_dict, build_file_size_dict, file_list)
    print "Finish build file size dict"

    file_list = []    
    # put all same size files in a list
    for (key, value) in file_size_dict.items():
        if (len(value) > 1):
            file_list.extend(file_size_dict[key])

    print "Building file hash dict"
    build_dict(file_hash_dict, build_file_hash_dict, file_list)
    print "Finish build file hash dict"

    find_duplicate_files(file_hash_dict)
except KeyboardInterrupt:
    print("W: interrupt received, stopping")
