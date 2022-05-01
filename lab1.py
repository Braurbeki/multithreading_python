from argparse import ArgumentParser
from concurrent.futures import thread
from functools import reduce
import os
import concurrent.futures
import time

def parse_args():
    parser = ArgumentParser()
    parser.add_argument('--path', help='path to root txt folder')
    return parser.parse_args()

def get_files(root):
    files = next(os.walk(root), (None, None, []))[2]
    return [os.path.join(root, files[i]) for i in range(len(files))]

def read_file(path):
    return open(path, 'r').read().split('\n')

def read_files_total(rootDir):
    res = []
    for f in get_files(rootDir):
        res += read_file(f)
    return res

def reducer(i, j):
    for k in j: i[k] = i.get(k, 0) + j.get(k, 0)
    return i

def single_thread_solution(rootDir):
    files = get_files(rootDir)
    res = dict()
    for file in files:
        for symbol in read_file(file):
            if symbol in res:
                res[symbol] += 1
            else:
                res[symbol] = 1
    return res
    
def single_thread_map_solution(rootDir):
    file_word_counters = map(lambda char: dict([[char, 1]]), read_files_total(rootDir))
    return reduce(reducer, file_word_counters)

def multi_thread_solution(rootDir):
    file_word_counters = []
    futures = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for file in get_files(rootDir):
            futures.append(executor.submit(lambda x: map(lambda char: dict([[char, 1]]), read_file(x)), file))
    for f in futures:
        file_word_counters.append(list(f.result()))
    return reduce(reducer, reduce(lambda a,b: a + b ,file_word_counters))
    

if __name__ == '__main__':
    args = parse_args()
    rootDir = str(args.path)
    start_time = time.time()
    print(single_thread_solution(rootDir))
    print(f'single_thread_solution: {time.time() - start_time}')
    start_time = time.time()
    single_thread_map_solution(rootDir)
    print(f'single_thread_map_solution: {time.time() - start_time}')
    start_time = time.time()
    multi_thread_solution(rootDir)
    print(f'multi_thread_solution: {time.time() - start_time}')
