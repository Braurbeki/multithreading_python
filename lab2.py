import datetime
from queue import Queue
from time import sleep
import threading

queue = Queue()
res_queue = Queue()
lock = threading.Lock()

class WorkerItem:
    def __init__(self, foo) -> None:
        self.foo = foo
    
    def run(self):
        global res_queue
        global queue
        res_queue.put(self.foo(queue.get()))

    def future(self):
        return Future()

class Future:
    def __init__(self) -> None:
        pass
    
    def result(self):
        global res_queue
        return res_queue.get()

class ThreadPoolExecutor:
    def __init__(self, max_workers) -> None:
        self.max_workers = max_workers
        self.workers = []

    def mmap(self, foo, arr):
        global queue
        self.workers = [WorkerItem(foo) for _ in range(len(arr))]
        for el in arr:
            queue.put(el)
        threading.Thread(target=self.init_workers, daemon=True).start()
        return [self.workers[i].future() for i in range(len(self.workers))]
    
    def init_workers(self):
        threads = [threading.Thread(target=worker.run, daemon=True) for worker in self.workers]
        for t in threads:
            t.start()
            if threading.active_count() - 2 >= 2:
                t.join()
        
def longRunningTask(x):
    sleep(2)
    return x * 2

if __name__ == '__main__':
    exec = ThreadPoolExecutor(max_workers=2)
    futures = exec.mmap(longRunningTask, [1, 2, 3, 4])
    for f in futures:
        print(f'Res: {f.result()}. Time: {datetime.datetime.now()} ')