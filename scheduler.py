import threading
import random
import time

from crawler import Crawler

class Scheduler:
    def __init__(self, number_of_threads: int):
        self._crawlers = []
        self._threads = []
        self._progress = threading.Lock()

        for index in range(0, number_of_threads):
            self._crawlers.append(Crawler(index))
            self._threads.append(threading.Thread(target=self._run_thread, args=[index]))
            

    def _run_thread(self, index: int):
        self._crawlers[index].crawl()


    def _start_threads(self):
        for thread in self._threads:
            thread.start()
            time.sleep(2.0)


    def run(self):
        self._progress.acquire()
        self._start_threads()
        self._progress.release()

        while True:
            time.sleep(0.1)
            self._progress.acquire()
            if self.all_terminated():
                break
            self._progress.release()

        for t in self._threads:
            t.join()

        print("All threads terminated")


    def all_terminated(self) -> bool:
        for index in range(0, len(self._crawlers)):
            if self._threads[index].is_alive():
                return False

        return True
