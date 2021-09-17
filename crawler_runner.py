from scheduler import Scheduler

def run_crawler():
    scheduler = Scheduler(10)

    scheduler.run()


run_crawler()