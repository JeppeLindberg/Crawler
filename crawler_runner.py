from scheduler import Scheduler

def run_crawler():
    scheduler = Scheduler(2)

    scheduler.run()


run_crawler()