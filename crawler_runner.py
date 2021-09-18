from scheduler import Scheduler

def run_crawler():
    scheduler = Scheduler(5)

    scheduler.run()


run_crawler()