#
# https://apscheduler.readthedocs.io/en/3.x/

from apscheduler.schedulers.background import BackgroundScheduler
import time

scheduler = BackgroundScheduler()

def job():
    message = f"Job executed at {time.strftime('%Y-%m-%d %H:%M:%S')}"
    print(message)
    with open("job_log.txt", "a") as f:
        f.write(f"{message}\n")





if __name__ == "__main__":
    
    scheduler.add_job(job, 'interval', seconds=5, id='my_job_id')
    scheduler.start()

    try:
        # Keep the script running to allow the scheduler to run jobs
        
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        
        