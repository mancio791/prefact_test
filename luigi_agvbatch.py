import luigi
import json
import os
import uuid

from mtt.common import TaskConfig, RedisConfig, RedisConnection
from mtt.tasks import ExecuteRemoteShellReadCommandTask, generateInstance












#-----------------------------------------
# Scheduler part



def createSchedule(taskconfigkeys):
    from apscheduler.schedulers.background import BackgroundScheduler
    import time
    
    try:
        scheduler = BackgroundScheduler()
        
        def job(jobid):           
            # creazione task
            tc = TaskConfig(taskconfigkeys[0])
            tc.load()
            task = generateInstance(tc.type, job_id=jobid)
            task.task_config = tc
            task.task_deps.append('task_config_w')
            # esecuzione task
            return luigi.build([task], local_scheduler=True)
                

        print("Adding job to scheduler...")
        jobid = f'jobid_{uuid.uuid4()}'
        scheduler.add_job(job, kwargs={'jobid': jobid}, id=jobid, misfire_grace_time=5)
        print("New job added to scheduler", jobid)
        
        scheduler.start()                
        # scheduler.start(True)
        # print("Scheduler PAUSED")
        # time.sleep(2)
        # scheduler.resume()
        # print("Scheduler RESUMED")
        
        # Simulo il runtime period dello scheduler in una finestra di x secondi
        print("Attendo 30 secondi prima di terminare lo scheduler")
        time.sleep(30)
    except (KeyboardInterrupt, SystemExit):
        print("Manual interruption")
    else:
        print("GGGGOOOOAAAALLLL !!")
    # finally:
    #     scheduler.shutdown()
    #     print("Scheduler OFF")






#
# Creazione configurazione comando:
# il nome del file è la hkey in redis, i campi elencati sono gli item dell'hash
def saveTask(taskconfigfile) -> bool:
    hash_name = os.path.basename(taskconfigfile).split('.')[0]
    print(hash_name)
    
    try:
        with open(taskconfigfile, 'r') as tcf:
            d = tcf.read()
            jdata = json.loads(d) # dictionary
        
        redis_config = RedisConfig()
        rc = RedisConnection(redis_config)
        r = rc.open()
        
        for k,v in jdata.items():
            r.hset(hash_name, k, v)
    except Exception as e:
        print("Error", e)
        return False
    else:
        return True
    finally:
        rc.close()


def saveTasks(taskconfigfiles):
    for f in taskconfigfiles:
        saveTask(f)



if __name__ == "__main__":
    createSchedule(["task_config_r"])
    
    
    #saveTasks(["task_config_w.json","task_config_r.json"])
    