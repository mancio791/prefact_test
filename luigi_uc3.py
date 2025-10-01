import luigi
import redis
import random
import string
import os
import datetime


class DummyTarget(luigi.Target):
    def __init__(self, path):
        self.path = path

    def exists(self):
        return True
    


class PreWriteToRedisTask(luigi.Task):
    def output(self):
        return DummyTarget(None)
    
    def run(self):
        print( self.output().items() )
    
    def complete(self):
        return True
        
        
        
        

class WriteToRedisTask(luigi.Task):
    redis_host = luigi.Parameter()
    redis_port = luigi.IntParameter(default=6379)
    redis_db = luigi.IntParameter()
    redis_password = luigi.Parameter()
    redis_key = luigi.Parameter()
    redis_value = luigi.Parameter()
    
    def requires(self):
        return PreWriteToRedisTask()
    
    def output(self):
        return luigi.LocalTarget("redis_key.tmp")
    
    def run(self):
        try:
            r = redis.Redis(host=self.redis_host, port=self.redis_port, db=self.redis_db, 
                            decode_responses=True, password=self.redis_password)

            r.set(self.redis_key, self.redis_value, ex=120)  # Imposta una scadenza di 120 secondi (2 minuti)

            # Scrive un file locale per marcare il completamento del task
            with self.output().open('w') as f:
                f.write(self.redis_key)
        except Exception as e:
            print(f"Error: {e}")
        finally:
            r.close()



class ReadFromRedisTask(luigi.Task):
    redis_host = luigi.Parameter()
    redis_port = luigi.IntParameter(default=6379)
    redis_db = luigi.IntParameter()
    redis_password = luigi.Parameter()
    redis_key = luigi.Parameter()

    # dipendenza dal task di scrittura
    def requires(self):
        return WriteToRedisTask(
            redis_host=self.redis_host,
            redis_port=self.redis_port,
            redis_db=self.redis_db,
            redis_password=self.redis_password,
            redis_key=self.redis_key,
            redis_value=''.join(random.choices(string.ascii_letters + string.digits, k=16))  # Genera un valore casuale di 16 caratteri
        )
    
    def output(self):
        return luigi.LocalTarget(f"redis_value_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.txt")
        
    
    def run(self):
        try:
            r = redis.Redis(host=self.redis_host, port=self.redis_port, db=self.redis_db, 
                            decode_responses=True, password=self.redis_password)
            
            rkey = self.input().open().read().strip()
            print(f"Reading key: {rkey}")
            value = r.get(rkey)
            print(f"Reading value: {value}")
            
            with self.output().open('w') as f:
                f.write(value if value else 'Key not found')
        
        except Exception as e:
            print(f"Error: {e}")
        else:
            os.remove(self.input().path)
        finally:
            r.close()



#-----------------------------------------
# Scheduler part



def createSchedule():
    from apscheduler.schedulers.background import BackgroundScheduler
    import time
    
    scheduler = BackgroundScheduler()
    
    def job():
        random_key = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        return luigi.build([ReadFromRedisTask(redis_key=random_key)], local_scheduler=True)


    scheduler.add_job(job, 'interval', seconds=60, id=f'jobid_{datetime.datetime.now().strftime("%Y%m%d%H%M%S")}')
    scheduler.start()

    # Una volta avviato lo scheduler, mantieni lo script in esecuzione
    
    try:
        # Keep the script running to allow the scheduler to run jobs
        while True:
            print("Next check in 30 seconds...")
            time.sleep(30)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()






if __name__ == "__main__":
    # Genera una stringa di 8 caratteri casuali (puoi cambiare la lunghezza)
    # random_key = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
    # print(random_key)
        
    # random_value = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
    # print(random_value)

    # result = luigi.build([ReadFromRedisTask(
    #     redis_key=random_key
    # )], local_scheduler=True)
    
    createSchedule()
    