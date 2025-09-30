import luigi
import redis
import random
import string
import os
import datetime



class WriteToRedisTask(luigi.Task):
    redis_host = luigi.Parameter()
    redis_port = luigi.IntParameter(default=6379)
    redis_db = luigi.IntParameter()
    redis_password = luigi.Parameter()
    redis_key = luigi.Parameter()
    redis_value = luigi.Parameter()
    
    
    def output(self):
        return luigi.LocalTarget("redis_key.tmp")
    
    def run(self):
        try:
            r = redis.Redis(host=self.redis_host, port=self.redis_port, db=self.redis_db, 
                            decode_responses=True, password=self.redis_password)
            
            r.set(self.redis_key, self.redis_value)
            
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


if __name__ == "__main__":
    # Genera una stringa di 8 caratteri casuali (puoi cambiare la lunghezza)
    random_key = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
    # print(random_key)
        
    # random_value = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
    # print(random_value)

    luigi.build([ReadFromRedisTask(
        redis_key=random_key
    )], local_scheduler=True)