import luigi
import redis
import datetime
import paramiko
import json
import os
import io




class Connection:
    def __init__(self, host, port):
        super().__init__()
        self.host = host
        self.port = port

class UsernamePasswordConnection(Connection):
    def __init__(self, host, port, username=None, password=None):
        super().__init__(host, port)
        self.username = username
        self.password = password
        
class KeyPairConnection(UsernamePasswordConnection):
    def __init__(self, host, port, username=None, password=None):
        super().__init__(host, port, username, password)
    
    @property
    def privateKey(self):
        return self._privateKey

    @privateKey.setter
    def privateKey(self, value):
        self._privateKey = value

    @privateKey.deleter
    def privateKey(self):
        del self._privateKey



class RedisConnection(UsernamePasswordConnection):
    def __init__(self, host, port, username=None, password=None, database=0):
        super().__init__(host, port, username, password)
        self.database = database
        
    def open(self):
        self._connection =  redis.Redis(host=self.host, port=self.port, db=self.database, 
                                        decode_responses=True, password=self.password)
        return self._connection
    
    def close(self):
        self._connection.close()


    






















class ExecutionConfiguration(luigi.Task):
    job_id = luigi.Parameter()
    
    #----- lette da cfg
    redis_host = luigi.Parameter()
    redis_port = luigi.IntParameter(default=6379)
    redis_db = luigi.IntParameter()
    redis_password = luigi.Parameter()
    #----- lette da cfg
    
    redis_task_key = luigi.Parameter() # key redis per recuperare il comando da eseguire
    remote_config_key = luigi.Parameter() # key redis per recuperare le info di connessione ssh al server remoto

    
    def output(self):
        return luigi.LocalTarget(f"temp/exec_context_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.json")
    
    def run(self):
        try:
            rc = RedisConnection(host=self.redis_host, port=self.redis_port, database=self.redis_db, 
                                password=self.redis_password)
            r = rc.open()
            # registrazione esecuzione job
            hash_name = "scheduler_registry"
            field_key = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
            r.hset(hash_name, field_key, self.job_id)
            r.hexpire(hash_name, 180, field_key)
            
            # recupero configurazione connessione remote host
            remote_config = r.hgetall(self.remote_config_key)
            # recupero comando da eseguire
            cmd = r.hget(self.redis_task_key, 'cmd')

            data_ = dict()
            data_["cmd"] = cmd
            data_["remote"] = remote_config
            
            with self.output().open('w') as f: 
                f.write(json.dumps(data_))
        except Exception as e:
            print(f"Error: {e}")
        finally:
            rc.close()
            
            
            
            
            
            
            
            

class ExecuteRemoteShellCommand(luigi.Task):
    job_id = luigi.Parameter()
    redis_key = luigi.Parameter()
    
    def requires(self):
        return ExecutionConfiguration(redis_task_key=self.redis_key, remote_config_key="agevolo_batch_pd_connection", 
                                      job_id=self.job_id)
    
    def output(self):
        return luigi.LocalTarget("temp/dummy.tmp")
    
    def run(self):
        # collegamento al server remoto ed esecuzione del comando
        try:
            self.inputfile = self.input().path
            with open(self.inputfile, 'r') as data:
                d = data.read()
                jdata = json.loads(d)
                self.remote_config = jdata["remote"]
                cmd = jdata["cmd"]
            
            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                private_key = io.StringIO(self.remote_config["private_key"])
                pkey = paramiko.RSAKey.from_private_key(private_key)
                
                ssh.connect(self.remote_config["host"], self.remote_config["port"], 
                            username=self.remote_config["username"], pkey=pkey)
                
                # recupero il comando
                print("Executing command", {cmd})

                (stdin,stdout,stderr) = ssh.exec_command(cmd)
                stdout.channel.recv_exit_status()
                stdout.channel.set_combine_stderr(True)
                for message in self.sshMessageGenerator(stdout):
                    print(message)
                
        except Exception as e:
            print(f"Error: {e}")
        else:
             os.remove(self.inputfile)
        finally:
            ssh.close()
            
            
    def sshMessageGenerator(self, stdout):
        for m in stdout.readlines():
            yield m.rstrip()








#-----------------------------------------
# Scheduler part



def createSchedule(redisCommandKey):
    from apscheduler.schedulers.background import BackgroundScheduler
    
    try:
        scheduler = BackgroundScheduler()
        
        def job(jobid):
            return luigi.build([ExecuteRemoteShellCommand(redis_key=redisCommandKey,job_id=jobid)], local_scheduler=True)

        jobid = f'jobid_{datetime.datetime.now().strftime("%Y%m%d%H%M%S")}'
        scheduler.add_job(job, kwargs={'jobid': jobid}, id=jobid)
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("Manual interruption")
    # finally:
    #     scheduler.shutdown()






if __name__ == "__main__":
    createSchedule("agevolo_mctrent_feabatchtest")
    