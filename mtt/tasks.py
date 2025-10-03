import luigi
from .common import TaskConfig, RedisConfig, RedisConnection
import uuid
import paramiko
import io
import os
import pathlib




class ExecutionConfigurationTask(luigi.Task):
    '''
    Task di scrittura registry attività 
    '''
    
    job_id = luigi.Parameter()
    
    # uuid identificativo riferimenti configurazione
    uniqueId = str(uuid.uuid4())
    
    def output(self):
        # return luigi.LocalTarget(f"temp/exec_context_{self.uniqueId}.json")
        return luigi.LocalTarget('/'.join([str(pathlib.Path.cwd()),'temp', f"dummy_{self.job_id}.json"]))
    
    def run(self):
        try:
            redis_config = RedisConfig()
            rc = RedisConnection(redis_config)
            r = rc.open()
            # registrazione esecuzione job
            hash_name = "scheduler_registry"
            r.hset(hash_name, self.uniqueId, f"[JOB] {self.job_id}")
            r.hexpire(hash_name, 180, self.uniqueId)
        except Exception as e:
            print(f"Error: {e}")
        else:
            # scrivo le info di connessione all'host remoto nel file di output
            with self.output().open('w') as f: f.write("dummy")
        finally:
            rc.close()






class ExecuteRemoteShellWriteCommandTask(luigi.Task):
    job_id = luigi.Parameter()
    
    task_config = TaskConfig
    
    def requires(self):
        task = ExecutionConfigurationTask(job_id=self.job_id)
        return task
    
    
    def output(self):
        return luigi.LocalTarget('/'.join([str(pathlib.Path.cwd()),'temp', f"{self.job_id}.json"]))
    
    def run(self):
        # collegamento al server remoto ed esecuzione del comando
        try:
            # recupero info connessione
            redis_config = RedisConfig()
            rc = RedisConnection(redis_config)
            r = rc.open()
            remote_config = r.hgetall(self.task_config.connection)
            
            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                private_key = io.StringIO(remote_config["private_key"])
                pkey = paramiko.RSAKey.from_private_key(private_key)
                
                ssh.connect(remote_config["host"], remote_config["port"], 
                            username=remote_config["username"], pkey=pkey)
                
                # recupero il comando
                print("Executing command", {self.task_config.cmd})

                (stdin,stdout,stderr) = ssh.exec_command(self.task_config.cmd)
                stdout.channel.recv_exit_status()
                stdout.channel.set_combine_stderr(True)
                for message in self.sshMessageGenerator(stdout):
                    print(message)
                
        except Exception as e:
            print(f"Error: {e}")
        else:
            print("Removing", self.input().path)
            os.remove(self.input().path)
            # scrivo le info di connessione all'host remoto nel file di output
            with self.output().open('w') as f: f.write(f"End of {self.task_id}")
        finally:
            ssh.close()
            rc.close()
          
            
    def sshMessageGenerator(self, stdout):
        for m in stdout.readlines():
            yield m.rstrip()




class ExecuteRemoteShellReadCommandTask(luigi.Task):
    job_id = luigi.Parameter()
    
    task_config = TaskConfig
            
    def requires(self):
        # task = ExecuteRemoteShellWriteCommandTask(job_id=self.job_id)
        tc = TaskConfig(self.task_config.dependsOn)
        tc.load()
        task = generateInstance(tc.type, job_id=self.job_id)
        task.task_config = tc
        return task
    
    def output(self):
        return luigi.LocalTarget('/'.join([str(pathlib.Path.cwd()),'temp', f"finish_{self.job_id}.json"]))
    
    def run(self):
        # collegamento al server remoto ed esecuzione del comando
        try:           
            # recupero info connessione
            redis_config = RedisConfig()
            rc = RedisConnection(redis_config)
            r = rc.open()
            remote_config = r.hgetall(self.task_config.connection)
            
            with paramiko.SSHClient() as ssh:
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                private_key = io.StringIO(remote_config["private_key"])
                pkey = paramiko.RSAKey.from_private_key(private_key)
                
                ssh.connect(remote_config["host"], remote_config["port"], 
                            username=remote_config["username"], pkey=pkey)
                
                # recupero il comando
                print("Executing command", {self.task_config.cmd})

                (stdin,stdout,stderr) = ssh.exec_command(self.task_config.cmd)
                stdout.channel.recv_exit_status()
                stdout.channel.set_combine_stderr(True)
                for message in self.sshMessageGenerator(stdout):
                    print(message)
                
        except Exception as e:
            print(f"Error: {e}")
        else:
             os.remove(self.input().path)
             # scrivo le info di connessione all'host remoto nel file di output
             with self.output().open('w') as f: f.write(f"End of {self.task_id}")
        finally:
            ssh.close()
            rc.close()
          
            
    def sshMessageGenerator(self, stdout):
        for m in stdout.readlines():
            yield m.rstrip()





@luigi.Task.event_handler(luigi.Event.SUCCESS)
def task_callback(task:luigi.Task):
    # print(f"-+-+-+-+-+-+- Task {task.task_id} completed !")
    redis_config = RedisConfig()
    try:
        rc = RedisConnection(redis_config)
        r = rc.open()
        # registrazione esecuzione job
        hash_name = "scheduler_registry"
        field_key = str(uuid.uuid4())  
        r.hset(hash_name, field_key, f"[TASK] ({task.to_str_params()['job_id']}) {task.task_id} [SUCCESS]")
        r.hexpire(hash_name, 180, field_key)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        rc.close()
    

def generateInstance(classname, **kwargs):
    c = globals()[classname]
    i = c(**kwargs)
    return i