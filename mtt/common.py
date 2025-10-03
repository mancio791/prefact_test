
import luigi
import redis


class RedisConfig(luigi.Config):
    host = luigi.Parameter()
    port = luigi.IntParameter(default=6379)
    db = luigi.IntParameter()
    password = luigi.Parameter()
    username = luigi.Parameter()
    
    

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
    def __init__(self, redisConfig: RedisConfig):
        super().__init__(redisConfig.host, redisConfig.port, redisConfig.username, redisConfig.password)
        self.database = redisConfig.db
    
    
    def open(self):
        self._connection =  redis.Redis(host=self.host, port=self.port, db=self.database, 
                                        decode_responses=True, password=self.password)
        return self._connection
    
    def close(self):
        self._connection.close()







class TaskConfig:
    def __init__(self, name):
        self.name = name

    @property
    def name(self): return self._name

    @name.setter
    def name(self, value): self._name = value

    @name.deleter
    def name(self): del self._name
    
    @property
    def description(self): return self._description

    @description.setter
    def description(self, value): self._description = value

    @description.deleter
    def description(self): del self._description
    
    @property
    def connection(self): return self._connection

    @connection.setter
    def connection(self, value): self._connection = value

    @connection.deleter
    def connection(self): del self._connection
    
    @property
    def cmd(self): return self._cmd

    @cmd.setter
    def cmd(self, value): self._cmd = value

    @cmd.deleter
    def cmd(self): del self._cmd

    
    def load(self) -> bool:
        try:           
            redis_config = RedisConfig()
            rc = RedisConnection(redis_config)
            r = rc.open()
            htc = r.hgetall(self.name)
            self.description = htc['description']
            self.connection = htc['connection']
            self.cmd = htc['cmd']
            
        except Exception as e:
            print("Error", e)
            return False
        else:
            return True
        finally:
            rc.close()

