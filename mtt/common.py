
import luigi
import redis
from paramiko import PKey, RSAKey, Ed25519Key
from abc import abstractmethod, ABC


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
    
    
    @property
    def keyAlgorithm(self):
        return self._keyAlgorithm

    @keyAlgorithm.setter
    def keyAlgorithm(self, value):
        self._keyAlgorithm = value






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

    
    @property
    def description(self): return self._description

    @description.setter
    def description(self, value): self._description = value

    
    @property
    def connection(self): return self._connection

    @connection.setter
    def connection(self, value): self._connection = value

    
    @property
    def cmd(self): return self._cmd

    @cmd.setter
    def cmd(self, value): self._cmd = value

    
    @property
    def type(self): return self._type

    @type.setter
    def type(self, value): self._type = value

    
    @property
    def dependsOn(self): return self._dependsOn

    @dependsOn.setter
    def dependsOn(self, value): self._dependsOn = value

    

    
    def load(self) -> bool:
        try:           
            redis_config = RedisConfig()
            rc = RedisConnection(redis_config)
            r = rc.open()
            htc = r.hgetall(self.name)
            self.description = htc['description']
            self.connection = htc['connection']
            self.cmd = htc['cmd']
            self.type = htc['type']
            self.dependsOn = htc['dependsOn']
            
        except Exception as e:
            print("Error", e)
            return False
        else:
            return True
        finally:
            rc.close()





#
# https://refactoring.guru/design-patterns/strategy/python/example


class KeyStrategy(ABC):
    
    @abstractmethod
    def getKey(private_key) -> PKey:
        pass

class RSAKeyStrategy(KeyStrategy):    
    def getKey(self, private_key) -> PKey:
        return RSAKey.from_private_key(private_key)

class ED25519KeyStrategy(KeyStrategy):
    def getKey(self, private_key) -> PKey:
        return Ed25519Key.from_private_key(private_key)


class Context:
    def __init__(self, keyAlgorithmRef):
        clazz = globals()[keyAlgorithmRef]
        self._strategy = clazz()
        
    @property
    def strategy(self) -> KeyStrategy:
        """
        The Context maintains a reference to one of the Strategy objects. The
        Context does not know the concrete class of a strategy. It should work
        with all strategies via the Strategy interface.
        """

        return self._strategy

    @strategy.setter
    def strategy(self, strategy: KeyStrategy) -> None:
        """
        Usually, the Context allows replacing a Strategy object at runtime.
        """

        self._strategy = strategy
    
            
    def retrieveKey(self, private_key) -> PKey:
        return self._strategy.getKey(private_key)
    
    