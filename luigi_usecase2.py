import luigi

"""
Un po' di docuemntazione per capiro cosa fa questo workflow:

1 generare una coppia di chiavi ssh
2 copiare la chiave pubblica sul server remoto
3 connettersi al server remoto utilizzando la chiave privata ed eseguire un comando
"""
    
class GenerateSSHKeys(luigi.Task):
    def output(self):
        return luigi.LocalTarget('ssh_keys/id_rsa.pub')

    def run(self):
        import os
        os.makedirs('ssh_keys', exist_ok=True)
        os.system('ssh-keygen -t rsa -b 2048 -f ssh_keys/id_rsa -q -N ""')
        
class CopyPublicKeyToRemote(luigi.Task):
    def requires(self):
        return GenerateSSHKeys()

    def output(self):
        return luigi.LocalTarget('ssh_keys/copied.txt')

    def run(self):
        import os
        remote_user = 'mtt'
        remote_host = '192.168.3.56'
        os.system(f'ssh-copy-id -i ssh_keys/id_rsa.pub {remote_user}@{remote_host}')  # run only on linux hosts
        with self.output().open('w') as f:
            f.write('Public key copied to remote server.\n')

class ExecuteRemoteCommand(luigi.Task):
    def requires(self):
        return CopyPublicKeyToRemote()

    def output(self):
        return luigi.LocalTarget('ssh_keys/command_output.txt')

    def run(self):
        import os
        remote_user = 'mtt'
        remote_host = '192.168.3.56'
        os.system(f'ssh -i ssh_keys/id_rsa {remote_user}@{remote_host} "systemctl | grep agevolo"')
        with self.output().open('w') as f:
            f.write('Remote command executed.\n')
            
if __name__ == '__main__':
    luigi.run(main_task_cls=ExecuteRemoteCommand, local_scheduler=True)
    