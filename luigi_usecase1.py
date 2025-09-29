import luigi
import os



class CertificateConfig(luigi.Config):
    country = luigi.Parameter(default="XX")
    state = luigi.Parameter(default="xxxx")
    locality = luigi.Parameter(default="xxxx")
    organization = luigi.Parameter(default="xxxx")
    organizational_unit = luigi.Parameter(default="XX")
    common_name = luigi.Parameter(default="xxxx.yy.com")
    email = luigi.Parameter(default="xxx@yyy.zz")
    output_path = luigi.Parameter(default="xxx.pem")
    key_path = luigi.Parameter(default="xxxx.key")


class GenerateCertificate(luigi.Task):
    country = CertificateConfig().country
    state = CertificateConfig().state
    locality = CertificateConfig().locality
    organization = CertificateConfig().organization
    organizational_unit = CertificateConfig().organizational_unit
    common_name = CertificateConfig().common_name
    email = CertificateConfig().email
    output_path = CertificateConfig().output_path
    key_path = CertificateConfig().key_path
    
    def output(self):
        return luigi.LocalTarget(self.output_path)
    
    def run(self):
        os.system(f"mkdir certs")
        cmd = (
            f"openssl req -new -newkey rsa:2048 -days 365 -nodes -x509 "
            f"-subj '/C={self.country}/ST={self.state}/L={self.locality}/O={self.organization}/OU={self.organizational_unit}/CN={self.common_name}/emailAddress={self.email}' "
            f"-keyout certs/{self.key_path} -out certs/{self.output_path}"
        )
        
        with open("certs/info.txt", 'w') as f:
            f.write(f"Certificate generated at certs/{self.output_path}\n")
            f.write(f"Key generated at certs/{self.key_path}\n")
            f.write(f"Command executed: {cmd}\n")
        
        
if __name__ == "__main__":
    luigi.run(main_task_cls=GenerateCertificate, local_scheduler=True)