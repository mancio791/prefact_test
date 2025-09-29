#
# https://luigi.readthedocs.io/en/stable/index.html

import luigi


class T1(luigi.Task):
    def output(self):
        return luigi.LocalTarget("luigi_output00.txt")

    def run(self):
        print("In T1 run")
        with self.output().open("w") as f:
            f.write("Hello, Luigi!")
    

class T2(luigi.Task):
    

    def output(self):
        return luigi.LocalTarget("luigi_output01.txt")

    def run(self):
        print("In T2 run")
        other_target = yield T1()
        
        with other_target.open("r") as infile, self.output().open("w") as outfile:
            data = infile.read()
            outfile.write(data + "\nT2 completed.")    
            
if __name__ == "__main__":
    luigi.run(['T2', '--local-scheduler'])
    
    
    