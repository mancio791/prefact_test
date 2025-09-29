#
# https://luigi.readthedocs.io/en/stable/index.html

import luigi


# Define first task
class Task0(luigi.Task):
    def output(self):
        return luigi.LocalTarget("luigi_output0.txt")

    def run(self):
        with self.output().open("w") as f:
            f.write("Hello, Luigi!")


# Define a second task that depends on the first
class Task1(luigi.Task):
    def requires(self):
        return Task0() # Task1 depends on Task0

    def output(self):
        return luigi.LocalTarget("luigi_output1.txt")

    def run(self):
        with self.input().open("r") as infile, self.output().open("w") as outfile:
            data = infile.read()
            outfile.write(data + "\nTask1 completed.")
            
            
            
            

if __name__ == "__main__":
    luigi.run(['Task1', '--local-scheduler'])
    
    