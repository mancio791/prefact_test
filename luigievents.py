#
# https://luigi.readthedocs.io/en/stable/index.html

import luigi



# Define first task
class Task1(luigi.Task):
    def output(self):
        return luigi.LocalTarget("luigi_output_event_t1.txt")

    def run(self):
        with self.output().open("w") as f:
            f.write("Hello, Luigi!")


# Define a second task that depends on the first
class Task2(luigi.Task):
    def requires(self):
        return Task1() # Task2 depends on Task1

    def output(self):
        return luigi.LocalTarget("luigi_output_event_t2.txt")

    def run(self):
        with self.input().open("r") as infile, self.output().open("w") as outfile:
            data = infile.read()
            outfile.write(data + "\nTask1 completed.")
            
            
@luigi.Task.event_handler(luigi.Event.SUCCESS)
def task1_callback(task):
    print(f"Task {task} has finished!")
    
@luigi.Task.event_handler(luigi.Event.START)
def task2_callback(task):
    print(f"Task {task} is starting")    

@luigi.Task.event_handler(luigi.Event.FAILURE)
def task2_callback(task):
    print(f"Task {task} failed !") 


            

if __name__ == "__main__":
    luigi.run(['Task2', '--local-scheduler'])
    
    