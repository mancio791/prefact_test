#
# https://luigi.readthedocs.io/en/stable/index.html

import luigi, time


class T1(luigi.Task):
    
    def output(self):
        return luigi.LocalTarget("luigi_output000.txt")

    def run(self):
        with self.output().open("w") as f:
            for i in range(100):
                f.write(f"Serviamo il numero {i}\n")
                if i % 10 == 0:
                    self.set_status_message("Progress: %d / 100" % i)
                    # displays a progress bar in the scheduler UI
                    self.set_progress_percentage(i)
                time.sleep(5.0)  # Simulate a long-running task



class T2(luigi.Task):

    def requires(self):
        return T1()

    def output(self):
        return luigi.LocalTarget("luigi_output001.txt")

    def run(self):
        with self.input().open("r") as infile, self.output().open("w") as outfile:
            data = infile.read()
            outfile.write(data + "\nT2 completed.")    


            
if __name__ == "__main__":
    luigi.run(['T2', '--local-scheduler'])
    
    
    
    