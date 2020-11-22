from multiprocessing.connection import Listener, Client
import multiprocessing as mp
from udf import *

# run a master process
def run_master(master, map_function, reduce_function, ms_ready):
    user_input = None
    mapper_output = []
    reducer_output = []
    
    with Listener(master, authkey=b'secret password') as listener:
        # set the master service to ready
        ms_ready.set()
        with listener.accept() as conn:
            print('\nconnection accepted from', listener.last_accepted)
            # get input from user
            user_input = conn.recv()
            print(user_input, "\n")
            # map 
            for i in user_input[1]:
                mapper_output.append(map_function(i))
            # reduce 
            for i in mapper_output:
                reducer_output.append(reduce_function(i))
            # notify user
            conn.send("From master: Finished")
            conn.close()    

def map_reduce_single(file_function, map_function, reduce_function):
    # assign service ports
    master = ('localhost', 15000)

    ms_ready = mp.Event()
    # get user input file location
    files = file_function()
    print(files, len(files), '\n')
    # spawn master service
    master_proc = mp.Process(target=run_master, args=(
        master, map_function, reduce_function, ms_ready
        ))
    master_proc.start()
    # wait for user service ready
    ms_ready.wait()
    # notify user task finished
    with Client(master, authkey=b'secret password') as conn:
        conn.send(["From User", files])
        print("\n", conn.recv())