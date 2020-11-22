from multiprocessing.connection import Listener, Client
import multiprocessing as mp

# run a reducer process
def run_reducer(reducer, reduce_function, rd_ready):
    job_input = None
    output_dir = []
    # run the service
    with Listener(reducer, authkey=b'secret password') as listener:
        # set the reducer service to ready
        rd_ready.set()
        with listener.accept() as conn:
            print('\nconnection accepted from', listener.last_accepted)
            # obtain input file location from master
            job_input = conn.recv()
            print(job_input)
            # run reduce job
            for j in job_input[1]:
                reduced = reduce_function(j)
                output_dir.append(reduced)
            # send the location of reduced file to master
            conn.send(["From reducer: reducing finished ", output_dir])
            conn.close()

# run a mapper process
def run_mapper(mapper, map_function, mp_ready):
    job_input = None
    output_dir = []
    # run the service
    with Listener(mapper, authkey=b'secret password') as listener:
        # set the mapper service to ready
        mp_ready.set()
        with listener.accept() as conn:
            print('\nconnection accepted from', listener.last_accepted)
            # obtain input file location from master
            job_input = conn.recv()
            print(job_input)
            # run map job
            for j in job_input[1]:
                mapped = map_function(j)
                output_dir.append(mapped)
            # send the location of mapped file to master
            conn.send(["From mapper: mapping finished ", output_dir])
            conn.close()

def run_master(master, mapper, reducer, user, run_mapper, run_reducer, map_function, reduce_function, ms_ready, us_ready):
    user_input = None
    mapper_output = None
    # spawn mapper, reducer processes
    mp_ready, rd_ready = mp.Event(), mp.Event()
    map_proc = mp.Process(target=run_mapper, args=(mapper, map_function, mp_ready))
    reduce_proc = mp.Process(target=run_reducer, args=(reducer, reduce_function, rd_ready))
    map_proc.start()
    reduce_proc.start()

    # get input from user
    with Listener(master, authkey=b'secret password') as listener:
        # set the master service to ready
        ms_ready.set()
        with listener.accept() as conn:
            print('\nconnection accepted from', listener.last_accepted)
            user_input = conn.recv()
            print(user_input)
            conn.close()
    
    # wait for mapper service ready
    mp_ready.wait()
    # send task to mapper and receive the output file location
    with Client(mapper, authkey=b'secret password') as conn:
        conn.send(["From master: ", user_input[1]])
        mapper_output = conn.recv()
        print(mapper_output)
    
    # wait for reducer service ready
    rd_ready.wait()
    # send task to reducer and receive the output file location
    with Client(reducer, authkey=b'secret password') as conn:
        conn.send(["From master: ", mapper_output[1]])
        reducer_output = conn.recv()
        print(reducer_output)
    
    # wait for user service ready
    us_ready.wait()
    # notify user task finished
    with Client(user, authkey=b'secret password') as conn:
        conn.send("From master: Finished")

def map_reduce_multi(file_function, map_function, reduce_function):
    # assign service ports
    user = ('localhost', 15000)
    master = ('localhost', 15001)
    mapper = ('localhost', 15002)
    reducer = ('localhost', 15003)
    ms_ready, us_ready = mp.Event(), mp.Event()
    # get user input file location
    files = file_function()
    print(files, len(files), '\n')
    # spawn master service
    master_proc = mp.Process(target=run_master, args=(
        master, mapper, reducer, user, 
        run_mapper, run_reducer, 
        map_function, reduce_function,
        ms_ready, us_ready
        ))
    master_proc.start()
    # wait until master service ready
    ms_ready.wait()
    # send input file location to master
    with Client(master, authkey=b'secret password') as conn:
        conn.send(["From User", files])
    # run user service which waiting for master response
    with Listener(user, authkey=b'secret password') as listener:
        # set the user service state to ready
        us_ready.set()
        with listener.accept() as conn:
            print('\nconnection accepted from', listener.last_accepted)
            r = conn.recv()
            print(r)
            conn.close()