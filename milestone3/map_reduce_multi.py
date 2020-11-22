from multiprocessing.connection import Listener, Client
import multiprocessing as mp
import json, os, time, random, sys

# run a reducer process which reduces all the intermediate files of specific keys to one file
def run_reducer(reducer, reduce_function, rd_ready, file_location, reduced_folder, ID):
    # random fault
    proba = [1, 1, 1, 1, 0]
    if random.choice(proba) == 0:
        sys.exit(1)
    # dump reduced file to disk
    reduced = reduce_function(file_location, ID+1)
    os.chdir(reduced_folder)
    json.dump(reduced, open("mapper_"+str(ID+1)+".json", 'w'))
    # run the service
    with Listener(reducer, authkey=b'secret password') as listener:
        # set the reducer service to ready
        rd_ready.set()
        with listener.accept() as conn:
            print('\nconnection accepted from', listener.last_accepted)
            # send back reduced file location to master
            conn.send(["From reducer: reducing finished ", reduced_folder+"mapper_"+str(ID+1)+".json"])
            conn.close()

# run a mapper process which generate N intermediate files by keys
def run_mapper(mapper, map_function, mp_ready, file_location, mapped_folder, N):
    # random fault
    proba = [1, 1, 1, 1, 0]
    if random.choice(proba) == 0:
        sys.exit(1)
    # dump n intermediate files to disk
    mapped = map_function(file_location, N)
    if os.path.isdir(mapped_folder) is False:
        os.makedirs(mapped_folder)
    os.chdir(mapped_folder)
    for i in range(len(mapped)):
        json.dump(mapped[i], open(str(i+1)+".json", 'w'))
    # run the service
    with Listener(mapper, authkey=b'secret password') as listener:
        # set the mapper service to ready
        mp_ready.set()
        with listener.accept() as conn:
            print('\nconnection accepted from', listener.last_accepted)
            # send back intermediate file location to master
            conn.send(["From mapper: "+file_location+" mapping finished ", mapped_folder])
            conn.close()

# run a master process
def run_master(master, mapper, reducer, user, run_mapper, run_reducer, map_function, reduce_function, ms_ready, us_ready, n_worker):
    files, mapped_folder, reduced_folder = None, None, None
    intermediate_locations = []
    reduced_locations = []

    # get input file locations from user
    with Listener(master, authkey=b'secret password') as listener:
        # set the master service to ready
        ms_ready.set()
        with listener.accept() as conn:
            print('\nconnection accepted from', listener.last_accepted)
            input_locations = conn.recv()
            files, mapped_folder, reduced_folder = input_locations[1], input_locations[2], input_locations[3]
            print(input_locations)
            conn.close()

    # create events for mappers and reducers
    mapper_event = []
    reducer_event = []
    for i in range(n_worker):
        mapper_event.append(mp.Event())
        reducer_event.append(mp.Event())

    # create mappers and send each input file location to one mapper process
    print("\nmapping start")
    mapper_proc = []
    for i in range(n_worker):
        mapper_proc.append(mp.Process(target=run_mapper, args=(mapper[i], map_function, mapper_event[i], 
            files[i], mapped_folder+str(i+1), n_worker)))
    for p in mapper_proc:
        p.start()

    # get the intermediate file locations from mappers
    time.sleep(1)
    mappers = list(range(n_worker))
    print("\nping mappers")
    while mappers:
        for i in mappers:
            if mapper_event[i].is_set():
                with Client(mapper[i], authkey=b'secret password') as conn:
                    msg = conn.recv()
                    print(msg)
                    intermediate_locations.append(msg[1])
                mappers.remove(i)
            else:
                # if detect mapper dead, respawn
                print("\nMapper "+str(i+1)+" crashed, restarting...")
                mapper_proc[i] = mp.Process(target=run_mapper, args=(mapper[i], map_function, mapper_event[i], 
                files[i], mapped_folder+str(i+1), n_worker))
                mapper_proc[i].start()
        time.sleep(1)

    # create reducers and send each intermediate file location to one reducer process
    print("\nreducing start")
    reducer_proc = []
    for i in range(n_worker):
        reducer_proc.append(mp.Process(target=run_reducer, args=(reducer[i], reduce_function, reducer_event[i], 
            intermediate_locations, reduced_folder, i)))
    for p in reducer_proc:
        p.start()

    # get the reduced file locations from reducers
    time.sleep(1)
    reducers = list(range(n_worker))
    print("\nping reducers\n")
    while reducers:
        for i in reducers:
            if reducer_event[i].is_set():
                with Client(reducer[i], authkey=b'secret password') as conn:
                    msg = conn.recv()
                    print(msg)
                    reduced_locations.append(msg[1])
                reducers.remove(i)
            else:
                # if detect reducer dead, respawn
                print("\nReducer "+str(i+1)+" crashed, restarting...")
                reducer_proc[i] = mp.Process(target=run_reducer, args=(reducer[i], reduce_function, reducer_event[i], 
            intermediate_locations, reduced_folder, i))
                reducer_proc[i].start()
        time.sleep(1)
    
    # wait for user service ready
    us_ready.wait()
    # notify user task finished
    with Client(user, authkey=b'secret password') as conn:
        conn.send("From master: Finished")

def map_reduce_multi(file_function, map_function, reduce_function):
    # get user input file locations
    files, mapped_folder, reduced_folder = file_function()
    # number of worker is equal to number of input files
    n_worker = len(files)
    # assign service ports
    user = ('localhost', 15000)
    master = ('localhost', 15001)
    mapper = [('localhost', x) for x in range(16000, 16000+n_worker)]
    reducer = [('localhost', x) for x in range(17000, 17000+n_worker)]
    ms_ready, us_ready = mp.Event(), mp.Event()
    # spawn master service
    master_proc = mp.Process(target=run_master, args=(
        master, mapper, reducer, user, 
        run_mapper, run_reducer, 
        map_function, reduce_function,
        ms_ready, us_ready, n_worker
        ))
    master_proc.start()
    # wait until master service ready
    ms_ready.wait()
    # send input file location to master
    with Client(master, authkey=b'secret password') as conn:
        conn.send(["From User", files, mapped_folder, reduced_folder])
    # run user service which waiting for master response
    with Listener(user, authkey=b'secret password') as listener:
        # set the user service state to ready
        us_ready.set()
        with listener.accept() as conn:
            print('\nconnection accepted from', listener.last_accepted)
            r = conn.recv()
            print(r)
            conn.close()