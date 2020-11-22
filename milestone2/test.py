from udf import *
from map_reduce_multi import *

def run_multi():
    print("\nRunning test1 word count\n")
    map_reduce_multi(test1_get_inputs, test1_map_funciton, test1_reduce_function)
    print("\nRunning test2 sort array\n")
    map_reduce_multi(test2_get_inputs, test2_map_funciton, test2_reduce_function)
    print("\nRunning test3 total salary of each department\n")
    map_reduce_multi(test3_get_inputs, test3_map_funciton, test3_reduce_function)

if __name__ == '__main__':
    mp.set_start_method('spawn')
    print("\nMAP REDUCE START")
    run_multi()
    
    
