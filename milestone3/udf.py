"""
Test case 1: word count
"""
def test1_get_inputs():
    import glob
    inputs = glob.glob('word_count_data\\*.txt')
    mapped_folder = 'word_count_map\\'
    reduced_folder = 'word_count_reduce\\'
    return (inputs, mapped_folder, reduced_folder)

def test1_map_funciton(file, N):
    import re
    print("Mapping file: ", file)
    output = {}
    for i in range(N):
    	output[i] = []
    with open(file, 'r', encoding='UTF-8') as f:
        for line in f:
            line = line.lower()
            line = re.sub(r'[^\w\s]', ' ', line)
            line = line.split()
            for w in line:
                output[ord(w[0])%N].append((w, 1))
    return output

def test1_reduce_function(folders, ID):
    import json
    print("Reducing intermediate files from mapper: ", ID)
    data = []
    for folder in folders:
    	data = data + json.load(open(folder+"\\"+str(ID)+".json"))
    output = {}
    for k, v in data:
        if k in output:
            output[k] += v
        else:
            output[k] = v
    return output

"""
Test case 2: sort an array into n euqal intervals of given range (low, high)
For example: 
	input array, n=5: [1,3,4,6,8,2,7,9,10,5]
	output:
		1: [1,2]
		2: [3,4]
		3: [5,6]
		4: [7,8]
		5: [9,10]
"""
def test2_get_inputs():
    import glob
    inputs =  glob.glob('array_data\\*.json')
    mapped_folder = 'array_data_map\\'
    reduced_folder = 'array_data_reduce\\'
    return (inputs, mapped_folder, reduced_folder)

def test2_map_funciton(file, N):
    import json, bisect
    print("Mapping file: ", file)
    array, low, high = json.load(open(file)), -100, 100
    intervals = list(range(low, high, (high-low)//N))
    if len(intervals) == N+1:
        intervals.pop(-1)
    output = {}
    for i in range(N):
        output[i] = []
    for num in array:
        for i in reversed(range(N)):
            if num >= intervals[i]:
                output[i].append(num)
                break
    return output

def test2_reduce_function(folders, ID):
    import json
    print("Reducing intermediate files from mapper: ", ID)
    output = []
    for folder in folders:
        output = output + json.load(open(folder+"\\"+str(ID)+".json"))
    return sorted(output)

"""
Test case 3: total salary of each department
Given a salary table where each row is a tuple (str department, int salary).
Calculate the total salary by department.
"""
def test3_get_inputs():
    import glob
    inputs =  glob.glob('department\\*.json')
    mapped_folder = 'department_map\\'
    reduced_folder = 'department_reduce\\'
    return (inputs, mapped_folder, reduced_folder)

def test3_map_funciton(file, N):
    import json
    print("Mapping file: ", file)
    table = json.load(open(file))
    output = {}
    for i in range(N):
        output[i] = []
    for row in table:
        output[ord(row[0][0])%N].append(row)
    return output

def test3_reduce_function(folders, ID):
    import json
    print("Reducing intermediate files from mapper: ", ID)
    table = []
    output = {}
    for folder in folders:
        table = table + json.load(open(folder+"\\"+str(ID)+".json"))
    for row in table:
        if row[0] in output:
            output[row[0]] += row[1]
        else:
            output[row[0]] = row[1]
    return output

