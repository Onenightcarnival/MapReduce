"""
Test case 1: word count
"""
def test1_get_inputs():
    import glob
    return glob.glob('word_count_data\\*.txt')

def test1_map_funciton(file):
    import json, re
    print("Mapping file: ", file)
    output = []
    with open(file, 'r', encoding='UTF-8') as f:
        for line in f:
            line = line.lower()
            line = re.sub(r'[^\w\s]', ' ', line)
            line = line.split()
            for w in line:
                output.append((w, 1))
    output_dir = "word_count_map\\" + file[16:-3] + "json"
    json.dump(output, open(output_dir, 'w' ))
    return output_dir

def test1_reduce_function(file):
    import json
    print("Reducing file: ", file)
    data = json.load(open(file))
    output = {}
    for k, v in data:
        if k in output:
            output[k] += v
        else:
            output[k] = v
    output_dir = "word_count_reduce\\" + file[15:]
    json.dump(output, open(output_dir, 'w' ))
    return output_dir

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
    return glob.glob('array_data\\*.json')

def test2_map_funciton(file):
    import json, bisect
    print("Mapping file: ", file)
    array, n_split, low, high = json.load(open(file)), 10, -100, 100
    intervals = list(range(low, high, (high-low)//n_split))
    output = {}
    if len(intervals) == n_split:
        for i in range(n_split):
            output[i+1] = []
    else:
        for i in range(n_split+1):
            output[i+1] = []
    for i in array:
        output[bisect.bisect_right(intervals, i)].append(i)
    output_dir = "array_data_map\\" + file[11:]
    json.dump(output, open(output_dir, 'w' ))
    return output_dir

def test2_reduce_function(file):
    import json
    print("Reducing file: ", file)
    output = json.load(open(file))
    for k in output:
    	output[k] = sorted(output[k])
    output_dir = "array_data_reduce\\" + file[15:]
    json.dump(output, open(output_dir, 'w' ))
    return output_dir

"""
Test case 3: total salary of each department
Given a salary table where each row is a tuple (str department, int salary).
Calculate the total salary by department.
"""
def test3_get_inputs():
    import glob
    return glob.glob('department\\*.json')

def test3_map_funciton(file):
    import json, re
    print("Mapping file: ", file)
    table = json.load(open(file))
    output = {}
    for row in table:
        if row[0] in output:
            output[row[0]].append(row[1])
        else:
            output[row[0]] = [row[1]]
    output_dir = "department_map\\" + file[11:]
    json.dump(output, open(output_dir, 'w' ))
    return output_dir

def test3_reduce_function(file):
    import json
    print("Reducing file: ", file)
    output = json.load(open(file))
    for k in output:
        output[k] = sum(output[k])
    output_dir = "department_reduce\\" + file[15:]
    json.dump(output, open(output_dir, 'w' ))
    return output_dir

