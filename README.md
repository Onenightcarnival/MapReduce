# Project 2: Online ML Inference

# Collaborater: Chao Li, Zhihan Ying, Sihua Chen

# Project Folder: Online_ML_Inference

# How to run the APP (all 3 options were tested on Ubuntu 20.04)

# Option1: directly run<br>

Step1: clone this repo and go to project folder

Step2: install dependency:

`pip3 install requirements.txt`

Step3: run the server:

`FLASK_ENV=development FLASK_APP=app.py flask run`

# Option2: build docker image using the docker file we provide

Step1: install docker

Step2: clone this repo, and run following commands in the root directory of this repo

`docker build -t eveying/proj2 Online_ML_Inference`

`docker run -d -t -p 8000:8000 eveying/proj2`

# Option3: use prebuilt docker image

Step1: install docker

Step2: run following command

`docker run -d -t -p 8000:8000 eveying/proj2:v1.0`

Link: https://hub.docker.com/r/eveying/proj2

## how to test:

Option1: use our frontend

Open `localhost:8000`, upload file to test, here we provide 'dog.jpg' in the project folder for testing, you can use your image.

Option2: use curl in terminal

`curl -F "file=@Online_ML_Inference/dog.jpg" http://127.0.0.1:8000`

# Project 1: MapReduce

# milestone 1: one mapper, one reducer, one master, no fault tolerance

go to milestone 1 folder

in terminal run: `python3 test.py`

python version: 3.x

required packages: multiprocessing, glob, re, bisect, json

OS: Windows 10

Note:

1. if run on Linux/MacOS, modify the file path in udf.py from double slashes to "/" <br>
2. map_reduce_single.py: master, mapper, reducer are the same process which mentioned as minimum requirement according to Piazza. <br>
3. map_reduce_multi.py: master, mapper, reducer are three diffent processes. <br>
4. intermediate files will be saved at map folder. <br>
5. reduced files will be save at reduce folder. <br>

# milestone 2: one master, n mapper, n reducer, no fault tolerance

go to milestone 2 folder<br>
in terminal run: python3 test.py<br>
<br>
python version: 3.x <br>
required packages: multiprocessing, glob, re, os, json <br>
OS: Windows 10 <br>
Note: <br>

1. if run on Linux/MacOS, modify the file path in udf.py from double slashes to "/" <br>
2. each mapper will have one folder by mapper ID in the map folder, and the intermediate files will be saved in that folder.
3. reduced files will be save at reduce folder.

# milestone 3: one master, n mapper, n reducer, fault tolerance

go to milestone 3 folder<br>
in terminal run: python3 test.py<br>
<br>
python version: 3.x <br>
required packages: multiprocessing, glob, re, os, json, sys, time, random <br>
OS: Windows 10 <br>
Note: <br>

1. if run on Linux/MacOS, modify the file path in udf.py from double slashes to "/" <br>
2. each mapper will have one folder by mapper ID in the map folder, and the intermediate files will be saved in that folder.
3. reduced files will be save at reduce folder.
