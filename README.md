# milestone 1: one mapper, one reducer, one master, no fault tolerance

go to milestone 1 folder

in terminal run: `python3 test.py`

python version: 3.x

required packages: multiprocessing, glob, re, bisect, json

OS: Windows 10

Note:

1. if run on Linux/MacOS, modify the file path in udf.py from double slashes to "/" <br>
2. map_reduce_single.py: master, mapper, reducer are the same process <br>
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
