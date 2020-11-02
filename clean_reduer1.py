#!/usr/bin/env python3
import sys
summ=[0]*66
for line in sys.stdin:
    line=line.strip()
    line=line.strip("[")
    line=line.strip("]")
    line=line.split(",")
    for i in range(len(summ)):
        summ[i]+=int(line[i])

for key,val in enumerate(summ):
    print(key,val)
