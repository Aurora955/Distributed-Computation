#lm_reducer
#! /usr/bin/env python3
import sys
import numpy as np
m=0
for line in sys.stdin:
    line=line.strip()
    line=line.split(',')
    line=np.array(line,float)
    if m==0:
        m=line.shape[1]
        s=np.zeros(m)
    s+=line
p=26
xy = np.matrix(s[:p]).T
xx = np.matrix(s[p:].reshape(p,p)).I
beta = xx*xy
print(beta)
