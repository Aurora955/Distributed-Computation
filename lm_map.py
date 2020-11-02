#lm_map
#! /usr/bin/env python3
import sys
import numpy as np
xy_xx=[]
for line in sys.stdin
	line=line.split(',')
	line=np.array(line,float)
	x1=line[:19]
	x2=line[20:]
	x=np.concatenate((x1,x2))
	y=line[19]
	xy=x*y
	xx=np.matrix(x).T*np.matrix(x)
	xy_xx.append((xy,xx))

p=26#自变量的个数
xy=np.zeros(p)
xx=np.diag(np.zeros(p))

for item in xy_xx:
	xy1=item[0]
	xx1=item[1]
	xy+=xy1
	xx+=xx1

xy=list(xy)
xx=list(xx.reshape(1,p**2)[0])
data=xy+xx
print(",".join(str(i) for i in data))
