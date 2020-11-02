#!/usr/bin/env python3
import sys
import re
for line in sys.stdin:
 line=line.strip()
 st=re.findall(r'".*?"',line)
 for i in range(len(st)):
 line=line.replace(st[i],st[i].replace(',',';'))#将同一个变量内的逗号替换成分号，
便于后续对数据的正确分割
 line=line.split(",")
 miss=[1 if i=="" else 0 for i in line]
print(miss)
