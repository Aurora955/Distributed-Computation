#!/usr/bin/env python3
import sys
import re
def TF_to_10(item):#将 True 和 False 转化成 1 和 0
 num=1 if (item=="True" or item=="TRUE") else 0
 return num

def listed_date(date):#对 listed_data 这一变量的处理
 try:
 new=1 if date.split("-")[0]=="2020" else 0
 return new
 except:
 return 0

def find_num(s):#利用正则表达式提取数据
 try:
 return re.findall("\d*\.?\d+",s)[0]
 except:
 return 0

miss_list=[2,3,4,6,9,17,18,24,29,30,31,33,46,49,54,60]
char_list=[0,12,16,28,38,40,41,42,45,52,58,59]
dum_list=[5,7,13,15,20,23,37,53,56,61,62]
del_list=miss_list+char_list+dum_list#需要删除的变量对应的位置指标
del_list.sort()
num_list=[1,8,10,11,14,21,22,25,26,27,34,35,39,43,44,47,48,50,51,55,57,63,64,65]#需要提
取数字的变量指标
TF_list=[19,32]#布尔型变量指标
next(sys.stdin)

for line in sys.stdin:
     try:
         line=line.strip()
         st=re.findall(r'".*?"',line)
         for i in range(len(st)):
         line=line.replace(st[i],st[i].replace(',',';'))
         line=line.split(",")
         line=[re.sub('--','',lin) for lin in line]
         #print(line)
         if line.count("")>20:
             del line
         else:
         #print(len(line))
             for i in num_list:
             #print(line[i])
             line[i]=find_num(line[i])
         for i in TF_list:
             line[i]=TF_to_10(line[i])
         line[36]=listed_date(line[36])
         line[65]=float(line[65])-2000
         for i in reversed(del_list):
             del line[i]
         line=[0 if i=="" else i for i in line]#将最后仍然缺失的数据用 0 代替
         print(",".join([str(lin) for lin in line]))
     except:
         pass
