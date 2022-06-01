# -*- coding: utf-8 -*-
file1=open("task5out.txt","r",encoding="utf-8")
file2=open("task5out.csv","w",encoding="utf_8_sig")
a=file1.readlines()
head="Id,Community\n"
file2.write(head)
for line in a:
    line_new=line.split(":")[0]
    line_new=line_new.replace("\t",",")
    line_new+="\n"
    file2.write(line_new)
file2.close()