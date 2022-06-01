#!/usr/bin/python2.6
# -*- coding: utf-8 -*-
import csv
import pandas as pd
file=open("task2out.txt","r",encoding="utf-8")
file2=open("task2out.csv","w",encoding="utf_8_sig")
a = file.readlines()
head="Source,Target,Weight\n"
file2.write(head)
for line in a:
    line=line.replace("\t",",")
    file2.write(line)
file2.close()