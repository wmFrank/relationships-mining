file=open("task6_1.txt","r",encoding="utf-8")
file2=open("task4out.csv","w",encoding="utf_8_sig")
a = file.readlines()
head="Weight,Id\n"
file2.write(head)
for line in a:
    line=line.replace("\t",",")
    file2.write(line)
file2.close()