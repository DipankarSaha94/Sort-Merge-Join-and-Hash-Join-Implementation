import math
import sys
import os
import time
from io import open
from os import makedirs, path
from sys import argv, exit


def build_hash_table(from_file,index,B):
    hash_table = {}
    with open(from_file.name) as f:
        for line in f:
            temp = line.strip().split(" ")
            key = h1(temp[index],B)
            value = temp
            if key in hash_table:
                hash_table[key].append(value)
            else:
                hash_table[key] = []
                hash_table[key].append(value)
    return hash_table

def h1(hstr,B):
  h = 5381
  for i in hstr:
    h = (h*33 + ord(i)) % B
  return h

def init_buckets(name,B):
    if not path.exists('./tmp'):
        makedirs('./tmp')
    return [open('./tmp/{}_{}.txt'.format(name, i), 'w')
            for i in range(B)]

def hash_join_get_next(r, s,B, out_file):
    f = open(out_file, 'w')
    for bucket in range(B):
        hash_table = build_hash_table(r[bucket],1,B)
        part = join(hash_table, s[bucket],0,B)
        r[bucket].close()
        s[bucket].close()
        Rkey, Skey = 0,0
        for a, b in hash_table.items():
          Rkey, Rlist = a, b
        for a, b in part.items():
          Skey, Slist = a, b
        if ( Rkey == Skey and len(Rlist) > 0 and len(Slist) >0):
          for i in Rlist:
            for j in Slist:
              if i[1] == j[0]:
                f.write(i[0]+' '+i[1]+' '+j[1]+'\n')
        del hash_table
        del part
    f.close()

def join(hash_table, file,index,B):
    results = {}
    result = []
    k = 0
    with open(file.name) as f:
        for line in f:
            temp = line.strip().split(" ")
            key = h1(temp[index],B)
            value = temp
            if key in hash_table:
                results[key] = hash_table[key]
                k = key
                result.append(value)
    results[k] = result
    return results

def partition(src_file, to,idx,B):
    c = 0
    with open(src_file) as f:
        for line in f:
            temp = line.strip().split(" ")
            bucket = h1(temp[idx],B)
            to[bucket].write(temp[0]+' '+temp[1]+'\n')
            c += 1
    [to[x].close() for x in range(B)]
    return c

def closeH(B):
  for i in range(B):
    os.remove('./tmp/{}_{}.txt'.format('r', i))
  for i in range(B):
    os.remove('./tmp/{}_{}.txt'.format('s', i))
  os.rmdir('./tmp')


#split R into sorted sublist 
def split_sort_list_R(filename,B):
  lines = []
  with open(filename) as file_in:
    for line in file_in:
        temp = line.strip().split(" ")
        lines.append(temp)
  Block_Size_R = B
  #print(Block_Size_R)
  #print(lines)
  lines = sortlines(lines,1)
  #print(lines)
  filearrayR= split_to_file(lines,'R',Block_Size_R,1)
  return filearrayR

#split S into sorted sublist 
def split_sort_list_S(filename,B):
  lines = []
  with open(filename) as file_in:
    for line in file_in:
        temp = line.strip().split(" ")
        lines.append(temp)
  Block_Size_S = B
  #print(Block_Size_S)
  #print(lines)
  lines = sortlines(lines,0)
  #print(lines)
  filearrayS= split_to_file(lines,'S',Block_Size_S,0)
  return filearrayS

#sort the file based on the join key 
def sortlines(lines,col):
  return sorted(lines,key=lambda x: x[col])

#function for split into sublist
def split_to_file(lines,f,b_size,idx):
  c = 0
  filearray = []
  findex = 0
  tempresult = []
  #t = open('tempR'+str(findex)+'.txt')
  i = 0
  while i<len(lines):
    if (c == b_size):
      j = i
      while(j<len(lines)):
        if(lines[j][idx] == lines[j-1][idx]):
          tempresult.append(lines[j])
          j += 1
        else:
          break
      i = j
      if i >= len(lines):
        break
      filearray.append('temp'+f+str(findex)+'.txt')
      t = filearray[findex]
      f2 = open(t,'w')
      #print(i,c,f,len(tempresult))
      writelistoflist(f2,tempresult)
      tempresult=[]
      c = 0
      findex += 1
      f2.close()
    tempresult.append(lines[i])
    c += 1
    i += 1

  #print(i,c,f,len(tempresult))
  if c>0:
    filearray.append('temp'+f+str(findex)+'.txt')
    f2=open(filearray[findex],'w')
    writelistoflist(f2,tempresult)
    f2.close()
  
  return filearray

#write into temp file
def writelistoflist(f,r):
  for i in r:
    t = ""
    t = i[0]+' '+i[1]
    t += '\n'
    f.write(t)

def get_list_of_list(fname):
  lines = []
  with open(fname) as file_in:
    for line in file_in:
        temp = line.strip().split(" ")
        lines.append(temp)
  return lines

# Merge join both the relation based on the join key
def merge_join_get_next(R,S,out_file_name):
  tempR = get_list_of_list(R[0])
  tempS = get_list_of_list(S[0])
  ridx, sidx = 1,1
  i,j = 0,0
  mask = -1
  f2 = open(out_file_name,'w')
  while (True):
    if mask == -1:
      while tempR[i][1] < tempS[j][0]:
        if i+1 == len(tempR):
          if ridx < len(R):
            tempR = get_list_of_list(R[ridx])
            ridx += 1
            i = 0
          else:
            return
        else:
          i += 1 
      while tempR[i][1] > tempS[j][0]:
        if j+1 == len(tempS):
          if sidx < len(S):
            tempS = get_list_of_list(S[sidx])
            sidx += 1
            j = 0
          else:
            return
        else:
          j += 1
      mask = j
    if (tempR[i][1] == tempS[j][0]):
      f2.write(tempR[i][0]+' '+tempR[i][1]+' '+tempS[j][1]+'\n')
      if j+1 == len(tempS):
        j = mask
        if (i+1 >=len(tempR)):
          if(ridx >= len(R)):
            return 
          else:
            tempR = get_list_of_list(R[ridx])
            ridx += 1
            i =0
            mask = -1
        else:
          i += 1
      else:
        j += 1
    elif (tempR[i][1] < tempS[j][0]):
      j = mask
      if (i+1 >=len(tempR)):
        if (ridx >= len(R)):
            return 
        else:
          tempR = get_list_of_list(R[ridx])
          ridx += 1
          i =0
          mask = -1
      else:
          i += 1
    else:
      mask = -1
  
# Removing the temporary files created in between
def closeS(R,S):
  for i in R:
    os.remove(i)
  for i in S:
    os.remove(i)


def openS(fn,Main_Memory_Size,i):
  if i == 1:
    Relation = split_sort_list_R(fn,Main_Memory_Size)
  else:
    Relation = split_sort_list_S(fn,Main_Memory_Size)
  return Relation

def openH(Rfn,Sfn,M):
  r = init_buckets('r',M)
  s = init_buckets('s',M)
  rcount = partition(Rfn, r,1,M)
  scount = partition(Sfn, s,0,M)
  return r,s, rcount , scount


def main():
    start = time.process_time()
    Rfn = sys.argv[1]  # filename of R
    Sfn = sys.argv[2]  # filename of S
    join_type = sys.argv[3] # Merge or Hash join
    M = int(sys.argv[4]) # No fo Blocks
    Main_Memory_Size = M*100  # Main memory size
    out_file = os.path.basename(Rfn)+'_' +os.path.basename(Sfn)+'_join.txt'
    if join_type == 'sort':
      R = openS(Rfn,Main_Memory_Size,1)
      if len(R) >= M:
        print('total sublist in R is more than or equal to total No of Blocks(M) in Main Memory')
        closeS(R,[])
      else:
        S = openS(Sfn,Main_Memory_Size,0)
        if len(S) >= M:
          print('total sublist in S is more than or equal to total No of Blocks(M) in Main Memory')
          closeS(R,S)
        else:
          merge_join_get_next(R,S,out_file)
          closeS(R,S)
    elif join_type == 'hash':
      r,s, rc, sc = openH(Rfn,Sfn,M)
      if int(rc/M) > (M-1)*100:
      	 print('total tuple in relation R for a single hash value exceeds (M-1)*100 limit ')
      elif int(sc/M) > (M-1)*100:
      	 print('total tuple in relation S for a single hash value exceeds (M-1)*100 limit ')
      else:
      	 hash_join_get_next(r, s,M,out_file)
      closeH(M)
    else:
      print('Unknown join type, Join type can be either sort or merge. ')
    print(time.process_time()-start)

if __name__ == '__main__':
    main()
