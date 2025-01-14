# from collections import defaultdict as dd
# from itertools import product
from typing import Any, Dict, List, Tuple


def task_1(data_1: Dict[str, int], data_2: Dict[str, int]):
    for a,b in data_1.items():
        if a in data_2.keys():
            data_2[a]+=data_1[a]
        else:
            data_2[a]=data_1[a]
    return data_2

def task_2():
    dict1={}
    for i in range(1,16):
        dict1[i]=i**2
    return  dict1




def task_3(data: Dict[Any, List[str]]):
    result = data[list(data.keys())[0]]

    for key in list(data.keys())[1:]:
        result = [prefix + letter for prefix in result for letter in data[key]]

    return result


def task_4(data: Dict[str, int]):
    return sorted(data, key=data.get, reverse=True)[:3]



def task_5(data: List[Tuple[Any, Any]]) -> Dict[str, List[int]]:
        dict1={}
        for a,b in data:
            if a not in dict1:
                dict1[a]=[b]
            else:
                dict1[a].append(b)
        return dict1

def task_6(data: List[Any]):
    list_2=[]
    for i in data:
       if i not in list_2:
        list_2.append(i)
    return list_2


def task_7(words: [List[str]]) -> str:
    string=""
    for i in range(len(words[0])):
      for j in range(1,len(words)):
        if i<len(words[j]):
            if words[0][i]!=words[j][i]:
                return string
        else:
            return string
      string += words[0][i]
    return string


def task_8(haystack: str, needle: str) -> int:
    if needle=="":
        return 0
    n=len(haystack)
    m=len(needle)
    for i in range(n-m+1):
        match=True
        for j in range(m):
            if haystack[i+j]!=needle[j]:
                match=False
                break

        if match:
           return i
    return -1

