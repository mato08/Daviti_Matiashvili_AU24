from typing import List


def task_1(array: List[int], target: int) -> List[int]:
    visited = set()
    for num in array:
        complement = target - num
        if complement in visited:
            return [complement, num]
        visited.add(num)
    return []

def task_2(number: int) -> int:
    check_negativity=number<0
    number=abs(number)
    result=0

    while number>0:
        reminder=number%10
        result= result*10 +reminder
        number//=10

    if check_negativity:
        result=-result

    return result


def task_3(array: List[int]) -> int:
    setunia=set()
    for i in array:
        if i not in setunia:
            setunia.add(i)
        else:
            return i
    return -1



def task_4(string: str) -> int:
    dict = {
        'I': 1,
        'V': 5,
        'X': 10,
        'L': 50,
        'C': 100,
        'D': 500,
        'M': 1000
    }

    total = 0
    n = len(string)

    for i in range(n):
        if i < n - 1 and dict[string[i]] < dict[string[i + 1]]:
            total -= dict[string[i]]
        else:
            total += dict[string[i]]
    return total


def task_5(array: List[int]) -> int:
    lowest=array[0]
    for number in array:
       if number<lowest:
            lowest=number
    return lowest







