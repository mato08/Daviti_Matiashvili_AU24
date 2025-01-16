# import time
from typing import List

Matrix = List[List[int]]


def task_1(exp: int):
    def power_factory(base: int) -> int:
        return base ** exp
    return power_factory


def task_2(*args, **kwargs):
    for arg in args:
        print(arg)
    for value in kwargs.values():
        print(value)


def helper(func):
    def wrapper(*args, **kwargs):
        print("Hi, friend! What's your name?")
        func(*args, **kwargs)
        print("See you soon!")
    return wrapper


@helper
def task_3(name: str):
    print(f"Hello! My name is {name}.")


def timer(func):
    import time
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        run_time = time.time() - start_time
        print(f"Finished {func.__name__} in {run_time:.4f} secs")
        return result
    return wrapper


@timer
def task_4():
    return len([1 for _ in range(0, 10**8)])


def task_5(matrix: Matrix) -> Matrix:
    return [list(row) for row in zip(*matrix)]


def task_6(queue: str):
    stack = []
    for char in queue:
        if char == '(':
            stack.append(char)
        elif char == ')':
            if not stack or stack[-1] != '(':
                return False
            stack.pop()
    return len(stack) == 0
