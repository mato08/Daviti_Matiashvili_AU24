import os
from pathlib import Path
from random import seed, choice
from typing import List, Union
import requests
from requests.exceptions import RequestException
import re
from collections import Counter

S5_PATH = Path(os.path.realpath(__file__)).parent

PATH_TO_NAMES = S5_PATH / "names.txt"
PATH_TO_SURNAMES = S5_PATH / "last_names.txt"
PATH_TO_OUTPUT = S5_PATH / "sorted_names_and_surnames.txt"
PATH_TO_TEXT = S5_PATH / "random_text.txt"
PATH_TO_STOP_WORDS = S5_PATH / "stop_words.txt"

def task_1():
    seed(1)
    try:
        with open(PATH_TO_NAMES, 'r', encoding='utf-8') as names_file, \
                open(PATH_TO_SURNAMES, 'r', encoding='utf-8') as surnames_file:
            names = sorted(name.strip().lower() for name in names_file)
            surnames = [surname.strip().lower() for surname in surnames_file]
        with open(PATH_TO_OUTPUT, 'w', encoding='utf-8') as output_file:
            for name in names:
                surname = choice(surnames)
                output_file.write(f"{name} {surname}\n")
    except FileNotFoundError as e:
        print(f"Error: {e}")
    except UnicodeDecodeError as e:
        print(f"Encoding error: {e}")

def task_2(top_k: int):
    try:
        with open(PATH_TO_TEXT, 'r') as text_file, open(PATH_TO_STOP_WORDS, 'r') as stop_words_file:
            text = text_file.read().lower()
            stop_words = set(word.strip() for word in stop_words_file)
        words = re.findall(r'\b[a-z]+\b', text)
        filtered_words = [word for word in words if word not in stop_words]
        word_counts = Counter(filtered_words)
        return word_counts.most_common(top_k)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return []

def task_3(url: str):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        raise requests.exceptions.RequestException(f"Request failed for URL: {url}") from e

def task_4(data: List[Union[int, str, float]]):
    total = 0
    for item in data:
        try:
            total += float(item)
        except ValueError:
            raise TypeError(f"Cannot convert {item} to float")
    return total

def task_5():
    try:
        a, b = input("Enter two values separated by space: ").split()
        a, b = float(a), float(b)
        if b == 0:
            print("Can't divide by zero")
        else:
            print(a / b)
    except ValueError:
        print("Entered value is wrong")
