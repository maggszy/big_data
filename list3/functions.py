import numpy as np
import scipy.stats as sp
import requests
import re
from mpmath import mp



# Exercise 1

def mapper(x):
    return 0 <= x <= 100

def chunks_mapper(chunk):
    mapped_chunk = map(mapper, chunk.flatten())
    return sum(mapped_chunk)



# Exercise 2

def download_book(number):
    url = "https://www.gutenberg.org/cache/epub/" + str(number) + "/pg" + str(number) + ".txt"
    response = requests.get(url)
    if response.status_code == 200:
        with open('books/book' + str(number) + '.txt', 'w', encoding='utf-8') as file:
            file.write(response.text)

def count_words(book):
    with open('books/' + book, 'r', encoding='utf-8') as file:
        text = file.read()
    words = re.sub(r'[^\w\s]', '', text).replace('\r', '').replace('\n', '').split()
    return len(words)

def sum_of_words(books):
    mapped = map(count_words, books)
    return sum(mapped)



# Exercise 3

def pi_element(k):
    k = mp.mpf(k)
    return (4 / (8*k + 1) - 2 / (8*k + 4) - 1 / (8*k + 5) - 1 / (8*k + 6)) / 16**k

def pi_sum(chunk):
    mapped = map(pi_element, chunk)
    return sum(mapped)



# Exercise 4

with open('A.npy', 'rb') as f:
    A = np.load(f)
with open('B.npy', 'rb') as f:
    B = np.load(f)

def row_col_prod(row_col):
    row, col = row_col
    return ((row_col), np.sum(A[row,:] * B[:,col]))

