import json
import re
import time

from pyspark import SparkContext, SparkConf 
from pyspark.sql import SparkSession
from pyspark.sql.types import *

LIST_OF_BOOKS_v1 = []
LIST_OF_BOOKS_v1_FOUND = []

empty_book = {
    'name': [],
    'author': [],
    'country': [],
    'language': [],
    'series': [],
    'genre': [],
    'pub_date': [],
    'pages': []
}

book_found = False
created_record = False

sc = SparkContext("local[8]", "simple App")

spark = SparkSession.builder.master("local[8]").appName("simple App").getOrCreate()

'''
    Infobox book section
'''


def regex_plain(input):
    regex_output = re.findall(r'(?<=\= )(.*)(?=\\n)', input)
    if regex_output:
        return regex_output
    else:
        return []


def regex_name(input):
    if '&lt' in input:
        regex_output = re.findall(r'(?<=\= )(.*)(?=\&lt)', input)
        if regex_output:
            return regex_output
        else:
            return []
    else:
        return regex_plain(input)


def regex_pub(input):
    regex_output = re.findall(r'(\d{4})(.*?)', input)
    if regex_output:
        return regex_output[0][0]
    else:
        return []


def regex_pages(input):
    regex_output = re.findall(r'(\d{1,4})(.*?)', input)
    if regex_output:
        return regex_output[0][0]
    else:
        return []


def regex_other(input):
    if '[' in input:
        regex_output = re.findall(r'(?<=\[\[)(.*?)(?=[\||\]])', input)
        if regex_output:
            return regex_output
        else:
            return []
    else:
        return regex_plain(input)


'''
    End of Infobox book section
'''

def create_record():
    LIST_OF_BOOKS_v1.append({
        'name': [],
        'author': [],
        'country': [],
        'language': [],
        'series': [],
        'genre': [],
        'pub_date': [],
        'pages': []
    })

def append_item_detail(line):
    if '| name' in line:
        item = regex_name(line)
        if item:
            LIST_OF_BOOKS_v1[-1]['name'].append(item)
    elif '| author' in line:
        item = regex_other(str(line))
        if item:
            LIST_OF_BOOKS_v1[-1]['author'].append(item)
    elif '| country' in line:
        item = regex_plain(str(line))
        if item:
            LIST_OF_BOOKS_v1[-1]['country'].append(item)
    elif '| language' in line:
        item = regex_plain(str(line))
        if item:
            LIST_OF_BOOKS_v1[-1]['language'].append(item)
    elif '| series' in line:
        item = regex_other(str(line))
        if item:
            LIST_OF_BOOKS_v1[-1]['series'].append(item)
    elif '| genre' in line:
        item = regex_other(str(line))
        if item:
            LIST_OF_BOOKS_v1[-1]['genre'].append(item)
    elif '| pages' in line:
        item = regex_pages(str(line))
        if item:
            LIST_OF_BOOKS_v1[-1]['pages'].append(item)
    elif '| pub_date' in line or '| published' in line or '| release_date' in line or '| publish' in line:
        item = regex_pub(str(line))
        if item:
            if not len(LIST_OF_BOOKS_v1[-1]['pub_date']) == 4:
                LIST_OF_BOOKS_v1[-1]['pub_date'].append(item)

def parsing(line):
    global book_found, created_record, LIST_OF_BOOKS_v1, empty_book
    if line and "Infobox book" in str(line):
        book_found = True
        create_record()
        created_record = True
    elif "}}" in str(line) and book_found:
        book_found = False
        last_book = LIST_OF_BOOKS_v1[-1]
        if last_book != empty_book:
            return last_book
    elif book_found and created_record and line:
        append_item_detail(str(line))

def wtf(line):
    return "Ahoj"

if __name__ == '__main__':
    start = time.time()

    f = sc.textFile("/enwiki-latest-pages-articles-multistream1.xml-p1p41242")
    df = spark.createDataFrame(f, StringType()).toDF("text")

    rdd2 = df.rdd.map(lambda line: parsing(line)).filter(lambda line: line != None)
    rdd2.saveAsTextFile("./books_v7")

    end = time.time()
    print(end - start)

    """LIST_OF_BOOKS_v1 = []
    start = time.time()
    test = WikiParserRegex_v1()
    test.open_bz_file(WIKI_FILE_PATH_2)
    end = time.time()
    print(end - start)"""