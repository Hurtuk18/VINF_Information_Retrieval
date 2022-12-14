import json
import re
import time

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# General settings for using PySpark
sc = SparkContext("local[*]", "WikiParser")
spark = SparkSession.builder.master("local[*]").appName("WikiParser").getOrCreate()

'''
    RegEx section
'''


# Find Infobox book from input
def regex_infobox(input: str) -> list:
    regex = r'(?<=Infobox book|infobox book)(.*?)(?=\\n\}{2}\\{1,2}n)'
    regex_output = re.findall(regex, input)
    if regex_output:
        return regex_output
    else:
        return []


# Regex for finding our category based on @param in raw form... whole information for the parameter from infobox
def regex_raw_attribute(input: str, param: str):
    input = input.replace('"', "").replace("'", "").replace(",", "")
    regex = f"(\|)(\s*)({param})(\s*)(=)(\s*)(.*?)(?=\\\{{1,2}}n)"
    regex_output = re.findall(regex, input)
    if regex_output:
        final_output = regex_output[0][6]
        return final_output
    else:
        return ""


# Regex for getting whole information for the parameter from input
def str_to_list(input: str) -> list:
    regex = r"(.*)"
    regex_output = re.findall(regex, input)
    if regex_output:
        return regex_output
    else:
        return []


# Regex for getting information about the name of book from input
def regex_name(input: str) -> list:
    if '<!--' in input:
        regex = r"(.*?)(?=\s{0,}<!--)"
        regex_output = re.findall(regex, input)
        if regex_output:
            return regex_output
        else:
            return []
    else:
        return str_to_list(input)


# Regex for getting information / information's between [] from input
def regex_other(input: str) -> list:
    if '[' in input:
        regex_output = re.findall(r'(?<=\[\[)(.*?)(?=[\||\]])', input)
        if regex_output:
            return regex_output
        else:
            return []
    else:
        return str_to_list(input)


# Regex for getting information about number of pages from input
def regex_pages(input: str) -> list:
    regex_output = re.findall(r'(\d{1,4})(.*?)', input)
    if regex_output:
        return regex_output[0][0]
    else:
        return []


# Regex for getting information about the year of publication of pages from input
def regex_pub(input: str) -> list:
    regex_output = re.findall(r'(\d{4})(.*?)', input)
    if regex_output:
        return regex_output[0][0]
    else:
        return []


'''
    End of RegEx section
'''

# Get infobox from input
def get_infobox(line):
    infobox = regex_infobox(str(line))
    if infobox:
        return infobox


# Delete non-correct characters from input
def contains_wrong_chars(item_list):
    if item_list:
        if "" in item_list:
            item_list.remove("")
        for index, item in enumerate(item_list):
            if item and item[-1] == " ":
                item_list[index] = item[:-1]
            if item[:2] == "{{":
                item_list[index] = item[2:]
            if not item:
                item_list.remove(item)

        patterns = [r"\\'", r"'''", r"''", r"or<br/>", r"<br/>", r"<br>", r"<br />"]
        patterns_result = ["'", "", "", "", "", "", ""]
        patt_split = [r"\|", r"\}"]

        for ix, pat in enumerate(patterns):
            for index, item in enumerate(item_list):
                item_list[index] = re.sub(pat, patterns_result[ix], item)
        for ix, pat in enumerate(patt_split):
            for index, item in enumerate(item_list):
                patt_res = re.split(pat, item)
                item_list[index] = patt_res[0]

    return item_list


# Getting information about our categories for books using regex functions for more precise information
def prepare_values(name, author, country, language, series, genre,
                   pages, pub_date_1, pub_date_2, pub_date_3, pub_date_4):
    name = regex_name(name)
    author = regex_other(author)
    country = regex_other(country)
    language = regex_other(language)
    series = regex_other(series)
    genre = regex_other(genre)
    pages = regex_pages(pages)
    pub_date_1 = regex_pub(pub_date_1)
    pub_date_2 = regex_pub(pub_date_2)
    pub_date_3 = regex_pub(pub_date_3)
    pub_date_4 = regex_pub(pub_date_4)

    pub_date = ""

    # SET pub_date
    if not len(str(pub_date)) == 4:
        pub_date = pub_date_1
    if not len(str(pub_date)) == 4:
        pub_date = pub_date_2
    if not len(str(pub_date)) == 4:
        pub_date = pub_date_3
    if not len(str(pub_date)) == 4:
        pub_date = pub_date_4

    # Delete non-correct characters from input
    name = contains_wrong_chars(name)
    author = contains_wrong_chars(author)
    country = contains_wrong_chars(country)
    language = contains_wrong_chars(language)
    series = contains_wrong_chars(series)
    genre = contains_wrong_chars(genre)

    return name, author, country, language, series, genre, pages, pub_date


# Start parsing out input (input is represented by one row in file - using PySpark lambda)
def parsing(line):
    # GET Infobox book
    infobox = get_infobox(str(line))
    if infobox:
        name = regex_raw_attribute(str(line), "name")
        author = regex_raw_attribute(str(line), "author")
        country = regex_raw_attribute(str(line), "country")
        language = regex_raw_attribute(str(line), "language")
        series = regex_raw_attribute(str(line), "series")
        genre = regex_raw_attribute(str(line), "genre")
        pages = regex_raw_attribute(str(line), "pages")

        # The information about the year of publication can be in these parts: pub_date / published / release_date / publish
        pub_date = None
        pub_date_1 = regex_raw_attribute(str(line), "pub_date")
        pub_date_2 = regex_raw_attribute(str(line), "published")
        pub_date_3 = regex_raw_attribute(str(line), "release_date")
        pub_date_4 = regex_raw_attribute(str(line), "publish")

        # Analysing infobox
        name, author, country, language, series, genre, pages, pub_date = prepare_values(name, author, country,
                                                                                         language, series, genre,
                                                                                         pages, pub_date_1, pub_date_2,
                                                                                         pub_date_3, pub_date_4)
        book_test = {
            'name': name,
            'author': author,
            'country': country,
            'language': language,
            'series': series,
            'genre': genre,
            'pub_date': pub_date,
            'pages': pages
        }

        return book_test


if __name__ == '__main__':
    start = time.time()

    # Create XML schema
    xmlSchema = StructType([ \
        StructField('revision', StructType([
            StructField('text', StringType(), True),
        ]))
    ])

    # Load the file from /src folder... format of file can be .xml.bz2 or .xml
    df = spark.read.format('xml') \
        .options(rowTag="page") \
        .load("src/enwiki-20220920-pages-meta-current.xml.bz2", schema=xmlSchema)

    # Read the file by rows
    wiki_rdd = df.rdd.map(lambda line: parsing(line)).filter(lambda line: line != None)
    wiki_rdd.saveAsTextFile("./final_books")

    # Print the time which we needed for parsing
    print(time.time() - start)
