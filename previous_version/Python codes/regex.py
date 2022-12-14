import json
import re
import bz2

import regex_dir.regex_expressions as re_exp

WIKI_FILE_PATH_1 = "../../src/enwiki-latest-pages-articles-multistream1.xml-p1p41242.bz2"
WIKI_FILE_PATH_2 = "../../src/enwiki-latest-pages-articles-multistream10.xml-p4045403p5399366.bz2"
START_TIME = ""
LIST_OF_BOOKS = []

"""
phrase= "The hotdog was delicious."

for i in re.finditer("hot", phrase):
        indexlocation= i.span()
        print(indexlocation)
        startindex= i.start()
        endindex= i.end()
        print(startindex)
        print(endindex)
	    wholematch= phrase[indexlocation[0]:indexlocation[1]] 
        print(wholematch)
"""


class WikiParserRegex:

    def __init__(self):
        print("Parsing started")

    def create_record(self):
        LIST_OF_BOOKS.append({
            'name': [],
            'author': [],
            'country': [],
            'language': [],
            'series': [],
            'genre': [],
            'pub_date': [],
            'pages': []
        })

    def append_item_detail(self, line):
        if 'name' in line:
            item = re_exp.regex_name(str(line))
            if item:
                LIST_OF_BOOKS[-1]['name'].append(item)
        elif 'author' in line:
            item = re_exp.regex_other(str(line))
            if item:
                LIST_OF_BOOKS[-1]['author'].append(item)
        elif 'country' in line:
            item = re_exp.regex_plain(str(line))
            if item:
                LIST_OF_BOOKS[-1]['country'].append(item)
        elif 'language' in line:
            item = re_exp.regex_plain(str(line))
            if item:
                LIST_OF_BOOKS[-1]['language'].append(item)
        elif 'series' in line:
            item = re_exp.regex_other(str(line))
            if item:
                LIST_OF_BOOKS[-1]['series'].append(item)
        elif 'genre' in line:
            item = re_exp.regex_other(str(line))
            if item:
                LIST_OF_BOOKS[-1]['genre'].append(item)
        elif 'pages' in line:
            item = re_exp.regex_pages(str(line))
            if item:
                LIST_OF_BOOKS[-1]['pages'].append(item)
        elif 'pub_date' in line or 'published' in line or 'release_date' in line or 'publish' in line:
            item = re_exp.regex_pub(str(line))
            if item:
                if not len(LIST_OF_BOOKS[-1]['pub_date']) == 4:
                    LIST_OF_BOOKS[-1]['pub_date'].append(item)

    def open_bz_file(self, path_to_file):
        counter = 0
        with bz2.BZ2File(path_to_file, "r") as xml_file:
            book_found = False
            created_record = False
            for idx, line in enumerate(xml_file):
                if line and "Infobox book" in str(line):
                    book_found = True
                    self.create_record()
                    created_record = True
                elif "}}" in str(line) and book_found:
                    book_found = False
                    # uncomment this if you want only 2 items from wiki as an example
                    if counter == 5:
                        break
                    counter = counter + 1
                elif book_found and created_record and line:
                    #print(line)
                    self.append_item_detail(str(line))


if __name__ == '__main__':
    test = WikiParserRegex()
    test.open_bz_file(WIKI_FILE_PATH_1)
    #print(json.dumps(LIST_OF_BOOKS, indent=3))
    for book in LIST_OF_BOOKS:
        if 'genre' in book:
            for item in book['genre']:
                for name in item:
                    if "Science".lower() in name.lower():
                        print("got ya")
                    else:
                        print("fail")
