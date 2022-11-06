import bz2
import json

from datetime import datetime

WIKI_FILE_PATH_1 = "src/enwiki-latest-pages-articles-multistream1.xml-p1p41242.bz2"
WIKI_FILE_PATH_2 = "src/enwiki-latest-pages-articles-multistream10.xml-p4045403p5399366.bz2"
START_TIME = ""
LIST_OF_BOOKS = []
WIKI_FILE = open("wiki_file_test.txt", "a")

"""
{{Infobox book
| name = Harry Potter and the Philosopher's Stone <!-- The first edition was in the UK and was the Philosopher's Stone, NOT the Sorcerer's Stone. Read the second paragraph and do not change this! -->
| image          = Harry Potter and the Philosopher's Stone Book Cover.jpg
| caption        = Cover for one of the earliest UK editions
| author         = [[J. K. Rowling]]
| country        = United Kingdom
| language       = English
| illustrator    = [[Thomas Taylor (artist)|Thomas Taylor]] (first edition) 
| series         = ''[[Harry Potter]]''
| release_number = {{ordinal|1}} in series
| genre          = [[Fantasy novel|Fantasy]]
| publisher      = [[Bloomsbury Publishing|Bloomsbury]] (UK) 
| pub_date       = 26 June 1997
| pages          = 223 (first edition)
| isbn           = 0-7475-3269-9
| preceded_by    =
| followed_by    = [[Harry Potter and the Chamber of Secrets]]
}}
"""

class WikiParser:
    def __init__(self):
        print("Parsing started")
        START_TIME = datetime.now()

    def create_record(self):
        LIST_OF_BOOKS.append({
            'name': '',
            'author': '',
            'country': '',
            'language': '',
            'series': '',
            'genre': '',
            'publisher': '',
            'pub_date': '',
            'pages': ''
        })
        
    def get_value(self, line):
        if "(" in line:
            output = line[line.find("=") + 2:line.find("(") - 1]
        elif "<" in line:
            output = line[line.find("=") + 2:line.find("<") - 1]
        else:
            output = line[line.find("=") + 2:line.find("\n") - 2]
        return output.replace("'", "").replace("[", "").replace("]", "")

    def get_value_publish(self, line):
        counter = 0
        output_year = ""
        output_pub = ""
        if counter < 2:
            if counter == 0:
                counter = counter + 1
                if "(" in line:
                    output_year = line[line.find("=") + 2:line.find("(") - 1]
                elif "<" in line:
                    output_year = line[line.find("=") + 2:line.find("<") - 1]
                else:
                    output_year = line[line.find("=") + 2:line.find("\n") - 2]
            if counter == 1:
                if "[[" in line:
                    output_pub = line[line.find("[[") + 2:line.find("]]") - 1]
                else:
                    output_pub = -1
        return output_year, output_pub

    def append_item_detail(self, line):
        if 'name' in line:
            item = self.get_value(line)
            LIST_OF_BOOKS[-1]['name'] = item
        elif 'author' in line:
            item = self.get_value(line)
            LIST_OF_BOOKS[-1]['author'] = item
        elif 'country' in line:
            item = self.get_value(line)
            LIST_OF_BOOKS[-1]['country'] = item
        elif 'language' in line:
            item = self.get_value(line)
            LIST_OF_BOOKS[-1]['language'] = item
        elif 'series' in line:
            item = self.get_value(line)
            LIST_OF_BOOKS[-1]['series'] = item
        elif 'genre' in line:
            item = self.get_value(line)
            LIST_OF_BOOKS[-1]['genre'] = item
        elif 'pages' in line:
            item = self.get_value(line)
            LIST_OF_BOOKS[-1]['pages'] = item.replace(" ", "")
        elif 'publish' in line:
            item_year, item_pub = self.get_value_publish(line)
            if item_pub == -1:
                LIST_OF_BOOKS[-1]['pub_date'] = item_year[-5:].replace(".", "").replace(" ", "")
            else:
                LIST_OF_BOOKS[-1]['pub_date'] = item_year[-5:].replace(".", "").replace(" ", "")
                LIST_OF_BOOKS[-1]['publisher'] = item_pub
        elif 'pub_date' in line or 'published' in line or 'release_date' in line:
            item = self.get_value(line)
            LIST_OF_BOOKS[-1]['pub_date'] = item[-5:].replace(".", "")

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
                    WIKI_FILE.write("\n" + str(line) + "\n")
                elif "}}" in str(line) and book_found:
                    book_found = False
                    WIKI_FILE.write(str(line) + "\n")
                    # uncomment this if you want only 2 items from wiki as an example
                    if counter == 1:
                        break
                    counter = counter + 1
                elif book_found and created_record and line:
                    #print(line)
                    self.append_item_detail(str(line))
                    WIKI_FILE.write(str(line) + "\n")


if __name__ == '__main__':
    test = WikiParser()
    test.open_bz_file(WIKI_FILE_PATH_1)
    print(json.dumps(LIST_OF_BOOKS, indent=3))
