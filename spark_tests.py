import unittest
import json

from spark_search import *
from spark_parser import *

# Test inputs for indexing in UNIT Tests
BOOK_TEST_1 = "{'name': ['The Shining']}"
BOOK_TEST_2 = "{'name': ['The Shining'], 'author': ['Stephen King'], 'country': ['United States'], 'language': ['English'], 'series': [], 'genre': ['Gothic novel', 'Horror fiction', 'Psychological horror'], 'pub_date': '1977', 'pages': '447'}"

# Test input for using regex in UNIT Tests
BOOK_REGEX_1 = "\\n{{Infobox book\\n<!-- |edition       = Classic Edition -->| name = Fahrenheit 451\\n| image = Fahrenheit 451 1st ed cover.jpg\\n| alt = This original cover shows a drawing of a man, who appears to be made of newspaper and is engulfed in flames, standing on top of some books. His right arm is down and holding what appears to be a fireman\'s hat made of paper while his left arm is as if wiping sweat from the brow of his bowed head. The title and author\'s name appear in large text over the images and there is a small caption in the upper left-hand corner that reads, \"Wonderful stories by the author of The Golden Apples of the Sun\".\\n| caption = First edition cover ([[clothbound]])\\n| author = [[Ray Bradbury]]\\n| illustrator = [[Joseph Mugnaini]]<ref name=crider>{{cite journal|last=Crider|first=Bill|author-link=Bill Crider|editor1-last=Laughlin|editor1-first=Charlotte|editor2-last=Lee|editor2-first=Billy C.|title=Ray Bradbury\'s FAHRENHEIT 451|journal=Paperback Quarterly|isbn=978-1-4344-0633-0|date=Fall 1980|volume=III|url=https://books.google.com/books?id=8FVzjEjyHH0C&q=%22The+first+paperback+edition+featured+illustrations+by+Joe+Mugnaini+and+contained+two+stories+in+addition+to+the+title+tale%3A+%27The+Playground%27+and+%27And+The+Rock+Cried+Out.%27%22&pg=PA22|page=22|quote=The first paperback edition featured illustrations by [[Joseph Mugnaini|Joe Mugnaini]] and contained two stories in addition to the title tale: \'The Playground\' and \'And The Rock Cried Out\'.|issue=3}}</ref>\\n| country = United States\\n| language = English\\n| genre = [[Dystopian literature|Dystopian]]<ref name=gerall>{{cite book|last1=Gerall|first1=Alina|last2=Hobby|first2=Blake|editor1-last=Bloom|editor1-first=Harold|editor1-link=Harold Bloom|editor2-last=Hobby|editor2-first=Blake|title=Civil Disobedience|year=2010|publisher=Infobase Publishing|isbn=978-1-60413-439-1|page=148|chapter-url=https://books.google.com/books?id=N5NDMxfkum8C&q=%22While+Fahrenheit+451+begins+as+a+dystopic+novel+about+a+totalitarian+government+that+bans+reading%22&pg=PA148|chapter=Fahrenheit 451|quote=While \'\'Fahrenheit 451\'\' begins as a dystopic novel about a totalitarian government that bans reading, the novel concludes with Montag relishing the book he has put to memory.}}</ref>\\n| published = October 19, 1953 ([[Ballantine Books]])<ref>{{cite journal |date=October 19, 1953 |title=Books Published Today |journal=[[The New York Times]] |page=19 }}</ref>\\n| isbn = 978-0-7432-4722-1\\n| isbn_note = (current cover edition)\\n| dewey = 813.54 22\\n| congress = PS3503.R167 F3 2003\\n| oclc = 53101079\\n| pages = 256\\n}}\\n\'\'\'\'\'Fahrenheit 451\'\'\'\'\' is a 1953 [[Utopian and dystopian fiction|dystopian]] [[novel]] by American writer [[Ray Bradbury]]. Often regarded as one of his best works,<ref "
REGEX_CORRECT_RESULT = {
            'name': ["Fahrenheit 451"],
            'author': ["Ray Bradbury"],
            'country': ["United States"],
            'language': ["English"],
            'series': [],
            'genre': ["Dystopian literature"],
            'pub_date': "1953",
            'pages': "256"
        }


# The test based on unittest module
class WikiParserTests(unittest.TestCase):
    # Creating index of for book name
    def test_book_name(self):
        test_line = json.loads(json_prepare(BOOK_TEST_1))
        create_book(index_book, test_line['name'], 0)
        self.assertEqual(index_book[0]['name'][0], "The Shining", "Book name not found... :(")

    # Creating every possible index from book
    def test_index_whole_book(self):
        test_line = json.loads(json_prepare(BOOK_TEST_2))
        magic(test_line, 0)
        indexes = [index_author, index_country, index_language, index_series, index_genre, index_pub_date, index_pages]
        categories = ['author', 'country', 'language', 'series', 'genre', 'pub_date', 'pages']
        category_names = ["Author", "Country", "Language", "Series", "Genre", "Publication date", "Pages"]

        # Compare every category of the book and see if they are the same
        for idx, ix_item in enumerate(indexes):
            if ix_item and ix_item != index_pub_date and ix_item != index_pages:
                self.assertEqual(ix_item[0]['content'], test_line[categories[idx]][0], category_names[idx] + " doesn't exist... :(")
            if ix_item and (ix_item == index_pub_date or ix_item == index_pages):
                self.assertEqual(ix_item[0]['content'], int(test_line[categories[idx]]), category_names[idx] + " doesn't exist... :(")

    # Using RegEx for get information about book from the input and then compare correct result with the result
    def test_regex(self):
        test_parsed_book = parsing(BOOK_REGEX_1)
        categories = ['name', 'author', 'country', 'language', 'series', 'genre', 'pub_date', 'pages']

        # Compare every category of the book and see if they are the same
        for item in categories:
            self.assertEqual(test_parsed_book[item], REGEX_CORRECT_RESULT[item], item + " is not equal... :(")
