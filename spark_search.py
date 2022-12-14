import json
import os
import unittest

from spark_tests import *

"""
    Index lists part
"""

index_book = []
index_author = []
index_country = []
index_language = []
index_series = []
index_genre = []
index_pub_date = []
index_pages = []

"""
    Index lists part
"""


# Create index for names and set them their id
def create_book(index_name, book_name, book_id):
    global index_book
    index_name.append({
        'id': book_id,
        'name': book_name
    })


# Find existing item in the index
def find_index(index_name, index_item):
    global index_author, index_country, index_language, index_series, index_genre
    ix_exists = False
    ix_id = -1
    last_id = 0

    # Find if the index_item exists
    for index, item in enumerate(index_name):
        last_id = index
        if index_item == item['content']:
            ix_exists = True
            ix_id = index
            break

    # If index is empty
    if ix_id == -1 and len(index_name) == 0:
        return ix_exists, last_id
    # If index is not empty but item is not in the index yet
    elif ix_id == -1 and len(index_name) != 0:
        return ix_exists, last_id + 1
    # If item exists return its id
    else:
        return ix_exists, ix_id


# Append item to index - Only for strings
def index_parent(index_name, index_item, book_id):
    global index_author, index_country, index_language, index_series, index_genre

    ix_exists = False
    ix_id = None

    for item in index_item:
        ix_exists, ix_id = find_index(index_name, item)
        # If index_item is not in index, create the new record and set this book as related to this index_item
        if not ix_exists:
            index_name.append({
                'content': item,
                'items': []
            })
            index_name[ix_id]['items'].append(book_id)
        # If index_item is in index, set this book as related to this index_item
        else:
            index_name[ix_id]['items'].append(book_id)


# Append item to index - Only for numbers (Int)
def index_number_parent(index_name, index_item, book_id):
    global index_pub_date, index_pages

    ix_exists = False
    ix_id = None

    ix_exists, ix_id = find_index(index_name, int(index_item))

    # If index_item is not in index, create the new record and set this book as related to this index_item
    if not ix_exists:
        index_name.append({
            'content': int(index_item),
            'items': []
        })
        index_name[ix_id]['items'].append(book_id)
    # If index_item is in index, set this book as related to this index_item
    else:
        index_name[ix_id]['items'].append(book_id)


# This method start indexing for our book... so there the magic is happening :)
def magic(book, book_id):
    global index_author, index_country, index_language, index_series, index_genre, index_pub_date, index_pages
    if book['author']:
        index_parent(index_author, book['author'], book_id)
    if book['country']:
        index_parent(index_country, book['country'], book_id)
    if book['language']:
        index_parent(index_language, book['language'], book_id)
    if book['series']:
        index_parent(index_series, book['series'], book_id)
    if book['genre']:
        index_parent(index_genre, book['genre'], book_id)
    if book['pub_date']:
        index_number_parent(index_pub_date, book['pub_date'], book_id)
    if book['pages']:
        index_number_parent(index_pages, book['pages'], book_id)


# Working with input... There we are working with input for the searching... Only for strings
def text_evaluator(cnsl_input: str, index_name):
    global index_author, index_country, index_language, index_series, index_genre

    local_bool = False
    local_result = []

    # If input is not None... If input is none, we set it as '-' to the input
    if cnsl_input != '-':
        cnsl_input = cnsl_input.replace('"', "").replace("'", "").replace(",", "")
        # Going through every item in index
        for item_part in index_name:
            # Splitting input
            if item_part['content'].lower() != " " and item_part['content'].lower() != "":
                for part_find in cnsl_input.split(', '):
                    if str(part_find).lower() == str(item_part['content']).lower():
                        local_bool = True
                        # Going through every book for an item in index
                        for book_id in item_part['items']:
                            # Append only if book is not in the list (preventions of the duplicates)
                            if book_id not in local_result:
                                local_result.append(book_id)
    else:
        local_bool = True
    return local_bool, local_result


# Working with input... There we are working with input for the searching... Only for numbers (int)
def number_evaluator(cnsl_input: str, index_name):
    global index_pub_date, index_pages

    local_bool = False
    local_result = []
    if cnsl_input != '-':
        cnsl_input = cnsl_input.replace('"', "").replace("'", "").replace(",", "")
        # Going through every item in index
        for item_part in index_name:
            # Splitting input
            if item_part['content'] != " " and item_part['content'] != "":
                for part_find in cnsl_input.split(', '):
                    if int(item_part['content']) <= int(part_find):
                        local_bool = True
                        # Going through every book for an item in index
                        for book_id in item_part['items']:
                            # Append only if book is not in the list (preventions of the duplicates)
                            if book_id not in local_result:
                                local_result.append(book_id)
    else:
        local_bool = True
    return local_bool, local_result


# Working with input... There we are working with input for the searching... Only for book names
def name_evaluator(cnsl_input: str, index_name):
    global index_book

    local_bool = False
    local_result = []
    if cnsl_input != '-':
        cnsl_input = cnsl_input.replace('"', "").replace("'", "").replace(",", "")
        # Going through every item in index
        for item_part in index_name:
            # Splitting input
            if item_part['name'][0].lower() != " " and item_part['name'][0].lower() != "":
                for part_find in cnsl_input.split(', '):
                    if str(part_find).lower() in str(item_part['name'][0]).lower():
                        local_bool = True
                        # Append only if book is not in the list (preventions of the duplicates)
                        if item_part['id'] not in local_result:
                            local_result.append(item_part['id'])
    else:
        local_bool = True
    return local_bool, local_result


# There we are comparing 2 lists... we need to be sure that we have books with each parameter from input
def final_evaluator(list_1, list_2):
    final_list = []
    final_result = False

    if list_1 and list_2:
        for item in list_1:
            if item in list_2:
                final_list.append(item)
                final_result = True
    elif list_1 and not list_2:
        final_list = list_1
        final_result = True
    elif not list_1 and list_2:
        final_list = list_2
        final_result = True
    else:
        final_result = True
    return final_result, final_list


# Entry-based listing of book titles found
def book_name(list_of_items):
    global index_book
    print("\nWe can recommend you this set of books. Enjoy :)")
    for item in list_of_items:
        print(index_book[int(item)]['name'][0])


# Processing input | Process of finding books
def find_my_books(name: str, author: str, country: str, language: str, series: str, genre: str, pub_date: str,
                  pages: str):
    results_bool = True
    results_list = []
    list_of_lists = []

    # Compare input parameters and find books from our parser
    name_bool, name_try = name_evaluator(name, index_book)
    author_bool, authors_try = text_evaluator(author, index_author)
    country_bool, country_try = text_evaluator(country, index_country)
    language_bool, languages_try = text_evaluator(language, index_language)
    series_bool, series_try = text_evaluator(series, index_series)
    genre_bool, genre_try = text_evaluator(genre, index_genre)
    pub_bool, pub_date_try = number_evaluator(pub_date, index_pub_date)
    pages_bool, pages_try = number_evaluator(pages, index_pages)

    # Every parameter has to be in the input
    if name_bool and author_bool and country_bool and language_bool and series_bool and genre_bool and pub_bool and pages_bool:
        list_of_lists.append(name_try)
        list_of_lists.append(authors_try)
        list_of_lists.append(country_try)
        list_of_lists.append(languages_try)
        list_of_lists.append(series_try)
        list_of_lists.append(genre_try)
        list_of_lists.append(pub_date_try)
        list_of_lists.append(pages_try)

        # Get final set of books which has matching parameters from input
        for single_list in list_of_lists:
            if results_bool:
                results_bool, results_list = final_evaluator(single_list, results_list)
        if results_bool:
            return results_list
        else:
            return []
    else:
        return []


# Prepare input from our files to JSON
def json_prepare(line):
    line = line.replace("<br />", "").replace("['", '["').replace("']", '"]').replace("{'", '{"') \
        .replace("':", '":').replace("',", '",').replace(", '", ', "').replace(": '", ': "') \
        .replace("'}", '"}').replace("\\", "")
    return line


# Create index for our books items
def create_indexes():
    index = 0
    for file in os.listdir('./final_books'):
        if ".crc" not in file and "_SUCCESS" not in file:
            path = "./final_books/" + str(file)
            if os.stat(path).st_size != 0:
                with open(path, 'r') as parsed_book:
                    for line in parsed_book:
                        line = json_prepare(line)
                        if line:
                            json_line = json.loads(line)
                            if json_line['name']:
                                # Create a book and see the magic
                                create_book(index_book, json_line['name'], index)
                                magic(json_line, index)
                                index = index + 1


if __name__ == '__main__':
    print("Welcome to WikiParser! :)")

    # If you want to run unit tests set variable 'test' as True, otherwise False
    test = False
    if test:
        print("Running tests...")
        unittest.main()
    else:
        # Indexing...
        print("Creating indexes...")
        create_indexes()

        # Find book
        print("Trying to find a book...")
        #test_books_list = find_my_books('-', "Stephen King", '-', '-', '-', "dark fantasy", '-', '-')
        test_books_list = find_my_books('-', "Graeme Base", '-', '-', '-', '-', '-', '-')
        if test_books_list:
            book_name(test_books_list)
        else:
            print("There are no matches for you. We're sorry :(.\nTry another combination :)")
