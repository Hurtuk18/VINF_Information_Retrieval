import json

"""
    CREATE INDEX part
"""

index_book = []

index_name = []
index_author = []
index_country = []
index_language = []
index_series = []
index_genre = []
index_pub_date = []
index_pages = []

"""
    End of CREATE INDEX part
"""

def create_book(book_name, book_id):
    global index_book
    index_book.append({
        'id': book_id,
        'name': book_name
    })

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
    
    if ix_id == -1 and len(index_name) == 0:
        return ix_exists, last_id
    elif ix_id == -1 and len(index_name) != 0:
        return ix_exists, last_id + 1
    else:
        return ix_exists, ix_id

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
    else:
        index_name[ix_id]['items'].append(book_id)

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

def text_evaluator(cnsl_input: str, index_name) -> list:
    global index_author, index_country, index_language, index_series, index_genre

    local_bool = False
    local_result = []
    if cnsl_input != '-':
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

def number_evaluator(cnsl_input: str, index_name) -> list:
    global index_pub_date, index_pages

    local_bool = False
    local_result = []
    if cnsl_input != '-':
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

def name_evaluator(cnsl_input: str, index_name) -> list:
    global index_book

    local_bool = False
    local_result = []
    if cnsl_input != '-':
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

def book_name(list_of_items):
    global index_book
    print("\nWe can recommend you this set of books. Enjoy :)")
    for item in list_of_items:
        print(index_book[int(item)]['name'][0])

def find_my_books(name: str, author: str, country: str, language: str, series: str, genre: str, pub_date: str, pages: str):
    results_bool = True
    results_list = []
    list_of_lists = []
    
    name_bool, name_try = name_evaluator(name, index_book)
    author_bool, authors_try = text_evaluator(author, index_author)
    country_bool, country_try = text_evaluator(country, index_country)
    language_bool, languages_try = text_evaluator(language, index_language)
    series_bool, series_try = text_evaluator(series, index_series)
    genre_bool, genre_try = text_evaluator(genre, index_genre)
    pub_bool, pub_date_try = number_evaluator(pub_date, index_pub_date)
    pages_bool, pages_try = number_evaluator(pages, index_pages)

    if name_bool and author_bool and country_bool and language_bool and series_bool and genre_bool and pub_bool and pages_bool:
        list_of_lists.append(name_try)
        list_of_lists.append(authors_try)
        list_of_lists.append(country_try)
        list_of_lists.append(languages_try)
        list_of_lists.append(series_try)
        list_of_lists.append(genre_try)
        list_of_lists.append(pub_date_try)
        list_of_lists.append(pages_try)
        
        for single_list in list_of_lists:
            if results_bool:
                results_bool, results_list = final_evaluator(single_list, results_list)
        
        if results_bool:
            book_name(results_list)
        else:
            print("There are no matches for you. We're sorry :(.\nTry another combination :)")
    else:
        print("There are no matches for you. We're sorry :(.\nTry another combination :)")

def start_searching():
    name, author, country, language, series, genre, pub_date, pages = input("Hello in WikiParser!\n\n"
            "I've created an app that will search for the perfect set of books for you based on input parameters from the console!\n\n"
            "So LET'S GOO!!!\n\n"
            "Please insert the input from the keyboard, following these rules:\n"
            "- To separate multiple entries for a category use ',' and after this symbol use Space ' '\n"
            "- To separate categories use ';' and after this symbol use Space ' '\n"
            "- To skip a category, type '-'\n"
            "- !!! It is NECESSARY to enter all categories !!!\n\n"
            "The order of the categories is:\n"
            "- 'name'\n"
            "- 'author'\n"
            "- 'country'\n"
            "- 'language'\n"
            "- 'series'\n"
            "- 'genre'\n"
            "- 'pub_date'\n"
            "- 'pages'\n\n"
            "Thank you for using WikiParser!").split('; ')
    find_my_books(name, author, country, language, series, genre, pub_date, pages)
    
    # -; Graeme Base; -; -; -; -; -; -
    # -; Graeme Base; United Kingdom; -; -; -; -; -
    # -; Graeme Base; -; english; -; -; -; -
    # -; Jonathan Swift; -; -; -; -; -; -
    # -; Abracadabra; -; -; -; -; -; -
    # -; Graeme Base; United Kingdom; English; -; satire; 1962; 192
    # -; Graeme Base; United Kingdom; English; -; Satirical; 1962; 192
    # A Modest Proposal; -; -; -; -; -; -; -
    # Animal; -; -; -; -; -; -; -

if __name__ == '__main__':
    print("Here we go... Let's the fun begin! :)")

    # Indexing...
    with open('output_dir/test_json_file.json') as json_file:
        data = json.load(json_file)
        for index, json_line in enumerate(data):
            if json_line['name']:
                # Create a book and see the magic
                create_book(json_line['name'], index)
                magic(json_line, index)
    
    # Searching for a books
    start_searching()
    