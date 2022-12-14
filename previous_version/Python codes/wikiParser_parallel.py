import json
import bz2
import time
import multiprocessing
import regex_dir.regex_expressions as re_exp

from queue import Empty

WIKI_FILE_PATH_1 = "../../src/enwiki-latest-pages-articles-multistream1.xml-p1p41242.bz2"
WIKI_FILE_PATH_2 = "../../src/enwiki-latest-pages-articles-multistream10.xml-p4045403p5399366.bz2"
WIKI_FILE_PATH_3 = "../../src/enwiki-20220920-pages-meta-current.xml.bz2"
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


class WikiParserRegex_test:

    def append_item_detail(self, line, raw_book: dict) -> dict:
        if '| name' in line:
            raw_book['name'] = line
        elif '| author' in line:
            raw_book['author'] = line
        elif '| country' in line:
            raw_book['country'] = line
        elif '| language' in line:
            raw_book['language'] = line
        elif '| series' in line:
            raw_book['series'] = line
        elif '| genre' in line:
            raw_book['genre'] = line
        elif '| pages' in line:
            raw_book['pages'] = line
        elif '| pub_date' in line or '| published' in line or '| release_date' in line or '| publish' in line:
            raw_book['pub_date'] = raw_book['pub_date'] + line
        return raw_book

    def book_parser(self, book_key: str, book_value: str) -> list:
        match book_key:
            case 'name':
                regex_output = re_exp.regex_name(book_value)
                return regex_output

            case 'author':
                regex_output = re_exp.regex_other(book_value)
                return regex_output

            case 'country':
                regex_output = re_exp.regex_other(book_value)
                return regex_output

            case 'language':
                regex_output = re_exp.regex_other(book_value)
                return regex_output

            case 'series':
                regex_output = re_exp.regex_other(book_value)
                return regex_output

            case 'genre':
                regex_output = re_exp.regex_other(book_value)
                return regex_output

            case 'pub_date':
                regex_output = re_exp.regex_pub(book_value)
                return regex_output

            case 'pages':
                regex_output = re_exp.regex_pages(book_value)
                return regex_output

    def producer(self, parsing_queue):
        print('Producer: Running', flush=True)
        counter = 0
        with bz2.BZ2File(WIKI_FILE_PATH_3, "r") as xml_file:
            book_found = False
            created_record = False
            for idx, line in enumerate(xml_file):
                if line and "Infobox book" in str(line):
                    book_found = True
                    raw_book = {
                        'name': "",
                        'author': "",
                        'country': "",
                        'language': "",
                        'series': "",
                        'genre': "",
                        'pub_date': "",
                        'pages': ""
                    }
                    created_record = True
                elif "}}" in str(line) and book_found:
                    book_found = False
                    # add to the queue
                    parsing_queue.put(raw_book)
                    # uncomment this if you want only 2 items from wiki as an example
                    # if counter == 5:
                    #     parsing_queue.put(None)
                    #     break
                    counter = counter + 1
                elif book_found and created_record and line:
                    raw_book = self.append_item_detail(str(line), raw_book)
        parsing_queue.put(None)
        print('Producer: Done', flush=True)

    def consumer(self, parsing_queue, writing_queue):
        print('Consumer: Running', flush=True)
        while True:
            try:
                item = parsing_queue.get(block=False)
                book = {
                    'name': [],
                    'author': [],
                    'country': [],
                    'language': [],
                    'series': [],
                    'genre': [],
                    'pub_date': [],
                    'pages': []
                }
                if item is not None:
                    for key in item:
                        book[key] = self.book_parser(key, item[key])
                    writing_queue.put(book)
            except Empty:
                continue
            # check for stop
            if item is None:
                writing_queue.put(None)
                print('Consumer: Done', flush=True)
                break

    def writer(self, writing_queue):
        print('Writer: Running', flush=True)
        written = False
        while True:
            # get a unit of work
            try:
                item = writing_queue.get(block=False)
                if item is not None:
                    with open("../../output_dir/parsing_output.json", "a", encoding='utf-8') as json_file:
                        if written and empty_book != item:
                            json_file.write(',\n')
                            json.dump(item, json_file, ensure_ascii=False)
                        elif not written and empty_book != item:
                            json_file.write('[')
                            json.dump(item, json_file, ensure_ascii=False)
                            written = True
            except Empty:
                continue
            # check for stop
            if item is None:
                with open("../../output_dir/parsing_output.json", "a", encoding='utf-8') as json_file:
                    json_file.write(']')
                    json_file.close()
                print('Writer: Done', flush=True)
                break


if __name__ == '__main__':
    # start measuring time
    start = time.time()

    wikiParser = WikiParserRegex_test()

    # create the shared queues
    parsing_queue = multiprocessing.Queue()
    writing_queue = multiprocessing.Queue()

    # start the producer, consumer and writer
    producer_process = multiprocessing.Process(target=wikiParser.producer, args=(parsing_queue,))
    producer_process.start()

    consumer_process = multiprocessing.Process(target=wikiParser.consumer, args=(parsing_queue, writing_queue,))
    consumer_process.start()

    writer_process = multiprocessing.Process(target=wikiParser.writer, args=(writing_queue,))
    writer_process.start()

    # wait for all processes to finish
    producer_process.join()
    consumer_process.join()
    writer_process.join()

    parsing_queue.close()
    writing_queue.close()

    # stop measuring time
    end = time.time()
    print(end - start)

    print("The End!!!")
