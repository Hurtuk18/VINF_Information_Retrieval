import re

"""
    This file contains all regex expressions which I'll use in my Parser
    
    author: Bc. Adam Hurtuk
"""


"""
    Infobox book section
"""


def regex_plain(input: str) -> str:
    return str(re.findall("(?<=\= )(.*)(?=\n)", input))


def regex_name(input: str) -> str:
    if '<' in input:
        return str(re.findall("(?<=\= )(.*)(?=\<)", input))
    else:
        return regex_plain(input)


def regex_pub(input: str) -> str:
    return str(re.findall("(\d{4})(.*?)", input)[0])


def regex_pages(input: str) -> str:
    return str(re.findall("(\d{1,4})(.*?)", input)[0])


def regex_other(input: str) -> str:
    if '[' in input:
        return str(re.findall("(?<=\[\[)(.*?)(?=[\||\]])", input))
    else:
        return regex_plain(input)


"""
    End of Infobox book section
"""
