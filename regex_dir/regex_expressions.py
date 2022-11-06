import re

"""
    This file contains all regex expressions which I'll use in my Parser
    
    author: Bc. Adam Hurtuk
"""


"""
    Infobox book section
"""


def regex_plain(input: str) -> list:
    regex_output = re.findall(r"(?<=\= )(.*)(?=\\n)", input)
    if regex_output:
        return regex_output
    else:
        return []


def regex_name(input: str) -> list:
    if '<' in input:
        regex_output = re.findall(r"(?<=\= )(.*)(?=\<)", input)
        if regex_output:
            return regex_output
        else:
            return []
    else:
        return regex_plain(input)


def regex_pub(input: str) -> list:
    regex_output = re.findall(r"(\d{4})(.*?)", input)
    if regex_output:
        return regex_output[0][0]
    else:
        return []


def regex_pages(input: str) -> list:
    regex_output = re.findall("(\d{1,4})(.*?)", input)
    if regex_output:
        return regex_output[0][0]
    else:
        return []


def regex_other(input: str) -> list:
    if '[' in input:
        regex_output = re.findall(r"(?<=\[\[)(.*?)(?=[\||\]])", input)
        if regex_output:
            return regex_output
        else:
            return []
    else:
        return regex_plain(input)


"""
    End of Infobox book section
"""
