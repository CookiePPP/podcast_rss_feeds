import os
import re
from glob import glob

import requests
from collections import defaultdict
from xml.etree import cElementTree as ET

def etree_to_dict(t):
    d = {t.tag: {} if t.attrib else None}
    children = list(t)
    if children:
        dd = defaultdict(list)
        for dc in map(etree_to_dict, children):
            for k, v in dc.items():
                dd[k].append(v)
        d = {t.tag: {k:v[0] if len(v) == 1 else v for k, v in dd.items()}}
    if t.attrib:
        d[t.tag].update(('@' + k, v) for k, v in t.attrib.items())
    if t.text:
        text = t.text.strip()
        if children or t.attrib:
            if text:
              d[t.tag]['#text'] = text
        else:
            d[t.tag] = text
    return d

def RSS_to_dict(url):
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Error: {response.status_code} with {url}")
    feed_d = etree_to_dict(ET.XML(response.text))
    return feed_d

def RSS_to_title(url):
    rss_dict = RSS_to_dict(url)
    main_title = rss_dict['rss']['channel']['title']
    episodes = rss_dict['rss']['channel']['item']
    if type(episodes) is list:
        episode_titles = [item.get('title', '') for item in episodes]
    elif type(episodes) is dict:
        episode_titles = [episodes.get('title', '')]
    else:
        raise Exception(f"Unexpected type for episodes: {type(episodes)}")
    return main_title, episode_titles

def ascii_percent(text):
    """
    Calculates the percentage of ASCII characters in a string.
    """
    ascii_count = 0
    for char in text:
        if ord(char) < 128:
            ascii_count += 1
    return (ascii_count / len(text)) * 100

def is_empty(directory):
    return glob(os.path.join(directory, '**', '*.*'), recursive=True) == 0

def make_path_safe(path_str):
    # Remove any characters that are not allowed in Windows file names
    path_str = re.sub(r'[\\/*?:;"<>|]', '', path_str)
    # Trim any leading or trailing spaces
    path_str = path_str.strip()
    # Limit the length of the file name to 260 characters
    path_str = path_str[:260]
    return path_str

