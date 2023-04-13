import gzip
import requests
import json
import time
import os
import random
from tqdm import tqdm
from collections import defaultdict
from xml.etree import cElementTree as ET

# -----------------------------

MAX_REQUEST_RATE = 600  # max requests per minute

RESULTS_PATH = 'podcast_search_results_full.json'
RSS_DICTS_NAME = 'rss_dicts/rss_dicts_chunk'


# -----------------------------

def etree_to_dict(t):
    d = {t.tag: {} if t.attrib else None}
    children = list(t)
    if children:
        dd = defaultdict(list)
        for dc in map(etree_to_dict, children):
            for k, v in dc.items():
                dd[k].append(v)
        d = {t.tag: {k: v[0] if len(v) == 1 else v for k, v in dd.items()}}
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
    # request with timeout
    response = requests.get(url, timeout=1)
    if response.status_code != 200:
        raise Exception(f"Error: {response.status_code} with {url}")
    feed_d = etree_to_dict(ET.XML(response.text))
    return feed_d


# split a dictionary into multiple smaller dictionaries
def split_dictionary(d, chunk_size=10_000):
    return [dict(list(d.items())[i:i + chunk_size]) for i in range(0, len(d), chunk_size)]


def json_dump_zipped(d, path):
    json_str = json.dumps(d)
    json_bytes = json_str.encode('utf-8')
    compressed_bytes = gzip.compress(json_bytes)
    with open(path + '.tmp', "wb") as f:
        f.write(compressed_bytes)
    os.replace(path + '.tmp', path)


def json_load_zipped(path):
    with open(path, "rb") as f:
        compressed_bytes = f.read()
    json_bytes = gzip.decompress(compressed_bytes)
    json_str = json_bytes.decode('utf-8')
    d = json.loads(json_str)
    return d


def save(chunk_size=10_000):
    chunked_rss_dicts = split_dictionary(rss_dicts, chunk_size)
    for i, rss_dict_chunk in enumerate(chunked_rss_dicts):
        json_dump_zipped(rss_dict_chunk, f'{RSS_DICTS_NAME}_{i}.json.gz')


def load():
    rss_dicts = {}
    i = 0
    while True:
        path = f'{RSS_DICTS_NAME}_{i}.json.gz'
        if not os.path.exists(path):
            break
        rss_dicts.update(json_load_zipped(path))
        i += 1
    return rss_dicts


# -----------------------------

if __name__ == '__main__':
    results = json.loads(open(RESULTS_PATH, 'r').read())
    
    rss_dicts = load()
    rss_links = list(rss_dicts.keys())
    
    min_time_between_requests = 60.0 / MAX_REQUEST_RATE
    last_request_time = 0.0
    n_exceptions = 0
    n_exceptions_in_a_row = 0
    counter = 0
    done = False
    
    new_results = [result for result in results if ('feedUrl' in result) and (result['feedUrl'] not in rss_dicts)]
    random.shuffle(new_results)
    for result in tqdm(new_results, initial=len(rss_dicts), total=len(results), smoothing=0.0):
        counter += 1
        
        # wait for time between requests
        time_since_last_request = time.time() - last_request_time
        if time_since_last_request < min_time_between_requests:
            time.sleep(min_time_between_requests - time_since_last_request)
        
        if result['feedUrl'] in rss_links:
            continue
        
        try:
            last_request_time = time.time()
            response_dict = RSS_to_dict(result['feedUrl'])
            n_exceptions_in_a_row = 0
        except Exception as e:
            n_exceptions += 1
            n_exceptions_in_a_row += 1
            if n_exceptions_in_a_row > 10:
                print(e)
            continue
        except KeyboardInterrupt as e:
            raise e
        
        rss_dicts[result['feedUrl']] = response_dict
        rss_links.append(result['feedUrl'])
        
        if counter % 40000 == 0:
            print(f"Saving {len(rss_links)} RSS links, {n_exceptions} skipped")
            save_start = time.time()
            save()
            print(f"Took {time.time() - save_start:.1f}s to save, Continuing now!")
    
    print("Saving for the final time")
    save()
    
    # Seperately save list of feedUrl's for podcast downloaders
    with open('feedurls.txt', 'w') as f:
        for result in results:
            if 'feedUrl' in result:
                f.write(result['feedUrl'] + '\n')
    
    print("Finished")