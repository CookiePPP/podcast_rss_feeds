import gzip
import json
import math
import time
import os
import random
from tqdm import tqdm

# -----------------------------

RSS_DICTS_NAME = 'rss_dicts/rss_dicts_chunk'

# -----------------------------

def json_load_zipped(path):
    with open(path, "rb") as f:
        compressed_bytes = f.read()
    json_bytes = gzip.decompress(compressed_bytes)
    json_str = json_bytes.decode('utf-8')
    d = json.loads(json_str)
    return d

def load(n_chunks='all'):
    rss_dicts = {}
    i = 0
    while True:
        path = f'{RSS_DICTS_NAME}_{i}.json.gz'
        if not os.path.exists(path):
            break
        rss_dicts.update(json_load_zipped(path))
        i += 1
        if n_chunks != 'all' and i >= n_chunks:
            break
    return rss_dicts

def strip_curly_brackets(s):
    # remove any curly bracket wrapped text from the string
    while True:
        start = s.find('{')
        if start == -1:
            break
        end = s.find('}')
        if end == -1:
            break
        s = s[:start] + s[end+1:]
    return s

# -----------------------------

if __name__ == '__main__':
    
    print('Loading rss_dicts...')
    rss_dicts = load()
    print('Done.')
    
    n_english = 0
    n_non_english = 0
    n_failed = 0
    english_duration = 0.0
    non_english_duration = 0.0
    
    pod_tsv_list = [] # [f'{rss_link}\t{lang}\t{duration_s}' for rss_link, lang in rss_linklang]
    
    for i, (rss_link, rss_dict) in enumerate(rss_dicts.items()):
        if i < 0:
            continue
        try:
            pod_dict = rss_dict['rss']['channel']
            pod_dict = {strip_curly_brackets(k): v for k, v in pod_dict.items()}
            # keys = ['title', 'description', 'link', 'image', 'generator', 'lastBuildDate', 'author',
            #         'copyright', 'language', 'summary', 'type', 'owner', 'explicit', 'category', 'item']
            
            #pod_title     = pod_dict['title']
            #pod_link      = pod_dict['link'] # sometimes missing
            #pod_image_url = pod_dict['image'] # sometimes missing
            #pod_copyright = pod_dict.get('copyright', None)
            #pod_author    = pod_dict['author']
            pod_language  = pod_dict['language'] # 'en-us'
            pod_description = pod_dict['description']
            if isinstance(pod_dict['item'], dict):
                pod_dict['item'] = [pod_dict['item']]
            elif isinstance(pod_dict['item'], str):
                pod_dict['item'] = [json.loads(pod_dict['item'])]
            
            pod_duration_sec = 0.0
            for ep_dict in pod_dict['item']:
                ep_dict = {strip_curly_brackets(k): v for k, v in ep_dict.items()}
                # keys = ['title', 'description', 'link', 'guid', 'creator', 'pubDate', 'enclosure', 'summary',
                #         'explicit', 'duration', 'image', 'season', 'episode', 'episodeType']
                
                if 'duration' not in ep_dict:
                    continue
                
                #ep_title    = ep_dict['title']
                #ep_link     = ep_dict['link']
                ep_duration = ep_dict['duration'] # '00:00:00' or '00:00'
                #ep_pubDate  = ep_dict['pubDate']  # 'Wed, 01 Jan 2020 00:00:00 +0000'
                
                try: # https://support.google.com/podcast-publishers/answer/9889544?hl=en#:~:text=Duration%20of%20the%20episode%2C%20in%20one%20of%20the%20following%20formats%3A
                    ep_duration = ep_duration.replace(';', ':').replace(',', ':').replace('::', ':') # fix basic typos
                    if ep_duration.count(':') == 2:
                        ep_duration_sec = 0.0
                        hr, mn, sec = ep_duration.split(':')
                        ep_duration_sec += float(hr) * 3600
                        ep_duration_sec += float(mn) * 60
                        ep_duration_sec += float(sec)
                        pod_duration_sec += ep_duration_sec
                    elif ep_duration.count(':') == 1:
                        ep_duration_sec = 0.0
                        mn, sec = ep_duration.split(':')
                        ep_duration_sec += float(mn) * 60
                        ep_duration_sec += float(sec)
                        pod_duration_sec += ep_duration_sec
                    else:
                        ep_duration_sec = float(ep_duration)
                    assert math.isfinite(ep_duration_sec), f'ep_duration is not finite: {ep_duration}'
                    assert ep_duration_sec >= 0.0, f'ep_duration is negative: {ep_duration}'
                    assert ep_duration_sec <= 3600*8, f'ep_duration is longer than 8 hours: {ep_duration}'
                except:
                    raise ValueError(f'ep_duration of "{ep_duration}" is not valid')
            
            if pod_duration_sec < 3600:
                continue
            
            pod_tsv_list.append(f'{rss_link.strip()}\t{pod_language.strip()}\t{pod_duration_sec:.1f}')
            
            # custom code
            if 'en' in pod_language:
                n_english += 1
                english_duration += pod_duration_sec
            else:
                n_non_english += 1
                non_english_duration += pod_duration_sec
        except Exception as e:
            import traceback
            traceback.print_exc()
            n_failed += 1
            continue
        except KeyboardInterrupt:
            break
    
    print(f'English: {n_english}')
    print(f'Non-English: {n_non_english}')
    print(f'Total: {n_english + n_non_english}')
    print(f'Failed: {n_failed}')
    print('')
    print(f'English Duration: {english_duration}')
    print(f'Non-English Duration: {non_english_duration}')
    print(f'Total Duration: {english_duration + non_english_duration}')
    
    with open('podcast_over1hr.tsv', 'w') as f:
        f.write('\n'.join(pod_tsv_list))

    completed_url_paths = ["feedurls_h/feedurls_rank01_h.txt", "feedurls_h/feedurls_rank09_h.txt"]
    completed_urls = set()
    for url_path in completed_url_paths:
        with open(url_path, 'r') as f:
            for line in f.readlines():
                completed_urls.add(line.strip())
    
    max_duration_per_chunk = 268435456.0 # 1TB @ 32kbps Opus
    pod_tsv_list_chunk = []
    n_duration = 0
    chunk_i = 0
    for _ in pod_tsv_list:
        rss_link, lang, duration_s = _.split('\t')
        if rss_link in completed_urls:
            continue
        if 'en' in lang:
            pod_tsv_list_chunk.append(_)
            n_duration += float(duration_s)
        if n_duration > max_duration_per_chunk:
            with open(f'podcast_tsv_chunks/podcast_over1hr_english_chunk_{chunk_i}.tsv', 'w') as f:
                f.write('\n'.join(pod_tsv_list_chunk))
            pod_tsv_list_chunk = []
            n_duration = 0
            chunk_i += 1