import requests
import json
import time
import os
from tqdm import tqdm


def load_podcast_search(term):
    url = f"https://itunes.apple.com/search?media=podcast&entity=podcast&attribute=titleTerm&limit=200&term={term}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    }
    response = requests.get(url, headers=headers)
    code = response.status_code
    if code == 200:
        data = json.loads(response.text)
        assert data is not None, "No data returned"
        return data
    else:
        raise Exception(f"Error: {code} with {term}\n{response.text}")


# -----------------------------

MAX_REQUEST_RATE = 20  # max requests per minute, itunes api has a limit of 'approximately' 20

RESULTS_PATH = 'podcast_search_results_full.json'
TERM_PATH = 'current_term.txt'

results = json.loads(open(RESULTS_PATH, 'r').read()) if os.path.exists(RESULTS_PATH) else []
result_ids = set()

term = open(TERM_PATH, 'r').read() if os.path.exists(TERM_PATH) else 'a'


def save_results():
    with open(RESULTS_PATH + '.tmp', 'w') as f:
        json.dump(results, f)
    os.replace(RESULTS_PATH + '.tmp', RESULTS_PATH)
    with open(TERM_PATH + '.tmp', 'w') as f:
        f.write(term)
    os.replace(TERM_PATH + '.tmp', TERM_PATH)


min_time_between_requests = 60.0 / MAX_REQUEST_RATE
last_request_time = 0.0
n_exceptions = 0
counter = 0
done = False
pbar = tqdm(total=26**2)
while not done:
    
    # wait for time between requests
    time_since_last_request = time.time() - last_request_time
    if time_since_last_request < min_time_between_requests:
        time.sleep(min_time_between_requests - time_since_last_request)
    
    response_dict = None
    while response_dict is None:
        try:
            last_request_time = time.time()
            response_dict = load_podcast_search(term)
            for result in response_dict['results']:
                if result['trackId'] not in result_ids:
                    result_ids.add(result['trackId'])
                    results.append(result)
            n_exceptions = 0
        except Exception as e:
            # if we get an exception, wait a bit and try again
            
            if n_exceptions > 5:  # if we get 6 exceptions in a row,
                # print stacktrace and give up on this term
                import traceback
                
                traceback.print_exc()
                n_exceptions -= 1
                break
            
            time.sleep(2 ** n_exceptions)  # exponential backoff
            n_exceptions += 1
        except KeyboardInterrupt:
            raise KeyboardInterrupt
    
    # set next term
    n_responses = len(response_dict['results'])
    tqdm.write(f"term: \"{term}\" has {n_responses} results")
    old_term = term # save old term for progress bar
    if n_responses >= 200: # if we got the maximum number of results, increase how specific the search is
        term += 'a'
    else: # otherwise, increment the last character
        term = term[:-1] + chr(ord(term[-1]) + 1)  # increment last character
        # if we've reached the end of the alphabet, go up a level and increment the new last character
        while term.endswith('{') and not done:
            term = term[:-1]
            if term == '':
                done = True
                break
            term = term[:-1] + chr(ord(term[-1]) + 1)
    # e.g:
    # if 'aa' returned under 200 results: 'aa' -> 'ab'
    # but 'aa' -> 'aaa' if 'aa' has more results then we can see yet
    # there's also special case where 'aaz' -> 'ab' because there is no char after 'z'
    
    # dumb pbar to show progress (I can implement a proper pbar if someone cares)
    if f'{old_term} '[1] != f'{term} '[1]:
        pbar.update(1)
    
    if counter % 50 == 0:
        save_results()
    counter += 1
save_results()

print(f"Found {len(results)} results.")

# Seperately save list of feedUrl's for podcast downloaders
with open('feedurls.txt', 'w') as f:
    for result in results:
        f.write(result['feedUrl'] + '\n')