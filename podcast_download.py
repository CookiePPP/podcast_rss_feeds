# pip3 install langdetect
import os
import time
import argparse
import subprocess
from os.path import exists
from shutil import rmtree
from multiprocessing import Process
from utils import RSS_to_title, is_empty, ascii_percent, make_path_safe
from langdetect import detect
from tqdm import tqdm

def add_completed_url(url):
    completed_urls.add(url)
    with open(completed_file, 'a') as f:
        f.write(f"{url}\n")

def run(cmd):
    with open(os.devnull, 'w') as devnull:
        subprocess.call(cmd, shell=True, stdout=devnull)

# function to download a single podcast
def download_podcast(url, progress_str):
    try:
        title, episode_titles = RSS_to_title(url)
        title_is_invalid = ascii_percent(title) < 95 or len(title.strip()) < 8 or len(title.strip()) > 80
        language_is_invalid = detect(title+' '+' '.join(episode_titles)) != 'en'
        if title_is_invalid or language_is_invalid:
            print(f"{progress_str} Skipping [{title}]  ({url})")
            add_completed_url(url)
            return
    except Exception as e:
        print(f"{progress_str} Skipping {url} (error: {e})")
        add_completed_url(url)
        return
    
    safe_title = make_path_safe(title)
    pod_dur = os.path.join(out_dir, safe_title)
    os.makedirs(pod_dur, exist_ok=True)
    
    #print(f"{progress_str} Downloading [{safe_title}]  ({url})")
    run(f'{binary_path} "{url}" "{pod_dur}"')
    
    add_completed_url(url)
    
    if not exists(pod_dur):
        return
    
    if is_empty(pod_dur):
        # remove empty directory
        rmtree(pod_dur)
    #else:
        # convert to opus
        #print(f"{progress_str} Converting [{safe_title}]  ({url})")
        #dir_to_opus(pod_dur, max_workers=4).shutdown(wait=True)
    
    return

def pretty_format_time(secs: float):
    """12386712 -> 3d:10h:17m:52s"""
    days = int(secs // (24 * 3600))
    secs = secs % (24 * 3600)
    hours = int(secs // 3600)
    secs %= 3600
    minutes = int(secs // 60)
    secs %= 60
    seconds = int(secs)
    ETA_str = f"ETA: {days}d:{hours:02d}h:{minutes:02d}m:{seconds:02d}s"
    return ETA_str

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_chunk', type=int, default=-1)
    parser.add_argument('--final_chunk', type=int, default=-1)
    args = parser.parse_args()
    return args.start_chunk, args.final_chunk

if __name__ == '__main__':
    start_chunk, final_chunk = parse_args()
    assert start_chunk != -1 and final_chunk != -1, "--start_chunk={int} and --final_chunk={int} must be specified"
    if final_chunk == -1:
        final_chunk = float('inf')
    
    completed_file = 'completed.txt'
    binary_path = './poddl'
    max_processes = 4 # decides how many connections to make at once
    
    # load completed URLs (to skip)
    completed_urls = set()
    if exists(completed_file):
        with open(completed_file, 'r') as f:
            completed_urls = set(l.strip() for l in f.readlines())
    
    current_chunk = start_chunk
    while current_chunk <= final_chunk:
        tsv_file = os.path.join('podcast_tsv_chunks', f'podcast_over1hr_english_chunk_{current_chunk}.tsv')
        out_dir = f'podcasts_chunk_{current_chunk}'
        
        # check the binary exists
        if not exists(binary_path):
            raise ValueError("Invalid path to poddl.exe binary")
        
        # read URLs from file
        print(f"Reading {tsv_file}")
        with open(tsv_file, 'r') as f:
            lines = f.readlines()
            urls = [line.strip().split('\t')[0] for line in lines]
            durations = [float(line.strip().split('\t')[2]) for line in lines]
        total_duration = sum(durations)
        
        # init progress bar / eta
        ETA_str = ''
        pbar = tqdm(
            initial=sum(durations[id] for id, url in enumerate(urls) if url in completed_urls),
            total=total_duration, desc=f'downloading chunk {current_chunk}', smoothing=0.0)
        
        # run poddl.exe on each URL that hasn't been completed, limited by max_processes
        active_processes = []
        active_process_durations = []
        for id, url in enumerate(urls):
            if url in completed_urls:
                #print(f"Skipping {url} (already processed)")
                continue
            else:
                start_time = time.time()
                duration = durations[id]
                
                # wait for a process to finish
                while len(active_processes) >= max_processes:
                    for i, p in list(enumerate(active_processes)):
                        p_dur = active_process_durations[i]
                        if not p.is_alive():
                            # process finished, remove it from the list
                            active_processes.remove(p)
                            active_process_durations.remove(p_dur)
                            #total_duration_done += p_dur
                            pbar.update(p_dur)
                            
                            # calculate new ETA
                            #total_time_elapsed = time.time() - start_time
                            #time_per_duration = total_time_elapsed / total_duration_done
                            #remaining_duration = total_duration - total_duration_done
                            #ETA_secs = remaining_duration * time_per_duration
                            #ETA_str = pretty_format_time(ETA_secs)
                            break
                    #print(f"{ETA_str}  {total_duration_done/total_duration*100:.2f}%  {len(active_processes)}/{max_processes}  {url}", end='\r')
                    time.sleep(0.1)
                
                # add a new process
                p = Process(target=download_podcast, args=(url,ETA_str))
                p.start()
                active_processes.append(p)
                active_process_durations.append(duration)
        
        current_chunk += 1