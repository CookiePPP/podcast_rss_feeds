import os
import subprocess
import time
import concurrent.futures

# File extensions to monitor
file_extensions = [".mp3", ".flac", ".wav", ".m4a", ".mp4"]

def convert_file_to_opus(input_file_path):
    # Build the paths to the output file
    output_file_path = os.path.splitext(input_file_path)[0] + ".opus"

    # Check if file has been written to in the last 60 seconds
    last_modified = os.path.getmtime(input_file_path)
    if time.time() - last_modified < 60:
        return
    
    # Convert the input file to OPUS using ffmpeg
    command = [
        "ffmpeg",
        "-i", input_file_path,
        "-ac", "1",  # mono
        "-b:a", "32k",  # 32kbps
        "-c:a", "libopus",
        "-vn",  # no video
        "-y",  # overwrite output file
        output_file_path
    ]
    subprocess.run(command)

    # Delete the input file if output file exists and is not empty
    if os.path.exists(output_file_path) and os.path.getsize(output_file_path) > 0.05 * os.path.getsize(input_file_path):
        try:
            os.remove(input_file_path)
        except: pass

def dir_to_opus_paths(directory): # generator
    for root, dirs, files in os.walk(directory):
        for file in files:
            if os.path.splitext(file)[1] in file_extensions:
                yield os.path.join(root, file)
    
def dir_to_opus(directory, max_workers=8):
    # Convert all files in the directory to OPUS
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Convert all files in the target directory to OPUS
        for file_path in dir_to_opus_paths(directory):
            executor.submit(convert_file_to_opus, file_path)
    return executor

# If this file is run directly, convert all files in the target directory to OPUS
if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--n_workers", default=4, type=int,
                        help="The number of workers to run for re-encoding the audio.")
    args = parser.parse_args()
    
    # Path to the directory to monitor
    directory_to_monitor = "."
    
    # Maximum number of threads to use
    max_workers = int(args.n_workers)
    
    while True:
        # Convert all files in the target directory to OPUS
        dir_to_opus(directory_to_monitor, max_workers=max_workers)
        
        # Wait for 10 seconds then check again
        time.sleep(10)
