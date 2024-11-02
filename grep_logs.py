import subprocess
import multiprocessing
import time
from datetime import datetime
from collections import OrderedDict
import argparse

def grep_word(index_word, logs_file):
    """Runs grep for the given word and returns the formatted result with its original index."""
    index, word = index_word
    try:
        # Form the grep command, use -i for case-insensitive search
        command = f'grep -m 1 -i \'"text":"{word}"\' {logs_file}'
        # Run the grep command, capture output
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:  # if grep found a match
            # Extract the first line found by grep
            first_line = result.stdout.splitlines()[0]
            # Parse the JSON to extract the timestamp, displayName, and text
            start_index = first_line.find('"timestamp":"') + 13
            end_index = first_line.find('"', start_index)
            timestamp = first_line[start_index:end_index]

            chatter_start = first_line.find('"displayName":"') + 15
            chatter_end = first_line.find('"', chatter_start)
            chatter = first_line[chatter_start:chatter_end]

            chatter_text_start = first_line.find('"text":"') + 8
            chatter_text_end = first_line.find('"', chatter_text_start)
            chatter_text = first_line[chatter_text_start:chatter_text_end]

            # Format the timestamp and the result
            ts = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            formatted_timestamp = ts.strftime('%m-%d-%Y %H:%M')
            
            # Return formatted result with original index
            print(f"Match found for {word}")
            return index, f"{formatted_timestamp} {chatter}: {chatter_text}"
        else:
            # If no match was found
            return index, f'no match found for {word}'
    except Exception as e:
        return index, f'Error processing {word}: {str(e)}'

def process_words(words, logs_file):
    """Processes each word in parallel using multiprocessing."""
    with multiprocessing.Pool() as pool:
        # Run grep_word function in parallel for each word with its index
        results = pool.starmap(grep_word, [(index_word, logs_file) for index_word in enumerate(words)])
    return results

def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Process some logs.')
    parser.add_argument('logs_file', type=str, help='Path to the logs file')
    parser.add_argument('words_file', type=str, help='Path to the words file')
    parser.add_argument('output_file', type=str, help='Path to the output file')
    args = parser.parse_args()

    # Load all words from the words file and remove duplicates
    with open(args.words_file, 'r') as f:
        words = list(OrderedDict.fromkeys(line.strip() for line in f.readlines()))

    # print(words[:10])
    print(f"Loaded {len(words)} unique words from {args.words_file}")

    # Start time to track performance
    start_time = time.time()

    # Process words in parallel and capture results
    results = process_words(words, args.logs_file)

    # Sort results based on the original indices
    results.sort(key=lambda x: x[0])

    # Write results to the output file
    with open(args.output_file, 'w') as f:
        for _, result in results:
            f.write(result + '\n')

    # Print performance stats
    end_time = time.time()
    total_time = end_time - start_time
    print(f"Processing completed in {total_time:.2f} seconds.")
    print(f"Results saved to {args.output_file}")

if __name__ == '__main__':
    main()