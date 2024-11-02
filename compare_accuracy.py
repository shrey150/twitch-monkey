import argparse

def calculate_match_percentage(file_path):
    total_words = 0
    no_match_count = 0

    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            if "no match found for" in line:
                no_match_count += 1
            else:
                total_words += 1

    total_entries = total_words + no_match_count
    match_percentage = (total_words / total_entries) * 100 if total_entries > 0 else 0

    print(f"Total words: {total_words}")
    print(f"No match found entries: {no_match_count}")
    print(f"Match percentage: {match_percentage:.2f}%")

def main():
    parser = argparse.ArgumentParser(description='Calculate match percentage from a file.')
    parser.add_argument('file_path', type=str, help='Path to the file to be processed')
    args = parser.parse_args()

    calculate_match_percentage(args.file_path)

if __name__ == "__main__":
    main()