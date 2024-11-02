import sys
import re

def words_to_lines(input_file_name, output_file_name):
    try:
        with open(input_file_name, 'r') as file:
            text = file.read()
        
        # Replace all punctuation except apostrophes with whitespace using regex
        text = re.sub(r"[^\w\s']", ' ', text)

        # Split text into words
        words = text.split()

        # Write each word to a new line
        with open(output_file_name, 'w') as output_file:
            for word in words:
                output_file.write(f"{word}\n")
        
        print(f"Words have been written to '{output_file_name}'.")
    
    except FileNotFoundError:
        print(f"Error: The file '{input_file_name}' was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <input_text_file> <output_text_file>")
    else:
        words_to_lines(sys.argv[1], sys.argv[2])