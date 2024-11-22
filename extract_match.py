import sys

def extract_no_match(input_file, output_file):
    try:
        # Open the input file for reading
        with open(input_file, 'r', encoding='utf-8') as infile:
            # Read all lines
            lines = infile.readlines()
        
        # Filter lines containing "no match found"
        no_match_lines = [line for line in lines if "no match found" in line]

        # Write these lines to the output file
        with open(output_file, 'w') as outfile:
            outfile.writelines(no_match_lines)

        print(f"Extracted {len(no_match_lines)} 'no match found' lines to '{output_file}'.")
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # Ensure proper usage
    if len(sys.argv) != 3:
        print("Usage: python extract_no_match.py <input_file> <output_file>")
    else:
        input_file = sys.argv[1]
        output_file = sys.argv[2]
        extract_no_match(input_file, output_file)
