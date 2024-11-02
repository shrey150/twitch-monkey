def generate_xqc_r_and_j(singleline_file, chat_file, output_file):
    # Read the Romeo and Juliet words from the file and convert to lowercase
    with open(singleline_file, 'r') as f:
        rj_words = [line.strip().lower() for line in f.readlines()]
    
    # Read the chat messages file
    chat_messages = []
    with open(chat_file, 'r') as f:
        chat_messages = [line.strip() for line in f.readlines()]

    # Create a dictionary to store lowercase chat word -> original message mappings
    chat_dict = {}
    for line in chat_messages:
        if "no match found for" in line:
            continue
        parts = line.split(': ', 1)
        if len(parts) == 2:
            message = parts[1].strip()
            chat_dict[message.lower()] = line  # store the full message for easy lookup

    # Create the output file
    with open(output_file, 'w') as out_file:
        for word in rj_words:
            if word in chat_dict:
                out_file.write(f"{chat_dict[word]}\n")
            else:
                out_file.write(f"no match found for {word}\n")

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 4:
        print("Usage: python sort_messages.py <single_line file> <chat_file> <output_file>")
    else:
        singleline_file = sys.argv[1]
        chat_file = sys.argv[2]
        output_file = sys.argv[3]
        generate_xqc_r_and_j(singleline_file, chat_file, output_file)