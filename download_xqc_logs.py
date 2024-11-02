import requests
from datetime import datetime, timedelta
import concurrent.futures
from dateutil.relativedelta import relativedelta
import multiprocessing

# Set the parameters
channel = "xqc"
channel_id_type = "channel"  # Could be "channelid" if needed
start_date = "2019-04-23"
end_date = "2024-10-07"
ndjson_format = False  # Change to True if you want newline-delimited JSON format

# Convert the start and end dates to datetime objects
start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
end_datetime = datetime.strptime(end_date, "%Y-%m-%d")

# Create a file to store all the logs
with open('all_logs.json', 'w') as all_logs_file:
    start_month = start_datetime.replace(day=1)
    end_month = end_datetime.replace(day=1)
    months = [start_month + relativedelta(months=i) for i in range((end_month.year - start_month.year) * 12 + end_month.month - start_month.month + 1)]
    
    # Define the URL and parameters for the request
    url = f"https://logs.ivr.fi/{channel_id_type}/{channel}"
    
    # Create a ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        # Define a function to make a request for a specific month
        def fetch_logs(current_datetime):
            # Format the 'from' and 'to' dates for the current month
            from_date = current_datetime.strftime("%Y-%m-%dT00:00:00Z")
            next_month = current_datetime + relativedelta(months=1)
            to_date = (next_month - timedelta(seconds=1)).strftime("%Y-%m-%dT23:59:59Z")
            
            params = {
                'from': from_date,
                'to': to_date,
                'ndjson': ndjson_format
            }
            
            # Make the request
            response = requests.get(url, params=params)
            
            # Check if the request was successful
            if response.status_code == 200:
                # Write the logs to the file
                with multiprocessing.Lock():
                    all_logs_file.write(response.text + '\n')
                print(f"Successfully downloaded logs for {from_date}")
            else:
                print(f"Failed to retrieve logs for {from_date}: {response.status_code} - {response.text}")
        
        # Use the executor to make requests for all months in parallel
        executor.map(fetch_logs, months)

print("Download completed.")