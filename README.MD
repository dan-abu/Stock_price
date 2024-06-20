# DAILY STOCK PRICE EXTRACTOR


The code in this repository will allow you to extract the daily COP position from Yahoo Finance and Alpha Vantage.
This script could be easily implemented and scheduled via a tool such as AWS Glue.


### Requirements:
- S3 bucket to store the data
- Technical dependencies as per the `requirements.txt` file
- The symbol/ticker of the stock you'd like to follow (e.g. Nvidia = NVDA)
- Capacity to execute the script via the command line (e.g. python3 ./src/main.py <ticker> <api_key> <s3_uri> <s3_key>)


### Considerations:
- Both Yahoo Finance and Alpha Vantage release the previous day's COP data in the afternoon/evening (UK time).
This means that if you do choose to implement and automate this code, then you should schedule it for a time in the
afternoon/evening.
- If you schedule this code to run daily, it might be worth considering updating the code so that it includes
partitions within the S3 bucket. Without this, you will eventually end up with a large folder that is slow to query.