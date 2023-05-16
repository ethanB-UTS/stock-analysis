# stock-analysis
Repository containing python code for the AWS lambda and MySql queries for the ELT process.

The lambda_function.py contains the python code and MySql queries.
Each function has comments explaining the purpose of the function.
The lambda runs the main lambda_handler() function when an event triggers the lambda.

## A highlevel overview of the code:
1. Event triggers lambda_handler()
2. The ticker codes of the top 200 stocks are retrieved via a file in an S3 location
3. All the relevent stock data of the top 200 stocks is extracted via yfinace API
4. The relevent stock data is loaded into the MySql databases landingTable
5. MySql queries are then exectued via python to load the other tables in the database
6. The queries execute the calculations
7. The order of queries is structured so tables are only loaded once their parent tables are loaded.

## How to upload the python file to AWS lambda:
* Zip the lambda_function.py file with NO parent folder
* Upload the zip file to the AWS lambda via the AWS console or to S3 via AWS CLI
* The lambda contains layers with all the required packages so no additional files are required in the zip file
