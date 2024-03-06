
import boto3
import base64
from botocore.exceptions import ClientError, ParamValidationError, WaiterConfigError, WaiterError
import json
import pandas as pd
import numpy as np
import datetime
from datetime import timedelta
import os
import loguru
from loguru import logger
import re
import io
import yaml

# Open and read the YAML file
with open('backfill/config.yml', 'r') as file:
    config = yaml.safe_load(file)

backfill_start_date = config['backfill_start_date']
backfill_end_date = config['backfill_end_date']

# -------------------------------------
# Variables
# -------------------------------------

REGION = 'us-east-1'

AWS_ACCESS_KEY_ID=os.environ['AWS_ACCESS_KEY']
AWS_SECRET_ACCESS_KEY=os.environ['AWS_ACCESS_SECRET']

start_time = datetime.datetime.now()
logger.info(f'Start time: {start_time}')

# -------------------------------------
# Functions
# -------------------------------------


# FUNCTION TO EXECUTE ATHENA QUERY AND RETURN RESULTS
# ----------

def run_athena_query(query:str, database: str, region:str):

        
    # Initialize Athena client
    athena_client = boto3.client('athena', 
                                 region_name=region,
                                 aws_access_key_id=AWS_ACCESS_KEY_ID,
                                 aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    # Execute the query
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': database
            },
            ResultConfiguration={
                'OutputLocation': 's3://prymal-ops/athena_query_results/'  # Specify your S3 bucket for query results
            }
        )

        query_execution_id = response['QueryExecutionId']

        # Wait for the query to complete
        state = 'RUNNING'

        while (state in ['RUNNING', 'QUEUED']):
            response = athena_client.get_query_execution(QueryExecutionId = query_execution_id)
            if 'QueryExecution' in response and 'Status' in response['QueryExecution'] and 'State' in response['QueryExecution']['Status']:
                # Get currentstate
                state = response['QueryExecution']['Status']['State']

                if state == 'FAILED':
                    logger.error('Query Failed!')
                elif state == 'SUCCEEDED':
                    logger.info('Query Succeeded!')
            

        # OBTAIN DATA

        # --------------



        query_results = athena_client.get_query_results(QueryExecutionId=query_execution_id,
                                                MaxResults= 1000)
        


        # Extract qury result column names into a list  

        cols = query_results['ResultSet']['ResultSetMetadata']['ColumnInfo']
        col_names = [col['Name'] for col in cols]



        # Extract query result data rows
        data_rows = query_results['ResultSet']['Rows'][1:]



        # Convert data rows into a list of lists
        query_results_data = [[r['VarCharValue'] if 'VarCharValue' in r else np.NaN for r in row['Data']] for row in data_rows]



        # Paginate Results if necessary
        while 'NextToken' in query_results:
                query_results = athena_client.get_query_results(QueryExecutionId=query_execution_id,
                                                NextToken=query_results['NextToken'],
                                                MaxResults= 1000)



                # Extract quuery result data rows
                data_rows = query_results['ResultSet']['Rows'][1:]


                # Convert data rows into a list of lists
                query_results_data.extend([[r['VarCharValue'] if 'VarCharValue' in r else np.NaN for r in row['Data']] for row in data_rows])



        results_df = pd.DataFrame(query_results_data, columns = col_names)
        
        return results_df


    except ParamValidationError as e:
        logger.error(f"Validation Error (potential SQL query issue): {e}")
        # Handle invalid parameters in the request, such as an invalid SQL query

    except WaiterError as e:
        logger.error(f"Waiter Error: {e}")
        # Handle errors related to waiting for query execution

    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        
        if error_code == 'InvalidRequestException':
            logger.error(f"Invalid Request Exception: {error_message}")
            # Handle issues with the Athena request, such as invalid SQL syntax
            
        elif error_code == 'ResourceNotFoundException':
            logger.error(f"Resource Not Found Exception: {error_message}")
            # Handle cases where the database or query execution does not exist
            
        elif error_code == 'AccessDeniedException':
            logger.error(f"Access Denied Exception: {error_message}")
            # Handle cases where the IAM role does not have sufficient permissions
            
        else:
            logger.error(f"Athena Error: {error_code} - {error_message}")
            # Handle other Athena-related errors

    except Exception as e:
        logger.error(f"Other Exception: {str(e)}")
        # Handle any other unexpected exceptions


# Check S3 Path for Existing Data
# -----------

def check_path_for_objects(bucket: str, s3_prefix:str):

  logger.info(f'Checking for existing data in {bucket}/{s3_prefix}')

  # Create s3 client
  s3_client = boto3.client('s3', 
                          region_name = REGION,
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

  # List objects in s3_prefix
  result = s3_client.list_objects_v2(Bucket=bucket, Prefix=s3_prefix )

  # Instantiate objects_exist
  objects_exist=False

  # Set objects_exist to true if objects are in prefix
  if 'Contents' in result:
      objects_exist=True

      logger.info('Data already exists!')

  return objects_exist

# Delete Existing Data from S3 Path
# -----------

def delete_s3_prefix_data(bucket:str, s3_prefix:str):


  logger.info(f'Deleting existing data from {bucket}/{s3_prefix}')

  # Create an S3 client
  s3_client = boto3.client('s3', 
                          region_name = REGION,
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

  # Use list_objects_v2 to list all objects within the specified prefix
  objects_to_delete = s3_client.list_objects_v2(Bucket=bucket, Prefix=s3_prefix)

  # Extract the list of object keys
  keys_to_delete = [obj['Key'] for obj in objects_to_delete.get('Contents', [])]

  # Check if there are objects to delete
  if keys_to_delete:
      # Delete the objects using 'delete_objects'
      response = s3_client.delete_objects(
          Bucket=bucket,
          Delete={'Objects': [{'Key': key} for key in keys_to_delete]}
      )
      logger.info(f"Deleted {len(keys_to_delete)} objects")
  else:
      logger.info("No objects to delete")

# ========================================================================
# Execute Code
# ========================================================================

DATABASE = 'prymal'
REGION = 'us-east-1'

# QUERY DATA  =======================================

# Construct query to pull data by product
# ----

QUERY = f"""
            with line_item AS (
            SELECT *
            , DATE(CONCAT(year,'-',month,'-',day)) as partition_date
            FROM "prymal"."shopify_line_items" 
            )

            SELECT * 
            FROM line_item
            WHERE partition_date >= DATE('{backfill_start_date}')
            AND partition_date <= DATE('{backfill_end_date}')
            

            """

# Query datalake to get quantiy sold per sku for the last 120 days
# ----

result_df = run_athena_query(query=QUERY, database=DATABASE, region=REGION)


# ADD NEW FEATURES =======================================

logger.info('adding features')


# Calculate product revenue for each line item
result_df['product_rev'] = result_df['price'].astype(float) * result_df['quantity'].astype(float)

# Create order month
result_df['order_date'] = result_df['order_date'].apply(lambda x: x[:10]).copy()
result_df['month'] = pd.to_datetime(result_df['order_date']).dt.strftime('%Y-%m')


# AGGREGATE DATA  =======================================

logger.info('agregating to one record per order')

# Consolidate into one record per order
orders_df = result_df.groupby(['order_id','email'],as_index=False)['order_date'].min()
orders_df.columns = ['order_id','email','order_date']

order_rev = result_df.groupby(['order_id'],as_index=False)['product_rev'].sum()
order_rev.columns = ['order_id','product_rev']

orders_df = orders_df.merge(order_rev,on='order_id',how='left')

orders_df['channel'] = 'Shopify'

# WRITE TO S3 =======================================

logger.info('configuring to write to s3')

# Create s3 client
s3_client = boto3.client('s3', 
                          region_name = REGION,
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY
                          )

# Set bucket
BUCKET = os.environ['S3_PRYMAL_ANALYTICS']

current_date = pd.to_datetime(backfill_start_date)

while  pd.to_datetime(current_date) <= pd.to_datetime(backfill_end_date):

    logger.info(current_date)


    daily_orders = orders_df.loc[orders_df['order_date']==pd.to_datetime(current_date).strftime('%Y-%m-%d')]

    # Log number of rows
    logger.info(f'{len(daily_orders)} rows in orders_df for {current_date}')

    # Configure S3 Prefix
    S3_PREFIX_PATH = f"order_log/partition_date={current_date}/order_log_{current_date}.csv"

    # Check if data already exists for this partition
    data_already_exists = check_path_for_objects(bucket=BUCKET, s3_prefix=S3_PREFIX_PATH)

    # If data already exists, delete it .. 
    if data_already_exists == True:
        
        # Delete data 
        delete_s3_prefix_data(bucket=BUCKET, s3_prefix=S3_PREFIX_PATH)


    logger.info(f'Writing to {S3_PREFIX_PATH}')


    with io.StringIO() as csv_buffer:
        orders_df.to_csv(csv_buffer, index=False)

        response = s3_client.put_object(
            Bucket=BUCKET, 
            Key=S3_PREFIX_PATH, 
            Body=csv_buffer.getvalue()
        )

        status = response['ResponseMetadata']['HTTPStatusCode']

        if status == 200:
            logger.info(f"Successful S3 put_object response for PUT ({S3_PREFIX_PATH}). Status - {status}")
        else:
            logger.error(f"Unsuccessful S3 put_object response for PUT ({S3_PREFIX_PATH}. Status - {status}")


    # Increment by 1 day
    current_date = pd.to_datetime(current_date) + timedelta(1)
