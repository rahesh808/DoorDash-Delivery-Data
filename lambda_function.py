import pandas as pd
import boto3

def lambda_handler(event, context):
    # Extract bucket and file path from the event
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']
    
    # Read JSON file into DataFrame
    s3 = boto3.client('s3')
    try:
        obj = s3.get_object(Bucket=source_bucket, Key=file_key)
        df = pd.read_json(obj['Body'])
        print(df)
    except Exception as e:
        # Handle exception and publish failure message to SNS topic
        publish_sns_message("Error reading JSON file: " + str(e))
        return {
            'statusCode': 500,
            'body': 'Error reading JSON file'
        }
    
    # Filter records where status is "delivered"
    filtered_df = df[df['status'] == 'delivered']
    
    # Write filtered DataFrame to a new JSON file
    target_bucket = 'doordash-target-zn-rahesh'
    target_key = 'filtered_data.json'
    try:
        filtered_json = filtered_df.to_json(orient='records', lines=True)
        s3.put_object(Bucket=target_bucket, Key=target_key, Body=filtered_json)
    except Exception as e:
        # Handle exception and publish failure message to SNS topic
        publish_sns_message("Error writing filtered JSON file: " + str(e))
        return {
            'statusCode': 500,
            'body': 'Error writing filtered JSON file'
        }
    
    # Publish success message to SNS topic
    publish_sns_message("Data processing completed successfully.")
    
    return {
        'statusCode': 200,
        'body': 'Data processing completed successfully.'
    }




def publish_sns_message(message):
    sns_topic_arn = "arn:aws:sns:us-east-1:455296827336:doordash-delievery-sns"
    sns_client = boto3.client('sns')
    sns_client.publish(
        TopicArn=sns_topic_arn,
        Message=message
    )
