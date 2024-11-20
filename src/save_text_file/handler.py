import json
import boto3
import os
from datetime import datetime

# Initialize S3 client
s3_client = boto3.client('s3')
bucket_name = os.environ['S3_BUCKET_NAME']

def save_file(event, context):
    if event.get("httpMethod") == "OPTIONS":
        return build_response(200)

    # Parse the incoming JSON event
    body = json.loads(event['body'])
    
    # Retrieve the content and filename from the request body
    file_content = body.get('file_content')
    file_name = body.get('file_name', f'{datetime.now().strftime("%Y%m%d%H%M%S")}.txt')
    
    if not file_content:
        return build_response(400, {'error': 'file_content is required'})

    # Save the content to S3 as a .txt file
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=file_content,
            ContentType='text/plain'
        )
        return build_response(200, {'message': f'File {file_name} saved successfully'})
    except Exception as e:
        return build_response(500, {'error': str(e)})

def build_response(status_code, body=None, binary=False):
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Credentials": True,
        "Access-Control-Allow-Methods": "OPTIONS,POST",
        "Access-Control-Allow-Headers": "Content-Type,api-token",
    }

    if not binary and body is not None:
        body = json.dumps(body)
    elif binary:
        headers["Content-Type"] = "audio/mp3"
        body = body

    return {"statusCode": status_code, "body": body, "headers": headers}
