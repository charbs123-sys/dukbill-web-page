import main
import json
import os
import boto3
sqs = boto3.client('sqs')
NEXT_QUEUE_URL = os.environ.get('NEXT_QUEUE_URL')
NEXT_QUEUE_IS_FIFO = False

# Set environment variables to use the layer
os.environ['PATH'] = f"{os.environ.get('PATH', '')}:/opt/bin"
os.environ['LD_LIBRARY_PATH'] = f"{os.environ.get('LD_LIBRARY_PATH', '')}:/opt/lib"
os.environ['TESSDATA_PREFIX'] = '/opt/tesseract/share/tessdata'



import pytesseract
from PIL import Image, ImageDraw, ImageFont
pytesseract.pytesseract.tesseract_cmd = '/opt/bin/tesseract'


import os
import subprocess
import json

def lambda_handler(event, context):
    try:
        # Create a simple test image with text
        img = Image.new('RGB', (300, 100), color='white')
        d = ImageDraw.Draw(img)
        d.text((10, 40), "123456", fill='black')
        
        # Perform OCR
        text = pytesseract.image_to_string(img, config='--psm 6')
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'success': True,
                'extracted_text': text.strip(),
                'message': 'OCR is working!'
            })
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'success': False,
                'error': str(e)
            })
        }

'''
def lambda_handler(event, context):

    for message in event['Records']:
        user_email = process_message(message)

        main.handle_new_entry_broker(user_email)

        break
'''
def process_message(message):
    try:
        print(f"Processing message: {message}")
        
        # Parse the JSON string in the body
        body = json.loads(message['body'])
        
        # Now access the parsed dictionary
        user_email = body['user_key']
        
        return user_email
    except Exception as err:
        print(f"An error occurred: {str(err)}")
        print(f"Message structure: {message}")
        raise err