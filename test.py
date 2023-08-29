import json
import boto3
import logging
import re
import decimal
import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb')


#table_names = ['HH_L', 'Resurgent', 'S_A']

getMethod = 'GET'
postMethod = 'POST'
patchMethod = 'PATCH'
fetchdatapath = '/getdata'
updatedatapath = '/update_data'

def get_table_name():
    dynamodb1 = boto3.resource('dynamodb')
    table_names = dynamodb1.meta.client.list_tables()['TableNames']
    logger.info(f"table_names........{table_names}")
    table_with_file_numbers = []
    
    for table_name in table_names:
        table = dynamodb1.Table(table_name)
        try:
            table.load()
            key_schema = table.key_schema
            
            for key in key_schema:
                if key['KeyType'] == 'HASH' and key['AttributeName'] == 'FileNumber':
                    table_with_file_numbers.append(table_name)
                    break  # Stop checking other keys for this table if 'FileNumber' is found
        except Exception as e:
            logger.error(f"Error describing table {table_name}: {e}")

    logger.info(table_with_file_numbers)
    return table_with_file_numbers


def preprocess_phone_number(phone_number):
    if phone_number:
        # Remove any non-digit characters from the phone number
        return re.sub(r'\D', '', phone_number)

def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return str(obj)
    if isinstance(obj, int):
        return obj
    raise TypeError

def fetch_data_from_dynamodb(table, phone_number):
    # Create dynamodb client
    dynamodb = boto3.resource('dynamodb')
    logger.info('dynamodb client created')

    # Get the dynamodb table
    table = dynamodb.Table(table)

    try:
        # Prepare the query parameters based on the input values
        filter_expression = None
        expression_attribute_values = {}
        expression_attribute_names = {}

        if phone_number:
            # phone_number_updated_number = int(phone_number)
            # filter_expression = '#phone = :phone_number'
            # expression_attribute_names['#phone'] = 'phone_updated'  # Update attribute name here
            # expression_attribute_values[':phone_number'] = phone_number_updated_number
            phone_number_updated_number = int(phone_number)
            filter_expression = '#phone = :phone_number AND #status = :status_value'
            expression_attribute_names['#phone'] = 'phone_updated'  # Update attribute name here
            expression_attribute_values[':phone_number'] = phone_number_updated_number
            expression_attribute_names['#status'] = 'status_of_file'
            expression_attribute_values[':status_value'] = 'open'
            
        logger.info(f"Expression Attribute Value: {expression_attribute_values}")
        

        if filter_expression:
            response = table.scan(
                FilterExpression=filter_expression,
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values
            )
            logger.info(response)
            return response['Items']
        else:
            return None

    except Exception as e:
        print(str(e))
        logger.info(str(e))
        return None

def get_data_type(attribute_value):
    # Helper function to determine the data type based on the Python data type
    if isinstance(attribute_value, str):
        return 'S'  # String
    elif isinstance(attribute_value, int):
        return 'N'  # Number (integer)
    elif isinstance(attribute_value, float):
        return 'N'  # Number (float)
    # Add more data type checks for other supported types (e.g., bool, list, dict) if needed
    else:
        return 'S'  # Default to using string data type if unrecognized data type
    
def get_existing_item(table_name, file_number):
    response = dynamodb.get_item(
        TableName=table_name,
        Key={
            'FileNumber': {'S': file_number}
        }
    )

    return response.get('Item')

def update_attributes(file_number, table_name, request_body):
    update_expression = 'SET '
    expression_attribute_values = {}
    expression_attribute_names = {}
    status_updated = False  # Flag to check if status_of_file attribute has been updated

    for attribute_name, attribute_value in request_body['attributes'].items():
        if attribute_name == 'status_of_file':
            status_updated = True  # Set the flag
        else:
            update_expression += f'#{attribute_name} = :{attribute_name}, '
            expression_attribute_names[f'#{attribute_name}'] = attribute_name

        # Dynamically determine the data type for each attribute value
        data_type = get_data_type(attribute_value)
        if data_type:
            expression_attribute_values[f':{attribute_name}'] = {data_type: attribute_value}
        else:
            logger.warning(f"Invalid data type for attribute '{attribute_name}': {type(attribute_value).__name__}")

    # Update the status_of_file attribute if it hasn't been updated
    if not status_updated:
        update_expression += '#status_of_file = :status_of_file, '
        expression_attribute_names['#status_of_file'] = 'status_of_file'
        expression_attribute_values[':status_of_file'] = {'S': 'close'}
    
    # Automatically update the today_date attribute with today's date
    update_expression += '#today_date = :today_date, '
    expression_attribute_names['#today_date'] = 'today_date'
    expression_attribute_values[':today_date'] = {'S': datetime.datetime.now().strftime('%Y-%m-%d')}

    update_expression = update_expression[:-2]  # Remove the trailing comma and space

    response = dynamodb.update_item(
        TableName=table_name,
        Key={
            'FileNumber': {'S': file_number}
        },
        UpdateExpression=update_expression,
        ExpressionAttributeNames=expression_attribute_names,
        ExpressionAttributeValues=expression_attribute_values
    )

    return response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 200

def lambda_handler(event, context):
    logger.info(event)
    httpMethod = event['httpMethod']
    logger.info(httpMethod)
    path = event['path']
    logger.info(path)
    table_names = get_table_name()
    logger.info(f"table_names with file no {table_names}")
    if httpMethod == getMethod and path == fetchdatapath:
        # Get the POE and clerk phone no from the query parameters
        query_parameters = event['queryStringParameters'] or {}
        phone_number = query_parameters.get('PhoneNumber')
        phone_number_updated = preprocess_phone_number(phone_number)
        logger.info(f"phone_number ..{phone_number_updated}")

        if not phone_number_updated:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'No valid input provided'})
            }

        # List of table names to iterate through
        data = []

        for table_name in table_names:
            result = fetch_data_from_dynamodb(table_name, phone_number=phone_number_updated)
            logger.info(f"Result----{result}")
            if result:
                data.extend(result)

        if data:
            return {
                'statusCode': 200,
                'body': json.dumps(data, default=decimal_default)
            }
        else:
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'No matching data found'})
            }

    elif httpMethod == postMethod and path == updatedatapath:
        try:
            request_body = json.loads(event['body'])  # Parse the JSON string from the event body
            logger.info(f"Received request body: {request_body}")

            if isinstance(request_body, list):  # Check if request_body is a list for multiple updates
                responses = []  # Initialize list to store responses

                for update_item in request_body:
                    try:
                        file_number = update_item.get('file_number')
                        phone_no = update_item.get('phone_no')
                        phone_no = preprocess_phone_number(phone_no)

                        # Initialize a flag to check if the file_number is found in any table
                        file_number_found = False
                        for table in table_names:
                            existing_item = get_existing_item(table, file_number)

                            if existing_item:
                                # Check phone_no match
                                if 'phone_updated' in existing_item:
                                    existing_phone_no = existing_item['phone_updated']['N']
                                    preprocessed_existing_phone_no = preprocess_phone_number(existing_phone_no)
                                    preprocessed_phone_no = preprocess_phone_number(phone_no)

                                    if preprocessed_existing_phone_no == preprocessed_phone_no:
                                        response = update_attributes(file_number, table, update_item)
                                        if response:
                                            responses.append({'status': 'Attributes updated successfully'})
                                        else:
                                            responses.append({'error': 'Error updating attributes'})
                                        file_number_found = True
                                        break
                        if not file_number_found:
                            responses.append({'error': 'File number does not exist'})
                    except Exception as e:
                        logger.error(f"Error updating item: {str(e)}")
                        responses.append({'error': f"Error updating item: {str(e)}"})
                return {
                    'statusCode': 200,
                    'body': json.dumps(responses)
                }
            else:
                # Single entry update handling
                if 'file_number' not in request_body or 'phone_no' not in request_body:
                    logger.error("Invalid request format. Missing file_number or phone_no in the request body.")
                    return {
                        'statusCode': 400,
                        'body': 'Invalid request format. Missing file_number or phone_no in the request body.'
                    }

                file_number = request_body['file_number']
                phone_no = request_body['phone_no']
                phone_no = preprocess_phone_number(phone_no)
                logger.info(phone_no)

                # Initialize a flag to check if the file_number is found in any table
                file_number_found = False
                for table in table_names:
                    existing_item = get_existing_item(table, file_number)

                    if existing_item:
                        # Check phone_no match
                        if 'phone_updated' in existing_item:
                            existing_phone_no = existing_item['phone_updated']['N']
                            preprocessed_existing_phone_no = preprocess_phone_number(existing_phone_no)
                            preprocessed_phone_no = preprocess_phone_number(phone_no)

                            if preprocessed_existing_phone_no == preprocessed_phone_no:
                                response = update_attributes(file_number, table, request_body)
                                if response:
                                    logger.info("Attributes updated successfully")
                                    return {
                                        'statusCode': 200,
                                        'body': 'Attributes updated successfully'
                                    }
                                else:
                                    logger.error("Error updating attributes")
                                    return {
                                        'statusCode': 500,
                                        'body': 'Error updating attributes'
                                    }
                logger.error('File number not found')
                return {
                    'statusCode': 404,
                    'body': 'File number does not exist'
                }
        except ValueError:
            logger.error("Invalid JSON format in the request body")
            return {
                'statusCode': 400,
                'body': 'Invalid JSON format in the request body'
            }
