import json
import uuid
import boto3
import re
from botocore.exceptions import ClientError
import traceback

# DynamoDB and S3 Clients
dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")

# DynamoDB table
folders_table = dynamodb.Table("ArchManageFolders")

# S3 Bucket Prefix
S3_BUCKET_PREFIX = "folder-"


class CustomException(Exception):
    """Base class for all custom exceptions."""

    def __init__(self, message, status_code=500):
        self.message = message
        self.status_code = status_code
        super().__init__(self.message)


class FolderAlreadyExistsError(CustomException):
    def __init__(self, folder_name):
        super().__init__(f"Folder {folder_name} already exists.", 409)


class FolderNotFoundError(CustomException):
    def __init__(self, folder_id):
        super().__init__(f"Folder with ID {folder_id} not found.", 404)


class FolderPermissionError(CustomException):
    def __init__(self):
        super().__init__(
            "You do not have permission to access or modify this folder.", 403
        )


class InvalidBucketNameError(CustomException):
    def __init__(self, bucket_name):
        super().__init__(f"Invalid S3 bucket name: {bucket_name}.", 400)


class FolderNameRequiredError(CustomException):
    def __init__(self):
        super().__init__("Folder name is required.", 400)


class InternalServerError(CustomException):
    def __init__(self, message="Internal server error"):
        super().__init__(message, 500)


class InvalidPayload(CustomException):
    def __init__(self, message="Invalid payload"):
        super().__init__(message, 400)


def build_response(status_code, body=None, binary=False):
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Credentials": True,
        "Access-Control-Allow-Methods": "OPTIONS,POST,PUT,DELETE",
        "Access-Control-Allow-Headers": "Content-Type,auth",
    }

    if not binary and body is not None:
        try:
            body = json.dumps(body)
        except Exception as e:
            raise InvalidPayload()
    elif binary:
        headers["Content-Type"] = "audio/mp3"
        body = body

    return {"statusCode": status_code, "body": body, "headers": headers}


def folder_exists_in_s3(folder_id, user_id):
    bucket_name = f"{folder_id}-{user_id}"  # Now using folder_id and user_id
    try:
        s3.head_bucket(Bucket=bucket_name)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise e


def sanitize_bucket_name(bucket_name):
    # Convert to lowercase
    bucket_name = bucket_name.lower()

    # Replace spaces with hyphens
    bucket_name = bucket_name.replace(" ", "-")

    # Remove any invalid characters (only allow lowercase letters, numbers, and hyphens)
    bucket_name = re.sub(r"[^a-z0-9\-]", "", bucket_name)

    # Ensure it doesn't start or end with a hyphen
    bucket_name = bucket_name.strip("-")

    # Ensure it's within the length limit (3 to 63 characters)
    if len(bucket_name) < 3 or len(bucket_name) > 63:
        raise InvalidBucketNameError(bucket_name)

    return bucket_name


def create_folder_in_s3(folder_name, folder_id, user_id):
    # Sanitize the folder name to create a valid S3 bucket name
    bucket_name = sanitize_bucket_name(
        f"{folder_id}-{user_id}"
    )  # Use folder_id and user_id for the bucket name

    if folder_exists_in_s3(folder_id, user_id):
        raise FolderAlreadyExistsError(bucket_name)

    # Create the S3 bucket with the sanitized name
    s3.create_bucket(Bucket=bucket_name)

    return bucket_name


def create_folder_in_dynamodb(folder_name, user_id, folder_parent, folder_type):
    # Check if a folder with the same name already exists for the given user
    existing_folder = folders_table.scan(
        FilterExpression="#owner = :owner and #name = :name",
        ExpressionAttributeNames={"#owner": "owner", "#name": "name"},
        ExpressionAttributeValues={":owner": user_id, ":name": folder_name},
    )

    if existing_folder.get("Items"):
        raise FolderAlreadyExistsError(folder_name)

    # If folder doesn't exist, create a new folder
    folder_id = str(uuid.uuid4())
    folder_data = {
        "id": folder_id,
        "owner": user_id,
        "name": folder_name,
        "sub_folders": [],
        "type": folder_type,
        "required_files": [],
        "files": {},
    }

    # If folder_parent exists, update the parent folder's sub_folders list
    if folder_parent:
        parent_folder = folders_table.get_item(Key={"id": folder_parent})
        if "Item" not in parent_folder:
            raise FolderNotFoundError(folder_parent)
        parent_folder = parent_folder["Item"]
        parent_folder["sub_folders"].append(folder_id)
        folders_table.update_item(
            Key={"id": folder_parent},
            UpdateExpression="SET sub_folders = :sub_folders",
            ExpressionAttributeValues={":sub_folders": parent_folder["sub_folders"]},
        )

    # Create folder entry in DynamoDB
    folders_table.put_item(Item=folder_data)

    return (
        folder_data,
        folder_id,
    )  # Return folder data and folder_id for further reference


def delete_folder_from_s3(folder_id, user_id):
    bucket_name = f"{folder_id}-{user_id}"
    try:
        s3.delete_bucket(Bucket=bucket_name)
    except ClientError as e:
        raise Exception(f"Error deleting S3 bucket {bucket_name}: {e}")


def delete_folder_from_dynamodb(folder_id):
    response = folders_table.get_item(Key={"id": folder_id})
    if "Item" not in response:
        raise FolderNotFoundError(folder_id)

    folder_data = response["Item"]
    # Remove folder ID from parent folder's sub_folders list
    if folder_data.get("sub_folders"):
        for sub_folder_id in folder_data["sub_folders"]:
            sub_folder = folders_table.get_item(Key={"id": sub_folder_id})["Item"]
            sub_folder["sub_folders"].remove(folder_id)
            folders_table.update_item(
                Key={"id": sub_folder_id},
                UpdateExpression="SET sub_folders = :sub_folders",
                ExpressionAttributeValues={":sub_folders": sub_folder["sub_folders"]},
            )

    folders_table.delete_item(Key={"id": folder_id})


def get_folder_by_id(folder_id, user_id):
    # Retrieve the folder from DynamoDB by folder_id
    response = folders_table.get_item(Key={"id": folder_id})

    # Check if folder exists
    folder_data = response.get("Item")
    if not folder_data:
        return None  # Folder does not exist

    # Check if the folder owner matches the user_id
    if folder_data.get("owner") != user_id:
        raise FolderPermissionError()

    return folder_data


def get_all_folders(user_id):
    try:
        # Perform a scan operation to find all folders that belong to this user
        response = folders_table.scan(
            FilterExpression="#owner = :owner",
            ExpressionAttributeNames={
                "#owner": "owner",
            },
            ExpressionAttributeValues={":owner": user_id},
        )

        folders = response.get("Items", [])

        # Check for pagination and retrieve additional folders if necessary
        while "LastEvaluatedKey" in response:
            response = folders_table.scan(
                FilterExpression="#owner = :owner",
                ExpressionAttributeNames={"#owner": "owner", "#name": "name"},
                ExpressionAttributeValues={":owner": user_id},
                ExclusiveStartKey=response["LastEvaluatedKey"],
            )
            folders.extend(response.get("Items", []))

        return folders

    except ClientError as e:
        raise Exception(f"Error retrieving folders from DynamoDB: {e}")


def update_folder_in_dynamodb(
    folder_id,
    folder_name,
    folder_type=None,
    required_files=None,
    sub_folders=None,
    user_id=None,
):
    # Retrieve the existing folder from DynamoDB
    response = folders_table.get_item(Key={"id": folder_id})

    if "Item" not in response:
        raise FolderNotFoundError(folder_id)

    folder_data = response["Item"]

    # Verify if the user is the owner of the folder
    if folder_data["owner"] != user_id:
        raise FolderPermissionError()

    # Update folder name if provided
    update_expression_parts = []
    expression_attribute_values = {}
    expression_attribute_names = {}

    if folder_name:
        folder_data["name"] = folder_name
        update_expression_parts.append("SET #name = :name")
        expression_attribute_values[":name"] = folder_name
        expression_attribute_names["#name"] = "name"

    # Update folder type if provided
    if folder_type:
        folder_data["type"] = folder_type
        update_expression_parts.append("SET #type = :type")
        expression_attribute_values[":type"] = folder_type
        expression_attribute_names["#type"] = "type"

    # Update required files if provided
    if required_files is not None:
        folder_data["required_files"] = required_files
        update_expression_parts.append("SET #required_files = :required_files")
        expression_attribute_values[":required_files"] = required_files
        expression_attribute_names["#required_files"] = "required_files"

    # Update sub_folders if provided
    if sub_folders is not None:
        folder_data["sub_folders"] = sub_folders
        update_expression_parts.append("SET #sub_folders = :sub_folders")
        expression_attribute_values[":sub_folders"] = sub_folders
        expression_attribute_names["#sub_folders"] = "sub_folders"

    # Update the folder entry in DynamoDB
    if update_expression_parts:
        update_expression = ",".join(update_expression_parts)
        folders_table.update_item(
            Key={"id": folder_id},
            UpdateExpression=update_expression,
            ExpressionAttributeNames=expression_attribute_names,
            ExpressionAttributeValues=expression_attribute_values,
        )

    return folder_data


def main(event, context):
    try:
        http_method = event.get("httpMethod")
        if http_method == "OPTIONS":
            return build_response(200)

        user_id = event["headers"].get("auth")

        if not user_id:
            return build_response(
                400, {"error": f"User ID is missing in headers {event['headers']}"}
            )

        if http_method == "POST":
            # Create Folder (POST)
            body = json.loads(event["body"])
            folder_name = body.get("folder_name")
            folder_parent = body.get("folder_parent")
            folder_type = body.get("type", "default")

            if not folder_name:
                raise FolderNameRequiredError()

            folder_data, folder_id = create_folder_in_dynamodb(
                folder_name, user_id, folder_parent, folder_type
            )

            create_folder_in_s3(folder_name, folder_id, user_id)

            return build_response(201, folder_data)

        elif http_method == "GET":
            parameters = event.get("pathParameters", {})
            folder_id = None
            if parameters:
                folder_id = parameters.get("folder_id")

            if folder_id:
                folder_data = get_folder_by_id(folder_id, user_id)
                if folder_data:
                    return build_response(200, folder_data)
                else:
                    raise FolderNotFoundError(folder_id)
            else:
                folders = get_all_folders(user_id)
                return build_response(200, folders)

        elif http_method == "PUT":
            folder_id = event.get("pathParameters", {}).get("folder_id")
            if not folder_id:
                raise FolderNotFoundError(folder_id)

            body = json.loads(event["body"])
            folder_name = body.get("folder_name")
            folder_type = body.get("type")
            required_files = body.get("required_files")
            sub_folders = body.get("sub_folders")

            folder_data = update_folder_in_dynamodb(
                folder_id,
                folder_name,
                folder_type,
                required_files,
                sub_folders,
                user_id,
            )

            return build_response(200, folder_data)

        elif http_method == "DELETE":
            folder_id = event.get("pathParameters", {}).get("folder_id")
            if not folder_id:
                raise FolderNotFoundError(folder_id)

            folder_data = get_folder_by_id(folder_id, user_id)
            if not folder_data:
                raise FolderNotFoundError(folder_id)

            delete_folder_from_s3(folder_id, user_id)
            delete_folder_from_dynamodb(folder_id)

            return build_response(200, {"message": "Folder deleted successfully"})

        else:
            return build_response(405, {"error": "Method Not Allowed"})

    except CustomException as e:
        return build_response(e.status_code, {"error": e.message})

    except Exception as e:
        error_details = traceback.format_exc()
        return build_response(
            500, {"error": f"Internal server error e:{e}\n details: {error_details}"}
        )
