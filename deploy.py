import subprocess
import boto3
import os

BUCKET_NAME = "archmanage"
REGION = "us-east-1"
STACK_NAME = "archmanage"
#LAYER_NAME = "YouTubeAudioDependencies"
#LAYER_PACKAGE_NAME = "layer.zip"

s3_client = boto3.client("s3", region_name=REGION)
lambda_client = boto3.client("lambda", region_name=REGION)


def bucket_exists(bucket_name):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        return True
    except:
        return False


def create_bucket(bucket_name, region):
    s3_client.create_bucket(
        Bucket=bucket_name,
    )

'''
def layer_exists(layer_name):
    layers = lambda_client.list_layers()
    for layer in layers.get("Layers", []):
        if layer_name == layer["LayerName"]:
            return True
    return False

def prepare_and_upload_layer(bucket_name):
    # Assuming you're in the root directory of your SAM app
    os.system(f"pip install requests==2.28.2 -t python/lib/python3.10/site-packages/")
    os.system(f"pip install openai -t python/lib/python3.10/site-packages/")
    os.system(f"pip install langdetect -t python/lib/python3.10/site-packages/")

    os.system(f"zip -r {LAYER_PACKAGE_NAME} python/")
    os.system(f"rm -rf python/")

    s3_client.upload_file(LAYER_PACKAGE_NAME, bucket_name, LAYER_PACKAGE_NAME)

    os.remove(LAYER_PACKAGE_NAME)
    return f"s3://{bucket_name}/{LAYER_PACKAGE_NAME}"

def create_layer(s3_uri, layer_name):
    lambda_client.publish_layer_version(
        LayerName=layer_name,
        Description="OpenAI Layer",
        Content={"S3Bucket": BUCKET_NAME, "S3Key": LAYER_PACKAGE_NAME},
        CompatibleRuntimes=["python3.10"],
    )
'''

def package_and_deploy(bucket_name, region, stack_name):
    subprocess.check_call(
        [
            "sam",
            "package",
            "--output-template-file",
            "packaged.yaml",
            "--s3-bucket",
            bucket_name,
        ]
    )

    subprocess.check_call(
        [
            "sam",
            "deploy",
            "--template-file",
            "packaged.yaml",
            "--region",
            region,
            "--capabilities",
            "CAPABILITY_IAM",
            "--stack-name",
            stack_name,
        ]
    )


if __name__ == "__main__":
    '''
    if not layer_exists(LAYER_NAME):
        print(f"Layer {LAYER_NAME} does not exist. Preparing and uploading...")
        s3_uri = prepare_and_upload_layer(BUCKET_NAME)
        create_layer(s3_uri, LAYER_NAME)
        print(f"Layer {LAYER_NAME} created and uploaded.")
    else:
        print(f"Layer {LAYER_NAME} already exists.")
    '''
    
    if not bucket_exists(BUCKET_NAME):
        print(f"Bucket {BUCKET_NAME} does not exist. Creating it...")
        create_bucket(BUCKET_NAME, REGION)
        print(f"Bucket {BUCKET_NAME} created.")
    else:
        print(f"Bucket {BUCKET_NAME} already exists.")

    print("Packaging and deploying the service...")
    package_and_deploy(BUCKET_NAME, REGION, STACK_NAME)
    print("Deployment completed.")
