import os
import boto3
import urllib.parse
import json
import logging

# Initialize S3 client
s3 = boto3.client('s3')
sns = boto3.client('sns')

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Set your destination bucket
DESTINATION_BUCKET_POSTFIX = os.getenv('DESTINATION_BUCKET_POSTFIX')
SNS_TOPIC_ARN = os.getenv('SNS_TOPIC_ARN')




def lambda_handler(event, context):
    logger.info("New S3 event received:")
    logger.info(json.dumps(event, indent=2))

    deleted_object_messages = []

    # Iterate over all records (in case of batch events)
    for record in event.get('Records', []):
        event_name = record['eventName']
        source_bucket = record['s3']['bucket']['name']
        destination_bucket = f'{source_bucket}-{DESTINATION_BUCKET_POSTFIX}'
        object_key = urllib.parse.unquote_plus(record['s3']['object']['key'])
        event_time = record['eventTime']
        user_identity = record.get("userIdentity", {}).get("principalId", "Unknown")
        source_ip = record.get("requestParameters", {}).get("sourceIPAddress", "Unknown")
        version_id = record['s3']['object'].get('versionId', 'N/A')

        logger.info(f"Event time: {event_time}")
        logger.info(f"Source bucket: {source_bucket}")
        logger.info(f"Object key: {object_key}")
        logger.info(f"Event name: {event_name}")
        logger.info(f"Version Id: {version_id}")
        logger.info(f"user Identity: {user_identity}")
        logger.info(f"Source Ip: {source_ip}")

        # Check what type of event it is
        if event_name.startswith("ObjectCreated:"):
            try:
                # Copy to backup bucket
                s3.copy_object(
                    CopySource={'Bucket': source_bucket, 'Key': object_key},
                    Bucket=destination_bucket,
                    Key=object_key
                )
                logger.info(f"File replicated to backup bucket: {destination_bucket}/{object_key}")
            except Exception as e:
                logger.error(f"Failed to replicate {object_key}: {e}")
                raise e

        elif event_name.startswith("ObjectRemoved:"):
            # Just log the deletion — no replication
            logger.info(f"Object deleted from source: {object_key}. No action taken.")
            delete_marker_flag = "Delete Marker Created" if event_name == "ObjectRemoved:DeleteMarkerCreated" else "Permanent Delete"
            deletion_message = (
                f"Object deleted from S3 bucket!\n"
                f"- Bucket: {source_bucket}\n"
                f"- Key: {object_key}\n"
                f"- VersionId: {version_id}\n"
                f"- Deleted Flag: {delete_marker_flag}\n"
                f"- Deleted at: {event_time}\n"
                f"- Actor (principalId): {user_identity}\n"
                f"- Source IP: {source_ip}\n\n"
            )
            deleted_object_messages.append(deletion_message)


        else:
            logger.info(f"Ignored event type: {event_name}")

    # Send one SNS email per batch if any objects were deleted
    if deleted_object_messages:
        try:
            message_body = "\n".join(deleted_object_messages)
            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject=f"S3 Object Deletion Notification ({len(deleted_object_messages)} objects)",
                Message=message_body
            )
            logger.info(f"SNS notification sent for {len(deleted_object_messages)} deleted object(s).")
        except Exception as e:
            logger.error(f"Failed to send SNS notification: {e}")
            raise e
    else:
        logger.info("No deletions detected in this batch.")

    return {"statusCode": 200, "body": "Event processed successfully"}
