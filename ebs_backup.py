#!/usr/bin/env python

import boto3
import datetime
from botocore.exceptions import ClientError

region = 'eu-west-3'
backup_tag = 'backup-a'
retention_days = 3
retention_hours = 1
retention = datetime.timedelta(hours = retention_hours)

def lambda_handler(event, context):
    try:
        ec2 = boto3.client('ec2', region_name=region)
    except ClientError as e:
        print e
        exit(1)
    # Job No. 1 (Find volumes and create snapshots for them)
    # Search volumes with tag = $backup_tag"
    print("Looking for volumes for {} (Daily)".format(backup_tag))
    response = ec2.describe_volumes(
        Filters=[
            { 'Name': 'tag:'+backup_tag,
              'Values': ['yes']
            }
        ])
    # For each volume found get its VolumeId and the InstanceId of the instance it is attached to"
    for volume in response['Volumes']:
        vol_id = volume['VolumeId']
        inst_id = volume['Attachments'][0]['InstanceId']
        print("\tFound volume {} attached to instance {}".format(vol_id, inst_id))
        # Get the instance name so we can add it to the snapshot
        instance_resp = ec2.describe_instances(InstanceIds=[inst_id]).get('Reservations');
        for i in instance_resp:
            for t in i['Instances'][0]['Tags']:
                if t['Key'] == 'Name':
                    instance_name = t['Value']

        time_now = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")

        # Create the tags which will be attached to the snapshot
        snapshot_tags = [ 
            {
                'ResourceType': 'snapshot',
                'Tags': [
                    {
                        'Key': 'Name',
                        'Value': instance_name
                    },
                    {
                        'Key': 'Instance_id',
                        'Value': inst_id
                    },
                    {
                        'Key': 'Volume_id',
                        'Value': vol_id
                    },
                    {
                        'Key': backup_tag,
                        'Value': 'yes'
                    },
                    {
                        'Key': 'DateTaken(UTC)',
                        'Value': time_now
                    }
                ]
            }
        ]

        # Create the snapshot for the volume
        snap = ec2.create_snapshot(
           Description='Automatic snapshot',
            VolumeId=vol_id,
            TagSpecifications = snapshot_tags
        )
    # Job No. 1 completed (Find volumes and create snapshots for them)

    # Job No. 2 (Find snapshots older than $retention_days and delete them)
    # Search snapshots with tag = $backup_tag"
    print("Looking for snapshots for {} (Daily) to delete".format(backup_tag))
    response_snap = ec2.describe_snapshots(
        Filters=[
            { 'Name': 'tag:'+backup_tag,
              'Values': ['yes']
            }
        ])
    # For each snapshot found get its SnapshotId and DateTaken value
    for snapshot in response_snap['Snapshots']:
        snap_id = snapshot['SnapshotId']
        for t in snapshot['Tags']:
            if t['Key'] == 'DateTaken(UTC)':
                candidate_time = t['Value']
        if 'candidate_time' in locals():
            # If the snapshot is older than our retention period delete it
            if datetime.datetime.strptime(candidate_time,"%Y-%m-%d-%H-%M") + retention < datetime.datetime.now():
                print("\tFound snapshot {} taken at {}".format(snap_id, candidate_time))
                ec2.delete_snapshot(
                    SnapshotId = snap_id,
                )

if __name__ == "__main__":
    lambda_handler(1,2)
