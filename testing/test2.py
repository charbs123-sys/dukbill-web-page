import boto3

client = boto3.client('elasticache', region_name='ap-southeast-2')

response = client.describe_replication_groups(
    ReplicationGroupId='dukbill-simple-cache'
)

status = response['ReplicationGroups'][0]['Status']
print(f"Status: {status}")

if status == 'available':
    endpoint = response['ReplicationGroups'][0]['NodeGroups'][0]['PrimaryEndpoint']
    print(f"\n✓ Cluster is ready!")
    print(f"Endpoint: {endpoint['Address']}:{endpoint['Port']}")
else:
    print(f"Still creating... check again in a few minutes")