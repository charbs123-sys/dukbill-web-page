import boto3
import logging

logging.basicConfig(level=logging.INFO)
client = boto3.client('elasticache', region_name='ap-southeast-2')

def create_cluster_mode_disabled(
    CacheNodeType='cache.t3.micro',
    EngineVersion='8.0',
    NumCacheClusters=1,  # 1 = just primary, no replicas (cheaper for testing)
    ReplicationGroupDescription='Test cluster',
    ReplicationGroupId=None,
    CacheSubnetGroupName=None,  # ADD THIS
    SecurityGroupIds=None  # ADD THIS
):
    """Creates an ElastiCache Cluster with cluster mode disabled"""
    
    if not ReplicationGroupId:
        return 'ReplicationGroupId parameter is required'
    
    if not CacheSubnetGroupName:
        return 'CacheSubnetGroupName parameter is required'
    
    params = {
        'AutomaticFailoverEnabled': False,  # False if NumCacheClusters=1
        'CacheNodeType': CacheNodeType,
        'Engine': 'valkey',
        'EngineVersion': EngineVersion,
        'NumCacheClusters': NumCacheClusters,
        'ReplicationGroupDescription': ReplicationGroupDescription,
        'ReplicationGroupId': ReplicationGroupId,
        'CacheSubnetGroupName': CacheSubnetGroupName,
        'TransitEncryptionEnabled': False,  # ADD THIS
        'AtRestEncryptionEnabled': False,   # ADD THIS (optional)
    }
    
    # Add security groups if provided
    if SecurityGroupIds:
        params['SecurityGroupIds'] = SecurityGroupIds
    
    response = client.create_replication_group(**params)
    return response


if __name__ == '__main__':
    # YOU NEED TO PROVIDE THESE:
    SUBNET_GROUP_NAME = 'public-subnet-group'  # Replace this
    SECURITY_GROUP_ID = 'sg-0ae701c48634f3e16'  # Replace this
    
    elasticacheResponse = create_cluster_mode_disabled(
        CacheNodeType='cache.t3.micro',  # Cheapest for testing
        EngineVersion='8.0',
        NumCacheClusters=1,  # Just 1 node for testing (no replicas)
        ReplicationGroupDescription='Valkey cluster mode disabled',
        ReplicationGroupId='dukbill-simple-cache',
        CacheSubnetGroupName=SUBNET_GROUP_NAME,
        SecurityGroupIds=[SECURITY_GROUP_ID]
    )
    
    logging.info(elasticacheResponse)