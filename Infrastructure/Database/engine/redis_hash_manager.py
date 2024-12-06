import redis
from redis.cluster import ClusterNode
import json
from datetime import datetime
from typing import Dict, List, Optional, Union
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RedisHashManager:
    def __init__(self):
        """Initialize Redis Cluster connection."""
        try:
            # Define cluster nodes
            startup_nodes = [
                ClusterNode("redis-master-1", 6379),
                ClusterNode("redis-master-2", 6379),
                ClusterNode("redis-master-3", 6379),
                ClusterNode("redis-replica-1", 6379),
                ClusterNode("redis-replica-2", 6379),
                ClusterNode("redis-replica-3", 6379)
            ]
            
            self.redis_client = redis.RedisCluster(
                startup_nodes=startup_nodes,
                decode_responses=True,
                read_from_replicas=True,
                require_full_coverage=False
            )
            logger.info("Successfully connected to Redis Cluster")
            
        except Exception as e:
            logger.error(f"Failed to connect to Redis Cluster: {str(e)}")
            raise

    def _generate_key(self, user_id: str, table_name: str, partition: str) -> str:
        """Generate Redis key for storing hash mappings."""
        return f"{{hash_mapping}}:{user_id}:{table_name}:{partition}"

    def get_latest_hash(self, user_id: str, table_name: str, partition: str) -> Optional[str]:
        """Get the most recent hash for a given partition."""
        key = self._generate_key(user_id, table_name, partition)
        try:
            records = self.redis_client.get(key)
            if records:
                records_list = json.loads(records)
                if records_list:
                    # Simply return the hash from the last record
                    return records_list[-1]['hash']
            return None
        except Exception as e:
            logger.error(f"Error getting latest hash: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Failed to get latest hash: {str(e)}")

    def get_hash_history(
        self,
        user_id: str,
        table_name: str,
        partition: str
    ) -> List[Dict[str, Union[str, int]]]:
        """Get complete hash history for a partition."""
        key = self._generate_key(user_id, table_name, partition)
        try:
            records = self.redis_client.get(key)
            return json.loads(records) if records else []
        except Exception as e:
            logger.error(f"Error getting hash history: {str(e)}")
            return []

    def add_hash_mapping(
        self,
        user_id: str,
        table_name: str,
        partition: str,
        hash_value: str,
        prev_hash: str = "",
        record_type: str = "create",
        flag: str = "initial",
        row_count: int = 0
    ) -> bool:
        """Add a new hash mapping record."""
        key = self._generate_key(user_id, table_name, partition)
        
        record = {
            "hash": hash_value,
            "prevHash": prev_hash,
            "type": record_type,
            "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "flag": flag,
            "rowCount": row_count
        }
        
        try:
            existing_records = self.redis_client.get(key)
            if existing_records:
                records = json.loads(existing_records)
                records.append(record)
            else:
                records = [record]
            
            self.redis_client.set(key, json.dumps(records))
            logger.info(f"Successfully added hash mapping for key: {key}")
            return True
        except Exception as e:
            logger.error(f"Error adding hash mapping: {str(e)}")
            raise