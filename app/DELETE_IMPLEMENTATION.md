# DELETE Query Implementation Documentation

## Overview

This document describes the implementation of DELETE query support in the Web3DB system. The DELETE functionality follows a versioning approach where new versions of data files are created without the deleted records, maintaining IPFS immutability while providing true data deletion capabilities.

## Architecture Approach

The DELETE implementation uses **Approach 2: Data Versioning with Compaction** from our design analysis:

- Creates new versions of data files without deleted records
- Replaces old CIDs with new CIDs in indexes  
- Maintains referential integrity
- Updates all indexes for entire batches of records

## API Endpoint

### `POST /delete`

Deletes records from the database based on a SQL DELETE query with access control.

**Request Body:**
```json
{
  "delete_query": "DELETE FROM patient_data WHERE PatientID = '323'",
  "wallet_address": "0x68ef100cC9dAdE0bb67a0aE99A02CDd1eaE54A2f"
}
```

**Response (Success):**
```json
{
  "message": "DELETE operation completed successfully",
  "deleted_count": 5,
  "affected_cids": 2,
  "cid_mapping": {
    "QmOldCID1...": "QmNewCID1...",
    "QmOldCID2...": "EMPTY"
  },
  "wallet_address": "0x68ef100cC9dAdE0bb67a0aE99A02CDd1eaE54A2f",
  "query": "DELETE FROM patient_data WHERE PatientID = '323'",
  "index_update_success": true,
  "policy_count": 2,
  "deletion_stats": {
    "total_deletions": 15,
    "last_deletion": 1727123456.789
  },
  "performance": {
    "cid_processing_time_seconds": 2.45,
    "records_processed": 5
  },
  "debug_info": {
    "deleted_patient_ids": ["323", "324", "325"],
    "old_cids_replaced": ["QmOldCID1...", "QmOldCID2..."],
    "new_cids_created": ["QmNewCID1...", "EMPTY"]
  }
}
```

**Response (Error):**
```json
{
  "error": "No access policies found for this wallet address",
  "wallet_address": "0x68ef100cC9dAdE0bb67a0aE99A02CDd1eaE54A2f"
}
```

## Implementation Details

### 1. Query Parsing (`parse_delete_query`)

Extracts components from DELETE SQL:
- Table name (must be `patient_data`)
- WHERE clause conditions
- Primary key value (PatientID) for optimization

```python
# Input: "DELETE FROM patient_data WHERE PatientID = '323'"
# Output: ("patient_data", "PatientID = '323'", "323")
```

### 2. CID Discovery (`find_cids_containing_records`)

Uses existing index system to find CIDs that might contain matching records:
- Leverages the PatientID index for efficient lookup
- Reuses the `query_index` function for consistent behavior
- Returns list of CIDs to process
- Supports configurable index attribute (defaults to 'PatientID')
- Provides async interface for integration with the DELETE endpoint

```python
async def find_cids_containing_records(where_clause: str, index_attribute: str = 'PatientID'):
    # Uses query_index to find relevant CIDs based on WHERE clause
    # Returns list of CIDs that potentially contain matching records
```

### 3. Parallel CID Processing (`process_cid_for_deletion`)

For each CID containing potentially matching records:

1. **Fetch & Decrypt**: Retrieve encrypted data from IPFS and decrypt
2. **Access Control**: Apply user's access policies to determine which records can be deleted
3. **Filter Records**: Remove deletable records from the dataset
4. **Re-encrypt & Store**: Create new encrypted Parquet file and upload to IPFS
5. **Handle Empty Cases**: Return "EMPTY" indicator if all records are deleted

**Threading Configuration:**
- Conservative worker count: `max_workers = min(4, len(relevant_cids))`
- Individual CID timeout: 15 seconds
- Overall processing timeout: 30 seconds
- Uses `ThreadPoolExecutor` with `concurrent.futures` for reliability

### 4. Index Management (`update_indexes_after_deletion`)

Updates all indexes (PatientID, HospitalID, Age) for the entire batch:

- **Remove Old Mappings**: Remove old CID from all record mappings in the batch
- **Add New Mappings**: Add new CID for remaining (non-deleted) records
- **Handle Empty CIDs**: Skip index updates for completely empty CIDs
- **Batch Updates**: Update smart contract with all index changes at once

### 5. Access Control Integration

DELETE operations respect the same access control policies as SELECT queries:
- Only records accessible by the user's policies can be deleted
- Query rewriting ensures policy enforcement
- Comprehensive audit trail maintained

## Key Features

### **ACID Properties**
- **Atomicity**: All-or-nothing operations per CID
- **Consistency**: Indexes always reflect current state
- **Isolation**: Parallel processing with proper coordination
- **Durability**: All changes persisted to IPFS and blockchain

### **Performance Optimized**
- **Parallel Processing**: Multiple CIDs processed concurrently
- **Efficient Indexing**: Batch updates to minimize smart contract calls
- **Memory Management**: Proper cleanup of temporary files
- **Index Reuse**: Leverages existing index infrastructure

### **Security & Access Control**
- **Policy Enforcement**: Only authorized deletions allowed
- **Audit Trail**: Complete record of deletion operations
- **Wallet Authentication**: All operations tied to wallet addresses
- **Immutable Logs**: Deletion statistics tracked permanently


## Usage Examples

### Simple Deletion
```bash
curl -X POST http://localhost:8001/delete \
  -H "Content-Type: application/json" \
  -d '{
    "delete_query": "DELETE FROM patient_data WHERE PatientID = '\''323'\''",
    "wallet_address": "0x68ef100cC9dAdE0bb67a0aE99A02CDd1eaE54A2f"
  }'
```

### Conditional Deletion
```bash
curl -X POST http://localhost:8001/delete \
  -H "Content-Type: application/json" \
  -d '{
    "delete_query": "DELETE FROM patient_data WHERE Age > 95 AND HospitalID = '\''HOSP1'\''",
    "wallet_address": "0x68ef100cC9dAdE0bb67a0aE99A02CDd1eaE54A2f"
  }'
```

### Range Deletion
```bash
curl -X POST http://localhost:8001/delete \
  -H "Content-Type: application/json" \
  -d '{
    "delete_query": "DELETE FROM patient_data WHERE Age BETWEEN 90 AND 95",
    "wallet_address": "0x68ef100cC9dAdE0bb67a0aE99A02CDd1eaE54A2f"
  }'
```

## Error Handling

### Common Error Scenarios

1. **Invalid SQL Syntax**
   ```json
   {"error": "Invalid DELETE query. Expected format: DELETE FROM table_name WHERE condition"}
   ```

2. **Unsupported Table**
   ```json
   {"error": "Unsupported table: wrong_table. Only 'patient_data' is supported."}
   ```

3. **Access Control Failure**
   ```json
   {"error": "No access policies found for this wallet address"}
   ```

4. **No Matching Records**
   ```json
   {"message": "No records found matching the DELETE criteria", "deleted_count": 0}
   ```

5. **CID Processing Timeout**
   ```json
   {"error": "DELETE operation timed out"}
   ```

6. **Index Update Timeout**
   ```json
   {"error": "DELETE operation failed: Index update timed out after 30 seconds"}
   ```

7. **CID Processing Failure**
   ```json
   {"error": "CID processing failed: [specific error details]"}
   ```

## Enhanced Response Format

The DELETE endpoint provides comprehensive information about the operation:

### Performance Metrics
- **cid_processing_time_seconds**: Time spent processing all CIDs
- **records_processed**: Total number of records that were deleted

### Debug Information
- **deleted_patient_ids**: List of PatientIDs that were actually deleted
- **old_cids_replaced**: List of original CIDs that were replaced
- **new_cids_created**: List of new CIDs created ("EMPTY" for completely deleted CIDs)

### Operation Metadata
- **policy_count**: Number of access policies applied
- **index_update_success**: Boolean indicating if index updates succeeded
- **deletion_stats**: Current system-wide deletion statistics


## Monitoring & Statistics

The system tracks deletion statistics in real-time:

```python
app.state.deletion_stats = {
    'total_deletions': 150,      # Total records deleted since startup
    'last_deletion': 1727123456.789  # Timestamp of last deletion (None if no deletions)
}
```


**Performance Monitoring:**
Each DELETE response includes detailed performance metrics:
```json
{
  "performance": {
    "cid_processing_time_seconds": 2.45,
    "records_processed": 5
  },
  "debug_info": {
    "deleted_patient_ids": ["323", "324"],
    "old_cids_replaced": ["QmOld..."],
    "new_cids_created": ["QmNew..."]
  }
}
```

## Integration with Existing Features

### **SELECT Queries**
- Deleted records automatically excluded from future queries
- No changes needed to existing SELECT implementation
- Consistent access control enforcement

### **Index System**
- Reuses existing CIDIndex infrastructure
- Compatible with PatientID, HospitalID, Age indexes
- Maintains index performance characteristics

### **Smart Contract Integration**
- Uses existing index CID storage
- Batch updates minimize blockchain transaction costs
- Consistent with upload/query patterns

### **Access Control**
- Same policy system as SELECT queries
- Query rewriting ensures security
- Audit trail maintained

## Future Improvements

### Potential Improvements

1. **Soft Delete Option**: Flag-based deletion for reversibility
2. **Bulk Operations**: Optimized multi-query deletion
3. **Compaction Scheduling**: Automatic cleanup of old CIDs
4. **Deletion Audit Log**: Detailed tracking of all deletions

### Advanced Features

1. **Transaction Log**: Maintain deletion transaction history
2. **Rollback Support**: Ability to undo recent deletions
3. **Cascading Deletes**: Handle related record deletions
4. **Conditional Rollback**: Undo deletions based on criteria

## Security Considerations

### **Access Control**
- DELETE operations require valid access policies
- Users can only delete records they have access to
- Wallet-based authentication mandatory

### **Data Protection**
- All data remains encrypted on IPFS
- Deletion creates new encrypted versions
- Original data may remain on IPFS (immutable)

### **Audit Trail**
- Complete logging of deletion operations
- Wallet addresses tracked for all deletions
- Timestamps and query details preserved

### ⚠️ **IPFS Immutability Note**
While records are "deleted" from the database perspective, the original encrypted data may still exist on IPFS nodes. For true data purging, additional IPFS garbage collection and pin management would be required.

## Implementation Functions

### Core Functions

#### `parse_delete_query(delete_query: str)`
- **Purpose**: Parse SQL DELETE statement into components
- **Input**: Raw SQL DELETE query string
- **Output**: Tuple of (table_name, where_clause, primary_key_value)
- **Validation**: Ensures proper SQL syntax and supported table names

#### `find_cids_containing_records(where_clause: str, index_attribute: str = 'PatientID')`
- **Purpose**: Async function to find CIDs that may contain matching records
- **Input**: WHERE clause and index attribute to search
- **Output**: List of relevant CIDs to process
- **Method**: Uses existing query_index infrastructure

#### `process_cid_for_deletion(cid: str, where_clause: str, wallet_address: str)`
- **Purpose**: Process individual CID for deletion
- **Input**: CID to process, WHERE clause, wallet address
- **Output**: Tuple of (new_cid, all_records, deleted_records)
- **Operations**: Decrypt → Filter → Access Control → Re-encrypt → Upload

#### `apply_where_clause_to_dataframe(df: pd.DataFrame, where_clause: str)`
- **Purpose**: Apply WHERE clause filtering to DataFrame
- **Input**: DataFrame and WHERE clause string
- **Output**: Filtered DataFrame with matching records
- **Features**: Supports complex WHERE conditions with proper SQL parsing

#### `update_indexes_after_deletion(old_cid: str, new_cid: str, all_records: List[dict], deleted_records: List[dict])`
- **Purpose**: Async function to update all indexes after deletion
- **Input**: Old/new CID mapping and record lists
- **Output**: Boolean success indicator
- **Operations**: Remove old mappings → Add new mappings → Batch smart contract update
