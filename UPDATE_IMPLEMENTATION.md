# UPDATE Implementation Documentation

## Architecture Philosophy

### Delete + Insert Paradigm
The UPDATE operation follows a **"delete + insert"** semantic:
- Original data CIDs are replaced with new CIDs containing updated records
- All records are preserved in the new CID (no data loss)
- Indexes are updated to point to new CIDs
- Historical versions remain accessible in IPFS

### Key Design Principles
1. **Immutability**: No data is ever truly deleted or modified in-place
2. **Atomicity**: Either all updates succeed or none do
3. **Multi-tenancy**: Strong isolation between different wallet owners
4. **Performance**: Parallel processing with optimized resource usage
5. **Auditability**: Complete operation history preserved

## API Specification

### Endpoint
```
POST /update
```

### Request Model
```python
class UpdateRequest(BaseModel):
    update_query: str      # SQL UPDATE statement
    wallet_address: str    # Wallet address for access control
```

### Example Request
```bash
curl -X 'POST' 'http://localhost:8001/update' \
  -H 'Content-Type: application/json' \
  -d '{
    "update_query": "UPDATE patient_data SET Name = \"John Doe\", Age = 30 WHERE PatientID = \"323\"",
    "wallet_address": "0x1A28b19f6d2ea1A05F9eFFbcCcbF7E9571877981"
  }'
```

### Response Format
```json
{
  "message": "UPDATE completed successfully",
  "updated_count": 2,
  "affected_cids": 1,
  "old_cids": ["QmABC123..."],
  "new_cids": ["QmXYZ789..."],
  "processing_time": 1.23,
  "index_update_success": true,
  "operation_stats": {
    "total_records_processed": 100,
    "accessible_records": 50,
    "matching_records": 2
  }
}
```

## Implementation Components

### 1. Query Parsing

#### `parse_update_query(update_query: str)`
Extracts components from SQL UPDATE statements using regex patterns.

**Input**: `"UPDATE patient_data SET Name = 'John Doe', Age = 30 WHERE PatientID = '323'"`

**Output**: 
- `table_name`: "patient_data"
- `set_clause`: "Name = 'John Doe', Age = 30"
- `where_clause`: "PatientID = '323'"
- `primary_key_value`: "323"

#### `parse_set_clause(set_clause: str)`
Converts SET clause into structured update fields.

**Input**: `"Name = 'John Doe', Age = 30, Gender = 'Male'"`

**Output**: `{'Name': 'John Doe', 'Age': 30, 'Gender': 'Male'}`

**Features**:
- Handles quoted strings (single and double quotes)
- Converts numeric values automatically
- Supports NULL values
- Preserves data types

### 2. Core Update Process

#### Step 1: CID Discovery
```python
relevant_cids = await find_cids_containing_records(where_clause, index_attribute)
```
- Uses PatientID index to find potentially matching CIDs
- Optimizes performance by avoiding full dataset scans
- Returns list of CID identifiers

#### Step 2: Parallel CID Processing
```python
def process_cid_for_update(cid: str, where_clause: str, update_fields: dict, wallet_address: str)
```

**For each CID**:
1. **Fetch & Decrypt**: Download encrypted data from IPFS
2. **Access Control**: Filter by `OwnerID == wallet_address`
3. **Apply WHERE Clause**: Find matching records using DuckDB
4. **Apply Updates**: Modify records with new field values
5. **Re-encrypt & Upload**: Create new encrypted CID
6. **Return Results**: New CID and affected records

#### Step 3: Index Management
```python
async def update_indexes_after_update(old_cid: str, new_cid: str, all_records: List[dict], updated_records: List[dict])
```
- Reuses DELETE index logic
- Updates PatientID, HospitalID, and Age indexes

### 3. Access Control Integration

#### Multi-Tenant Security
- Each record contains `OwnerID` field
- Only records where `OwnerID == wallet_address` can be updated

#### Policy Enforcement
```python
# Filter accessible records
accessible_df = df[df['OwnerID'] == wallet_address]

# Apply access policies from smart contract
success, policies = app.state.index_storage.get_access_policies(wallet_address)
```

#### Error Response Examples
```json
{
  "error": "Invalid UPDATE query. Expected format: UPDATE table_name SET column = value WHERE condition"
}

{
  "error": "No access policies found for this wallet address",
  "wallet_address": "0x..."
}

{
  "message": "No records found matching the UPDATE criteria",
  "updated_count": 0,
  "affected_cids": 0
}
```

## Data Flow Diagram

```
[SQL UPDATE Query] 
        ↓
[Parse Query Components]
        ↓
[Find Relevant CIDs via Index]
        ↓
[Parallel CID Processing]
        ↓
┌─────────────────────────────┐
│ For Each CID:               │
│ 1. Fetch & Decrypt from IPFS│
│ 2. Apply Access Control     │
│ 3. Filter by WHERE clause   │
│ 4. Apply UPDATE changes     │
│ 5. Re-encrypt & Upload      │
│ 6. Return new CID           │
└─────────────────────────────┘
        ↓
[Update All Indexes]
        ↓
[Return Success Response]
```

## Security Considerations

### Data Encryption
- AES-256 encryption for all data at rest
- Encrypted packages uploaded to IPFS

### Access Control
- Wallet-based authentication
- Smart contract policy enforcement

### Audit Trail
- All operations logged with timestamps
- Historical CIDs preserved in IPFS


## Comparison with Traditional Databases

| Aspect | Traditional DB | Web3DB UPDATE |
|--------|----------------|---------------|
| **Data Modification** | In-place updates | Versioned CID replacement |
| **History** | Lost (unless explicitly tracked) | Automatically preserved |
| **Atomicity** | Transaction-based | CID-level atomicity |
| **Consistency** | ACID compliance | Eventually consistent indexes |
| **Scalability** | Vertical scaling | Horizontal IPFS scaling |
| **Security** | Database-level permissions | Cryptographic + smart contract |

## Future Enhancements

### Planned Improvements
1. **Batch Operations**: Multiple UPDATE statements in single request
2. **Conditional Updates**: More complex WHERE clause support
3. **Cross-CID Transactions**: ACID properties across multiple CIDs
