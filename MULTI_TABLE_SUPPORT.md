# Multi-Table Support

**Branch**: feature/multi-table-support  
**Date**: November 11, 2025  

---

## 📋 Overview

Transformed from a **single-table database** (hardcoded to `patient_data`) into a **fully generic multi-table database** without modifying the smart contract.

---

## What Changed
Uses format `"table_name.attribute_name"` → `"CID"` in the existing smart contract:
```
patient_data.PatientID → QmXxxx...
users.UserID → QmYyyy...
```

**No smart contract changes required!**

### Features Added

#### 1. Generic Upload API
```bash
# ONE endpoint for ALL tables
POST /upload/{table_name}

# Examples:
POST /upload/users
POST /upload/orders
POST /upload/products
POST /upload/inventory
# Works for ANY table name!
```

#### 2. Generic Row Count API
```bash
# Get row count for any table
GET /query/count?table_name=users&index_attribute=UserID
GET /query/count?table_name=orders&index_attribute=OrderID
GET /query/count?table_name=products  # auto-detects index
GET /query/count  # defaults to patient_data
```

#### 3. Multi-Table Query/Delete/Update
All operations now accept `table_name` parameter:
- **Query**: Include `table_name` in request body
- **Delete**: Automatically extracts table from DELETE SQL
- **Update**: Automatically extracts table from UPDATE SQL

#### 4. Table Management Endpoints
```bash
POST /tables/config          # Register indexed attributes
GET /tables/config           # List all tables
GET /tables/config/{table}   # Get specific table config
```

---

## Quick Start

### Upload Data to Multiple Tables
```bash
# Upload users
curl -X POST http://localhost:8001/upload/users \
  -F "file=@users.csv"

```

### Query Different Tables
```bash
# Query users
curl -X POST http://localhost:8001/query \
  -H "Content-Type: application/json" \
  -d '{
    "table_name": "users",
    "index_attribute": "UserID",
    "query": "SELECT * FROM users WHERE Age > 25",
    "wallet_address": "0x..."
  }'
```

### Get Row Counts
```bash
# Get user count
curl "http://localhost:8001/query/count?table_name=users&index_attribute=UserID"

# Get all tables
curl http://localhost:8001/tables/config
```

---

## API Reference

### Upload API
**Endpoint**: `POST /upload/{table_name}`

**Features**:
- Auto-detects data types and creates indexes
- Supports CSV and SQL files
- Zero configuration required
- Uses first column as default index

**Example**:
```bash
curl -X POST http://localhost:8001/upload/my_table \
  -F "file=@data.csv"
```

**Response**:
```json
{
  "table_name": "my_table",
  "data_cid": "QmXxxxxx...",
  "index_cids": {
    "my_table.ID": "QmYyyyyy..."
  },
  "rows_processed": 1000,
  "indexed_attributes": ["ID"],
  "message": "Data uploaded successfully"
}
```

### Query API
**Endpoint**: `POST /query`

**Request**:
```json
{
  "table_name": "users",
  "index_attribute": "UserID",
  "query": "SELECT * FROM users WHERE Age > 25",
  "wallet_address": "0x..."
}
```

### Row Count API
**Endpoint**: `GET /query/count?table_name={table}&index_attribute={attr}`

**Parameters**:
- `table_name` (optional): Table name (default: patient_data)
- `index_attribute` (optional): Index to use (auto-detects if omitted)

**Response**:
```json
{
  "status": "success",
  "table_name": "users",
  "total_rows": 50000,
  "cids_processed": 10,
  "index_used": "UserID"
}
```

### Table Configuration API
**Endpoints**:
- `POST /tables/config` - Register indexed attributes
- `GET /tables/config` - List all tables
- `GET /tables/config/{table_name}` - Get specific table

**Register Table**:
```bash
curl -X POST http://localhost:8001/tables/config \
  -H "Content-Type: application/json" \
  -d '{
    "table_name": "products",
    "indexed_attributes": ["ProductID", "CategoryID", "Price"]
  }'
```

### Index Management API
**Endpoints**:
- `GET /index-cids?table_name={table}` - Get indexes for table
- `PUT /index-cids` - Update index CIDs (supports composite keys)

---

## Implementation Details

### Code Changes (app/scripts/app.py)

#### New Helper Functions
```python
make_index_key(table_name, attribute_name)     # Create composite keys
parse_index_key(index_key)                     # Parse composite keys  
get_table_indexed_attributes(table_name)       # Get indexed attributes
register_table_config(table_name, attrs)       # Register table config
```

#### Updated Functions (15+ functions modified)
All core functions now accept optional `table_name` parameter:
- `get_index_cid(attribute, table_name=None)`
- `set_index_cid(attribute, cid, table_name=None)`
- `retrieve_index(name, table_name=None)`
- `upload_encrypted_index(index, attr, table_name=None)`
- And many more...

#### New/Updated Endpoints
- `POST /upload/{table_name}` - Generic upload
- `GET /query/count?table_name=X&index_attribute=Y` - Generic row count
- `POST /query` - Multi-table query support
- `POST /delete` - Multi-table delete support
- `POST /update` - Multi-table update support
- `POST /tables/config` - Register table config
- `GET /tables/config` - List all tables
- `GET /tables/config/{table}` - Get table config

### Global State Structure
```python
app.state.table_configs = {
    "patient_data": ["PatientID", "HospitalID", "Age"],
    "users": ["UserID", "Email"],
}
```

---

## Architecture

### Before
```
- Single table (patient_data)
- Hardcoded indexes
- Code changes for new tables
```

### After
```
- Unlimited tables
- Dynamic indexes per table
- Generic APIs
- Auto-configuration
```

### Data Flow
```
Client Request
     ↓
POST /upload/{table_name}
     ↓
Auto-detect schema & indexes
     ↓
Encrypt data (AES-256-CBC)
     ↓
Store on IPFS → Get CID
     ↓
Create/update encrypted indexes
     ↓
Store index CIDs in smart contract
(using composite key: table.attribute)
     ↓
Return success response
```

---

## 💡 Usage Examples

### Example 1: Healthcare System
```bash
# Upload different medical tables
curl -X POST http://localhost:8001/upload/patients -F "file=@patients.csv"
curl -X POST http://localhost:8001/upload/visits -F "file=@visits.csv"
curl -X POST http://localhost:8001/upload/prescriptions -F "file=@prescriptions.csv"
curl -X POST http://localhost:8001/upload/lab_results -F "file=@lab_results.csv"

# Query specific table
curl -X POST http://localhost:8001/query \
  -H "Content-Type: application/json" \
  -d '{"table_name": "patients", "query": "SELECT * FROM patients WHERE Age > 65", "wallet_address": "0x..."}'

# Get patient count
curl "http://localhost:8001/query/count?table_name=patients"
```
