# README for IPFS Hive Query API

## Introduction

The IPFS Hive Query API is a sophisticated Flask-based API designed for executing SQL queries on a Hive database, managing database states with IPFS, and integrating seamlessly with Apache Spark and Hive. This API is especially useful for scenarios requiring stateful data processing and distributed query execution.

## Features

- RESTful API for executing SQL queries on Hive
- Database state management using IPFS
- Apache Spark and Hive integration for efficient data processing

## Deployment

The application is containerized and ready for deployment using Docker Compose. This simplifies the setup and ensures consistency across different environments.

## Using the API

### 1. Running the Service

To run the service, use Docker Compose:

```bash
docker-compose up
```

This command starts all necessary services, including the Flask API, Spark, Hive, and the IPFS node.

### 2. API Endpoints

The API provides a single endpoint:

- **POST /query/**: To execute a SQL query and return the results.

### 3. Making Requests

#### SELECT Queries (Read)

For `SELECT` queries (Read operations), send a POST request with the query and a valid IPFS hash of the previous database state.

```python
import requests
import json

url = "http://localhost:5000/query/"
payload = {
    "query": "SELECT * FROM your_table",
    "hash": "valid_ipfs_hash_of_previous_state"
}
headers = {'Content-Type': 'application/json'}

response = requests.post(url, data=json.dumps(payload), headers=headers)
print(response.json())
```

#### INSERT Queries (Create)

For `INSERT` queries (Create operations), send a POST request with the query and a valid IPFS hash of the previous database state.

```python
import requests
import json

url = "http://localhost:5000/query/"
payload = {
    "query": "INSERT INTO your_table (column1, column2) VALUES (value1, value2)",
    "hash": "valid_ipfs_hash_of_previous_state"
}
headers = {'Content-Type': 'application/json'}

response = requests.post(url, data=json.dumps(payload), headers=headers)
print(response.json())
```

#### UPDATE Queries (Update)

For `UPDATE` queries (Update operations), send a POST request with the query and a valid IPFS hash of the previous database state.

```python
import requests
import json

url = "http://localhost:5000/query/"
payload = {
    "query": "UPDATE your_table SET column1 = value1 WHERE condition",
    "hash": "valid_ipfs_hash_of_previous_state"
}
headers = {'Content-Type': 'application/json'}

response = requests.post(url, data=json.dumps(payload), headers=headers)
print(response.json())
```

#### DELETE Queries (Delete)

For `DELETE` queries (Delete operations), send a POST request with the query and a valid IPFS hash of the previous database state.

```python
import requests
import json

url = "http://localhost:5000/query/"
payload = {
    "query": "DELETE FROM your_table WHERE condition",
    "hash": "valid_ipfs_hash_of_previous_state"
}
headers = {'Content-Type': 'application/json'}

response = requests.post(url, data=json.dumps(payload), headers=headers)
print(response.json())
```

#### CREATE TABLE and Non-Dependent Queries

For `CREATE TABLE` and other non-dependent queries that do not rely on the previous state, a valid hash is not required. Use a dummy hash instead. The response will include a new IPFS hash representing the updated database state.

```python
import requests
import json

url = "http://localhost:5000/query/"
payload = {
    "query": "CREATE TABLE new_table (id INT, name STRING)",
    "hash": "dummy_ipfs_hash"
}
headers = {'Content-Type': 'application/json'}

response = requests.post(url, data=json.dumps(payload), headers=headers)
print(response.json())
```

### 4. Response Format

Responses are JSON objects containing:

- `message`: Status of the query execution.
- `data`: Query results (if successful).
- `hash`: New IPFS hash for the updated database state (for modifying queries).

### 5. Error Handling

The API returns detailed error messages and appropriate HTTP status codes for troubleshooting.

## Docker Compose

A `docker-compose.yml` file is provided for easy setup. This file defines the necessary services and configurations for running the API.

## Logging

The application logs key events and query executions for monitoring and debugging purposes.

---

**Note**: Ensure the Docker Compose setup aligns with your deployment requirements and environment. Adjust the API usage examples to match your specific use cases.
