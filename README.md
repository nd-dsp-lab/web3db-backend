# MtDB: A Decentralized Multi-Tenant Database for Secure Data Sharing

This repository contains the implementation and evaluation code for our research article titled: **"MtDB: A Decentralized Multi-Tenant Database for Secure Data Sharing"**.

MtDB is a novel data sharing system for healthcare data management. Key components:
- **Intel SGX V2** for confidential computing and privacy-preserving query processing
- **IPFS** for distributed storage with content-addressable data
- **Blockchain (Ethereum)** for metadata management and index integrity
- **Advanced Indexing** with delta-based updates for efficient querying
- **Query Re-writer** for enforcing in-enclave fine-grained access control

(**For details please read our paper**)

## 🏗️ Simplified Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Client App    │    │  SGX Enclave    │    │   Blockchain    │
│                 │───▶│                 │───▶│                 │
│ Query Interface │    │ Query Processor │    │ Index Metadata  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │      IPFS       │
                    │                 │
                    │ Encrypted Data  │
                    │   Partitions    │
                    └─────────────────┘
```

## 🔧 Prerequisites

- **Operating System**: Ubuntu 20.04/22.04 LTS
- **Intel SGX v2**: Hardware support with SGX driver installed
- **Gramine**: SGX runtime environment
- **Python**: 3.8 or higher
- **IPFS**: Local IPFS node
- **Docker**


## 📦 Installation

### 1. Clone Repository
```bash
git clone <MtDB>
cd MtDB/app
```

### 2. Install Dependencies
```bash
sudo pip3 install -r requirements.txt --break-system-packages
```

### 3. Build SGX Application
```bash
make clean
make SGX=1 (For SGX variant)
make (For vanilla variant)
```

### 4. Setup IPFS (if not already running)
```bash
cd MtDB/app/ipfs
sudo docker-compose up -d
```

## 🚀 Usage

### Basic Query Execution

1. **Start the SGX-enabled application**:
   ```bash
   sudo gramine-sgx ./python MtDB/scripts/app.py
   ```

2. **Run the vanilla variant**:
   ```python
   sudo gramine-direct ./python MtDB/scripts/app.py
   ```

### Configuration

Edit configuration files in `MtDB/app/` directory:
- `python.manifest.template`: SGX manifest configuration
- `requirements.txt`: Python dependencies

## 📊 Evaluation

### Performance Evaluation Scripts

Generate performance evaluation figures used in the paper:

```bash
cd MtDB/app/plot
python3 generate_four_panel_figure.py
```

### Benchmark Datasets

The evaluation uses synthetic healthcare datasets:
- **Size range**: 100M - 400M records
- **Partition size**: 100K records per CID (~11.848 Mbits)

### Key Performance Metrics

- **Query Latency**: With/without indexing across database sizes
- **Scalability**: Performance vs. number of CID partitions
- **Network Distribution**: LAN vs. WAN retrieval overhead
- **SGX Overhead**: Vanilla vs. SGX-enabled performance comparison

## 📈 Performance Results

Our evaluation demonstrates:

1. **Index Efficiency**: 1000x performance improvement with indexing
2. **SGX Overhead**: ~30% latency increase for privacy guarantees
3. **Linear Scalability**: Query time scales linearly with CID count
4. **Network Impact**: WAN retrieval adds significant overhead vs. LAN

Detailed results are available in the generated performance figures and the paper.

## 🎯 Reproducing Paper Results

### Figure Generation

```bash
cd MtDB/app/plot
python3 generate_four_panel_figure.py
```

### Dataset Generation

```bash
cd MtDB/app/utils
python3 generate_synthetic_data.py
```

### Performance Benchmarks

```bash
cd MtDB/app/scripts
sudo gramine-sgx ./python app.py
```

## 🔗 Related Docs

- [Intel SGX Documentation](https://www.intel.com/content/www/us/en/developer/tools/software-guard-extensions/overview.html)
- [Gramine SGX Runtime](https://gramine.readthedocs.io/)
- [IPFS Documentation](https://docs.ipfs.io/)

---

**Note**: This is research prototype software. For production use, additional security measures and testing are recommended.