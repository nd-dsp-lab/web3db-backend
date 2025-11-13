import web3db_contract_index
import bplustree
import logging
import requests
import os
from dotenv import load_dotenv
from typing import List, Tuple, Optional
INDEX_STORAGE = None
INDEXES = {"PatientID": [],  "HospitalID": [], "Age": []}
'''
DELTA_LAST = <CID>

def uploadData(data):
  # do i need to set lock from here
  getIndex()
  age_set = set(data['age])
  CID = IPFS_upload(data)
  prev_delta = smart_contract_get_age_index()
  with open("delta_file", "w") as tmp:
    tmp.write(prev_delta)
    for age in age_set:
      age_index[age].add(CID)
      tmp.write((age, CID))

  delta_CID = IPFS_upload("delta_file")
  smart_contract_update_age_index()
  # to here ???
  
def getIndex():
  tail_delta = smart_contract_get_age_index()
  delta = tail_delta
  while delta != DELTA_LAST:
    delta_file = IPFS_download(delta)
    prev_delta, changes = delta_file.parsed()
    for age, CID in changes:
      age_index[age].add(CID)
    delta = prev_delta
  DELTA_LAST = tail_delta
'''
def ipfs_upload(data: str) -> str:
  ipfs_api = "http://localhost:5001/api/v0/add"
  resp = requests.post(ipfs_api, files={"file": ("patient_data.enc", data)})
  resp.raise_for_status()
  data_cid = resp.json()["Hash"]
  return data_cid

def ipfs_fetch(cid: str) -> Optional[bytes]:
  try:
    resp = requests.post("http://localhost:5001/api/v0/cat", params={"arg": cid}, timeout=30)
    if resp.status_code != 200:
      print(f"Failed to fetch {cid} from IPFS: Status {resp.status_code}")
      return None
    return resp.content
  except Exception as e:
    print(f"Error fetching CID {cid}: {e}")
    return None

def upload(attribute, data):
  cid = ipfs_upload(data)
  
  print(INDEX_STORAGE)

def get_index(attribute):
  success, cids = INDEX_STORAGE.get_index(attribute)
  if success:
    return cids
  return []

def update_local_index(attribute):
  cids = get_index(attribute)
  for cid in cids:
    data = ipfs_fetch(cid).decode()
    print(data)
    INDEXES[attribute]


if __name__ == "__main__":
  # INDEX_STORAGE = web3db_contract_index.Web3dbContract(
  #   contract_address="0x5FbDB2315678afecb367f032d93F642f64180aa3",
  #   infura_api_key="eb1d43f1429e49fba50e18fbf5ebd4ab",
  #   private_key="0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
  # )
  # get_index("PatientID")
  cid = ipfs_upload("hi")
  print(ipfs_fetch(cid).decode())