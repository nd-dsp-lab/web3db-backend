const fs = require('fs');
const path = require('path');

async function main() {
  try {
    // Look for the deployed addresses file
    const deploymentPath = path.join(__dirname, '..', 'ignition', 'deployments', 'chain-31337', 'deployed_addresses.json');
    
    if (fs.existsSync(deploymentPath)) {
      const deployedAddresses = JSON.parse(fs.readFileSync(deploymentPath, 'utf8'));
      
      console.log('📋 Deployed Contract Addresses:');
      console.log('=' .repeat(50));
      
      for (const [moduleName, addresses] of Object.entries(deployedAddresses)) {
        console.log(`Module: ${moduleName}`);
        if (typeof addresses === 'string') {
          // If addresses is a string, it's the contract address
          console.log(`  Contract Address: ${addresses}`);
        } else {
          // If addresses is an object, iterate through it
          for (const [contractName, address] of Object.entries(addresses)) {
            console.log(`  ${contractName}: ${address}`);
          }
        }
        console.log('');
      }
      
      // Get the Web3dbContract address specifically
      let web3dbAddress = null;
      for (const [moduleName, addresses] of Object.entries(deployedAddresses)) {
        if (moduleName.includes('Web3dbModule')) {
          if (typeof addresses === 'string') {
            web3dbAddress = addresses;
          } else if (addresses.web3dbContract) {
            web3dbAddress = addresses.web3dbContract;
          }
          break;
        }
      }
      
      if (web3dbAddress) {
        console.log('🎯 Web3DB Contract Address for Backend:');
        console.log(`   ${web3dbAddress}`);
        console.log('');
        console.log('📝 To update your backend, set this environment variable:');
        console.log(`   export CONTRACT_ADDRESS="${web3dbAddress}"`);
        console.log('');
        console.log('Or update the hardcoded address in app/scripts/app.py:');
        console.log(`   contract_address="${web3dbAddress}"`);
      }
      
    } else {
      console.log('❌ No deployment found. Please deploy the contract first:');
      console.log('   npm run deploy:localhost');
    }
    
  } catch (error) {
    console.error('Error reading deployment addresses:', error);
  }
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error(error);
    process.exit(1);
  });
