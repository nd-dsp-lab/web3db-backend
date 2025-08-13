const { ethers } = require("hardhat");

async function main() {
    console.log("Checking account balance and network information...\n");

    // Get the deployer account
    const [deployer] = await ethers.getSigners();

    console.log("Account address:", deployer.address);

    // Get account balance
    const balance = await ethers.provider.getBalance(deployer.address);

    // Handle both ethers v5 and v6 formatEther
    let balanceFormatted;
    try {
        balanceFormatted = ethers.formatEther(balance);
    } catch (error) {
        // Fallback to v5 syntax
        balanceFormatted = ethers.utils.formatEther(balance);
    }
    console.log("Account balance:", balanceFormatted, "ETH");

    // Check if balance is sufficient for transactions
    let minBalance;
    try {
        minBalance = ethers.parseEther("0.01"); // v6 syntax
    } catch (error) {
        minBalance = ethers.utils.parseEther("0.01"); // v5 syntax
    }

    // Handle balance comparison for both ethers v5 and v6
    let isLowBalance;
    if (typeof balance.lt === 'function') {
        // ethers v5 - use BigNumber methods
        isLowBalance = balance.lt(minBalance);
    } else {
        // ethers v6 - use native BigInt comparison
        isLowBalance = balance < minBalance;
    }

    if (isLowBalance) {
        console.log("⚠️  WARNING: Low balance! You may need more ETH for gas fees.");
        console.log("   Minimum recommended: 0.01 ETH");
        console.log("   Consider getting testnet ETH from a Sepolia faucet.");
    } else {
        console.log("✅ Balance is sufficient for transactions.");
    }

    // Get network information
    const network = await ethers.provider.getNetwork();
    console.log("\nNetwork information:");

    // Handle chainId for both ethers v5 and v6
    let chainId;
    if (typeof network.chainId === 'bigint') {
        chainId = network.chainId;
    } else {
        chainId = BigInt(network.chainId);
    }

    console.log("  Chain ID:", chainId.toString());
    console.log("  Network name:", network.name);

    // Check if we're on Sepolia
    if (chainId === 11155111n) {
        console.log("✅ Connected to Sepolia testnet");
    } else {
        console.log("⚠️  WARNING: Not connected to Sepolia testnet");
        console.log("   Expected chain ID: 11155111");
        console.log("   Current chain ID:", chainId.toString());
    }

    // Get gas price
    const feeData = await ethers.provider.getFeeData();
    const gasPrice = feeData.gasPrice;

    // Handle formatUnits for both ethers v5 and v6
    let gasPriceFormatted;
    try {
        gasPriceFormatted = ethers.formatUnits(gasPrice, "gwei");
    } catch (error) {
        gasPriceFormatted = ethers.utils.formatUnits(gasPrice, "gwei");
    }
    console.log("  Current gas price:", gasPriceFormatted, "gwei");

    // Estimate costs for common operations
    console.log("\nEstimated transaction costs:");
    const estimatedSingleUpdate = gasPrice * 60000n; // ~60k gas for single update
    const estimatedBatchUpdate = gasPrice * 200000n; // ~200k gas for batch update

    // Handle formatEther for gas cost estimates
    let singleUpdateCost, batchUpdateCost;
    try {
        singleUpdateCost = ethers.formatEther(estimatedSingleUpdate);
        batchUpdateCost = ethers.formatEther(estimatedBatchUpdate);
    } catch (error) {
        singleUpdateCost = ethers.utils.formatEther(estimatedSingleUpdate);
        batchUpdateCost = ethers.utils.formatEther(estimatedBatchUpdate);
    }

    console.log("  Single index update:", singleUpdateCost, "ETH");
    console.log("  Batch index update:", batchUpdateCost, "ETH");

    // Check block number
    const blockNumber = await ethers.provider.getBlockNumber();
    console.log("  Latest block number:", blockNumber);

    console.log("\n✅ Account and network check completed");
}

main()
    .then(() => process.exit(0))
    .catch((error) => {
        console.error("Error:", error);
        process.exit(1);
    });
