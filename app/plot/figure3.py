import matplotlib.pyplot as plt
import numpy as np

# Data
x_labels = ['100-0', '75-25', '50-50', '25-75', '0-100']
lan_times = [2.5640, 2.7760, 3.0916, 3.4299, 3.6412]
wan_times = [0.0, 2.7267, 5.2705, 7.7896, 10.4356]

x = np.arange(len(x_labels))
width = 0.6

# IEEE-style font settings
plt.rcParams.update({
    "font.size": 11,
    "font.family": "serif",
    "figure.dpi": 300
})

# Plot
fig, ax = plt.subplots(figsize=(7, 5))
bars1 = ax.bar(x, lan_times, width, label='LAN Component', color='lightblue', edgecolor='black', linewidth=1.0)
bars2 = ax.bar(x, wan_times, width, bottom=lan_times, label='WAN Overhead', color='lightcoral', edgecolor='black', linewidth=1.0)

# Labels and ticks
ax.set_ylabel('Query Time (seconds)')
ax.set_xlabel('CID Distribution (Host 1 - Host 2)')
ax.set_xticks(x)
ax.set_xticklabels(x_labels)
ax.legend(loc='upper left', fontsize=12)

# Remove top and right spines
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)

# Grid and layout
ax.grid(True, which="major", axis='both', linestyle='-', linewidth=0.5, color='gray', alpha=0.3)
ax.set_axisbelow(True)  # Put grid behind bars
plt.tight_layout()

# Save as high-res PDF
plt.savefig("mtdb_network_distribution_query_time.pdf", format="pdf", dpi=300)
plt.show()
