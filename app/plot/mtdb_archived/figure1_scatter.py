import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import LogLocator, LogFormatter

# --- IEEE Style Formatting ---
# This section sets up the plot to look more like a standard IEEE publication figure.
# It includes settings for font, line thickness, and marker size for a professional look.
plt.rcParams.update({
    "font.size": 11,
    "font.family": "serif",
    "axes.labelsize": 12,
    "xtick.labelsize": 10,
    "ytick.labelsize": 10,
    "legend.fontsize": 10,
    "figure.figsize": (6, 4.5),
    "axes.linewidth": 1.5,
})

# --- Dataset ---
x_labels = ['100M', '200M', '300M', '400M']
with_index_nosgx = [0.025, 0.027, 0.027, 0.028]
with_index_sgx = [0.033, 0.036, 0.036, 0.037]
without_index_nosgx = [9.118, 18.980, 28.529, 33.344]
without_index_sgx = [17.293, 35.275, 47.580, 62.818]

# --- Scatter Plot Setup ---
# Create a single figure and axes
fig, ax = plt.subplots()
x = np.arange(len(x_labels))  # the label locations

# Define colors and markers for scatter plot
colors = {
    'green': '#2E8B57',     # Sea green
    'blue': '#4169E1',      # Royal blue
    'orange': '#FF8C00',    # Dark orange
    'red': '#DC143C'        # Crimson
}

# Different markers for better distinction
markers = {
    'with_vanilla': 'o',      # Circle
    'with_sgx': 's',          # Square
    'without_vanilla': '^',   # Triangle up
    'without_sgx': 'D'        # Diamond
}

# Plot scatter points with larger marker sizes for visibility
ax.scatter(x, with_index_nosgx, marker=markers['with_vanilla'], s=120, 
          label='With Index (Vanilla)', color=colors['green'], 
          edgecolors='black', linewidth=1.5, alpha=0.8)

ax.scatter(x, with_index_sgx, marker=markers['with_sgx'], s=120, 
          label='With Index (SGX)', color=colors['blue'], 
          edgecolors='black', linewidth=1.5, alpha=0.8)

ax.scatter(x, without_index_nosgx, marker=markers['without_vanilla'], s=120, 
          label='Without Index (Vanilla)', color=colors['orange'], 
          edgecolors='black', linewidth=1.5, alpha=0.8)

ax.scatter(x, without_index_sgx, marker=markers['without_sgx'], s=120, 
          label='Without Index (SGX)', color=colors['red'], 
          edgecolors='black', linewidth=1.5, alpha=0.8)

# --- Axes and Labels ---
ax.set_yscale('log')
ax.set_xlabel("Database Size (Rows)")
ax.set_ylabel("Query Execution Time (seconds, log scale)")
ax.set_xticks(x)
ax.set_xticklabels(x_labels)

# --- Ultra-Clean Y-Axis (No Noise) ---
# Set explicit y-axis limits to control the range
ax.set_ylim(0.01, 200)

# Use LogLocator with specific base and subs to control tick placement
major_locator = LogLocator(base=10, subs=(1,), numdecs=4, numticks=15)
ax.yaxis.set_major_locator(major_locator)

# Set specific major ticks
ax.set_yticks([0.01, 0.1, 1, 10, 100])
ax.set_yticklabels(['0.01', '0.1', '1', '10', '100'])

# Completely disable minor ticks
ax.yaxis.set_minor_locator(plt.NullLocator())
ax.tick_params(axis='y', which='minor', size=0, width=0, labelsize=0)

# --- Legend and Grid ---
# Updated legend to be horizontal at the top, outside the plot area.
ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.15),
          ncol=2, frameon=False)
# Use a sparse grid on the major ticks for both axes.
ax.grid(True, which="major", axis='y', linestyle=':', linewidth=1, color='gray', alpha=0.3)

# --- Final Touches ---
ax.spines['right'].set_visible(False)
ax.spines['top'].set_visible(False)
ax.tick_params(width=1.5, which='major')

# Adjust layout to ensure all elements, including the legend, fit perfectly.
plt.tight_layout(pad=1.0)

# --- Save and Show ---
# Save the figure in high-resolution PDF format, ideal for papers.
plt.savefig("mtdb_query_execution_time_scatter.pdf", format='pdf', dpi=300, bbox_inches='tight')
plt.show()
