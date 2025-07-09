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
with_index_nosgx = [0.025, 0.027, 0.027, 0.025]
with_index_sgx = [0.036, 0.033, 0.033, 0.037]
without_index_nosgx = [9.118, 18.980, 28.529, 33.344]
without_index_sgx = [17.293, 35.275, 47.580, 62.818]

# --- Grouped Bar Chart Setup ---
# Create a single figure and axes
fig, ax = plt.subplots()
x = np.arange(len(x_labels))  # the label locations
width = 0.2  # the width of the bars

# Define colors based on a standard, clear palette similar to the reference
colors = {
    'green': '#90EE90',     # Light green
    'blue': '#ADD8E6',      # Light blue
    'orange': '#FFB366',    # Light orange
    'red': '#FF9999'        # Light red
}

# --- Plot all bars on the single axes ---
# Calculate positions for each bar in the group
pos1 = x - 1.5 * width
pos2 = x - 0.5 * width
pos3 = x + 0.5 * width
pos4 = x + 1.5 * width

# Plot the bars
ax.bar(pos1, with_index_nosgx, width, label='With Index (Vanilla)', color=colors['green'])
ax.bar(pos2, with_index_sgx, width, label='With Index (SGX)', color=colors['blue'])
ax.bar(pos3, without_index_nosgx, width, label='Without Index (Vanilla)', color=colors['orange'])
ax.bar(pos4, without_index_sgx, width, label='Without Index (SGX)', color=colors['red'])

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
plt.savefig("mtdb_query_execution_time_bar.pdf", format='pdf', dpi=300, bbox_inches='tight')
plt.show()