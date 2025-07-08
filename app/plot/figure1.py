import matplotlib.pyplot as plt
import numpy as np

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
with_index_nosgx = [0.025, 0.025, 0.025, 0.025]
with_index_sgx = [0.175, 0.175, 0.202, 0.210]
without_index_nosgx = [8.91, 25.12, 38.85, 50.21]
without_index_sgx = [23.64, 50.21, 79.43, 100.35]

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
ax.bar(pos1, with_index_nosgx, width, label='With Index (No SGX)', color=colors['green'])
ax.bar(pos2, with_index_sgx, width, label='With Index (SGX)', color=colors['blue'])
ax.bar(pos3, without_index_nosgx, width, label='Without Index (No SGX)', color=colors['orange'])
ax.bar(pos4, without_index_sgx, width, label='Without Index (SGX)', color=colors['red'])


# --- Axes and Labels ---
ax.set_yscale('log')
ax.set_xlabel("Database Size (Rows)")
ax.set_ylabel("Query Execution Time (seconds, log scale)")
ax.set_xticks(x)
ax.set_xticklabels(x_labels)

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
ax.tick_params(width=1.0, which='minor')

# Adjust layout to ensure all elements, including the legend, fit perfectly.
plt.tight_layout(pad=1.0)

# --- Save and Show ---
# Save the figure in high-resolution PDF format, ideal for papers.
plt.savefig("mtdb_query_execution_time.pdf", format='pdf', dpi=300, bbox_inches='tight')
plt.show()