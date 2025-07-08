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
cids_returned = [25, 50, 75, 100]
execution_time = [0.8664239, 1.4923036, 2.0516290, 2.5640152]

# --- Bar Chart Setup ---
# Create a single figure and axes
fig, ax = plt.subplots()

# --- Plot the bars on the single axes ---
# ax.bar(cids_returned, execution_time, width=15, color='#f5a623', align='center')
ax.bar(cids_returned, execution_time, width=15, color='#ADD8E6', align='center')


# --- Axes and Labels ---
ax.set_xlabel("Number of CIDs Returned")
ax.set_ylabel("Query Execution Time (seconds)")
ax.set_xticks(cids_returned) # Set ticks to match the data points

# --- Grid and Annotation ---
# Use a subtle, dotted grid on the y-axis only
ax.grid(True, which="major", axis='y', linestyle=':', linewidth=1, color='gray', alpha = 0.3)

# Add the text annotation inside the plot
ax.text(27, 2.4, "Each CID chunk = 100K rows (11.848 Mbits)", fontsize=10, verticalalignment='top')


# --- Final Touches ---
ax.spines['right'].set_visible(False)
ax.spines['top'].set_visible(False)
ax.tick_params(width=1.5, which='major')
ax.tick_params(width=1.0, which='minor')

# Set y-axis limits to give some space
ax.set_ylim(bottom=0)


# Adjust layout to ensure all elements fit perfectly.
plt.tight_layout(pad=1.0)

# --- Save and Show ---
# Save the figure in high-resolution PDF format, ideal for papers.
plt.savefig("mtdb_query_execution_vs_cids.pdf", format='pdf', dpi=300, bbox_inches='tight')
plt.show()
