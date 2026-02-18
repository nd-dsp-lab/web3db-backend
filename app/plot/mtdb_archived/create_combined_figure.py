import matplotlib.pyplot as plt
import numpy as np

# IEEE publication style settings
plt.rcParams.update({
    'font.family': 'serif',
    'font.serif': ['Times New Roman', 'DejaVu Serif', 'serif'],
    'font.size': 8,
    'axes.labelsize': 9,
    'axes.titlesize': 10,
    'xtick.labelsize': 8,
    'ytick.labelsize': 8,
    'legend.fontsize': 7,
    'figure.titlesize': 10,
    'text.usetex': False,
    'axes.linewidth': 0.8,
    'grid.linewidth': 0.5,
    'lines.linewidth': 1.0,
})

def create_combined_performance_figure():
    """
    Create combined figure with three subfigures (a), (b), (c)
    """
    # Create figure with three subplots
    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(7.5, 2.5))
    
    # Colors - matching figure1_scatter.py
    colors = {
        'with_index_vanilla': '#2E8B57',    # Sea green
        'with_index_sgx': '#4169E1',        # Royal blue
        'without_index_vanilla': '#FF8C00',  # Dark orange
        'without_index_sgx': '#DC143C',     # Crimson
        'cid_performance': '#3498DB',
        'lan': '#2ECC71',
        'wan': '#E74C3C'
    }
    
    # (a) Query Execution Time vs Database Size - Scatter Plot
    x_labels = ['100M', '200M', '300M', '400M']
    with_index_nosgx = [0.025, 0.027, 0.027, 0.028]
    with_index_sgx = [0.033, 0.036, 0.036, 0.037]
    without_index_nosgx = [9.118, 18.980, 28.529, 33.344]
    without_index_sgx = [17.293, 35.275, 47.580, 62.818]
    
    x = np.arange(len(x_labels))
    
    # Different markers for better distinction
    markers = {
        'with_vanilla': 'o',      # Circle
        'with_sgx': 's',          # Square
        'without_vanilla': '^',   # Triangle up
        'without_sgx': 'D'        # Diamond
    }
    
    # Plot scatter points with appropriate sizes for subfigure
    ax1.scatter(x, with_index_nosgx, marker=markers['with_vanilla'], s=60, 
              label='With Index (Vanilla)', color=colors['with_index_vanilla'], 
              edgecolors='black', linewidth=1, alpha=0.8)
    
    ax1.scatter(x, with_index_sgx, marker=markers['with_sgx'], s=60, 
              label='With Index (SGX)', color=colors['with_index_sgx'], 
              edgecolors='black', linewidth=1, alpha=0.8)
    
    ax1.scatter(x, without_index_nosgx, marker=markers['without_vanilla'], s=60, 
              label='Without Index (Vanilla)', color=colors['without_index_vanilla'], 
              edgecolors='black', linewidth=1, alpha=0.8)
    
    ax1.scatter(x, without_index_sgx, marker=markers['without_sgx'], s=60, 
              label='Without Index (SGX)', color=colors['without_index_sgx'], 
              edgecolors='black', linewidth=1, alpha=0.8)
    
    ax1.set_xlabel('Database Size (Rows)', fontsize=8)
    ax1.set_ylabel('Query Time (seconds, log scale)', fontsize=8)
    ax1.set_title('(a) Query Execution Time', fontsize=9, fontweight='bold')
    ax1.set_xticks(x)
    ax1.set_xticklabels(x_labels)
    ax1.set_yscale('log')
    ax1.set_ylim(0.01, 100)
    # Set specific major ticks for log scale
    ax1.set_yticks([0.01, 0.1, 1, 10, 100])
    ax1.set_yticklabels(['0.01', '0.1', '1', '10', '100'])
    ax1.grid(True, alpha=0.3)
    # Place legend above the plot to avoid overlap with data points
    ax1.legend(fontsize=5, bbox_to_anchor=(0.5, 1.15), loc='center', ncol=2)
    
    # (b) Query Time vs Number of CIDs
    cids = [25, 50, 75, 100]
    query_times = [0.87, 1.52, 2.04, 2.56]
    
    ax2.plot(cids, query_times, marker='o', linewidth=2, markersize=6,
             color=colors['cid_performance'], label='Query Time')
    ax2.fill_between(cids, query_times, alpha=0.3, color=colors['cid_performance'])
    
    ax2.set_xlabel('Number of CIDs Retrieved', fontsize=8)
    ax2.set_ylabel('Query Time (seconds)', fontsize=8)
    ax2.set_title('(b) Scalability vs CIDs', fontsize=9, fontweight='bold')
    ax2.grid(True, alpha=0.3)
    ax2.legend(fontsize=6)
    
    # (c) Network Distribution Effects
    distributions = ['100-0', '75-25', '50-50', '25-75', '0-100']   
    local_times = [2.56, 2.56, 2.56, 2.56, 2.56]  # Local baseline
    lan_times = [2.56, 2.84, 3.12, 3.40, 3.64]
    wan_times = [2.56, 5.89, 8.32, 11.19, 14.07]
    
    x_dist = np.arange(len(distributions))
    
    ax3.plot(x_dist, lan_times, marker='s', linewidth=2, markersize=5,
             color=colors['lan'], label='LAN')
    ax3.plot(x_dist, wan_times, marker='^', linewidth=2, markersize=5,
             color=colors['wan'], label='WAN')
    
    ax3.set_xlabel('Distribution (Local-Remote)', fontsize=8)
    ax3.set_ylabel('Query Time (seconds)', fontsize=8)
    ax3.set_title('(c) Network Distribution', fontsize=9, fontweight='bold')
    ax3.set_xticks(x_dist)
    ax3.set_xticklabels(distributions, rotation=45)
    ax3.grid(True, alpha=0.3)
    ax3.legend(fontsize=6)
    
    # Adjust layout
    plt.tight_layout()
    
    return fig

# Generate the combined figure
if __name__ == "__main__":
    fig = create_combined_performance_figure()
    
    # Save in both PNG and PDF formats
    plt.savefig('combined_performance_evaluation.png', dpi=300, bbox_inches='tight', 
                facecolor='white', edgecolor='none')
    plt.savefig('combined_performance_evaluation.pdf', dpi=300, bbox_inches='tight', 
                facecolor='white', edgecolor='none')
    plt.show()
    
    print("Combined performance evaluation figure generated successfully!")
    print("Files saved:")
    print("- combined_performance_evaluation.png")
    print("- combined_performance_evaluation.pdf")
    print("\nThis figure combines:")
    print("(a) Query execution time vs database size")
    print("(b) Query scalability vs number of CIDs")
    print("(c) Network distribution effects")
    print("\nSpace saved: 3 figures → 1 figure with subfigures")
