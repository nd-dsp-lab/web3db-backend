import matplotlib.pyplot as plt
import numpy as np

# IEEE publication style settings
plt.rcParams.update({
    'font.family': 'serif',
    'font.serif': ['Times New Roman', 'DejaVu Serif', 'serif'],
    'font.size': 8,
    'axes.labelsize': 11,
    'axes.titlesize': 12,
    'xtick.labelsize': 10,
    'ytick.labelsize': 10,
    'legend.fontsize': 7,
    'figure.titlesize': 10,
    'text.usetex': False,
    'axes.linewidth': 0.8,
    'grid.linewidth': 0.5,
    'lines.linewidth': 1.0,
})

def create_four_panel_figure():
    """
    Create the exact 4-panel figure as shown in the image:
    (a) With Index, (b) Without Index, (c) Scalability vs CIDs, (d) Network Distribution
    """
    # Create figure with four subplots in a row
    fig, (ax1, ax2, ax3, ax4) = plt.subplots(1, 4, figsize=(15, 3.5))
    
    # Data for all subfigures
    x_labels = ['100M', '200M', '300M', '400M']
    x = np.arange(len(x_labels))
    width = 0.35
    
    # (a) With Index - Bar Chart
    with_index_vanilla = [0.025, 0.027, 0.027, 0.028]
    with_index_sgx = [0.033, 0.036, 0.036, 0.037]
    
    x1 = x - width/2
    x2 = x + width/2
    
    ax1.bar(x1, with_index_vanilla, width, label='Vanilla', color='#98FB98', 
            edgecolor='black', linewidth=0.5)  # Light green
    ax1.bar(x2, with_index_sgx, width, label='SGX (MtDB)', color='#ADD8E6', 
            edgecolor='black', linewidth=0.5)  # Light blue
    
    ax1.set_xlabel('Database size (rows)', fontsize=11)
    ax1.set_ylabel('Query time (seconds)', fontsize=11)
#     ax1.set_title('(a) With index', fontsize=12, fontweight='bold')
    ax1.set_xticks(x)
    ax1.set_xticklabels(x_labels)
    ax1.legend(fontsize=7, loc='upper left')
    ax1.grid(True, alpha=0.3, axis='y')
    ax1.set_ylim(0, 0.045)
    
    # (b) Without Index - Bar Chart
    without_index_vanilla = [9.118, 18.980, 28.529, 33.344]
    without_index_sgx = [17.293, 35.275, 47.580, 62.818]
    
    ax2.bar(x1, without_index_vanilla, width, label='Vanilla', color='#F0E68C', 
            edgecolor='black', linewidth=0.5)
    ax2.bar(x2, without_index_sgx, width, label='SGX', color='#FFB6C1', 
            edgecolor='black', linewidth=0.5)
    
    ax2.set_xlabel('Database size (rows)', fontsize=11)
    ax2.set_ylabel('Query time (seconds)', fontsize=11)
#     ax2.set_title('(b) Without index', fontsize=12, fontweight='bold')
    ax2.set_xticks(x)
    ax2.set_xticklabels(x_labels)
    ax2.legend(fontsize=7, loc='upper left')
    ax2.grid(True, alpha=0.3, axis='y')
    ax2.set_ylim(0, 70)
    
    # (c) Scalability vs CIDs - Line Chart with Fill
    cids = [25, 50, 75, 100]
    query_times = [0.87, 1.52, 2.04, 2.56]
    
    ax3.plot(cids, query_times, marker='o', linewidth=3, markersize=6,
             color='#87CEEB', label='Query time',
             markerfacecolor='white', markeredgecolor='#87CEEB', markeredgewidth=1.5)  # Hollow circles
    ax3.fill_between(cids, query_times, alpha=0.3, color='#B0E0E6')  # Light blue fill
    
    ax3.set_xlabel('Number of CIDs retrieved', fontsize=11)
    ax3.set_ylabel('Query time (seconds)', fontsize=11)
#     ax3.set_title('(c) Scalability vs CIDs', fontsize=12, fontweight='bold')
    ax3.grid(True, alpha=0.3)
    ax3.legend(fontsize=7, loc='upper left')
    ax3.set_ylim(0, 2.8)
    ax3.set_xlim(20, 105)
    ax3.set_xticks([25, 50, 75, 100])  # Fix x-axis ticks
    
    # (d) Network Distribution - Two Line Chart
    distributions = ['100-0', '75-25', '50-50', '25-75', '0-100']   
    lan_times = [2.56, 2.84, 3.12, 3.40, 3.64]
    wan_times = [2.56, 5.89, 8.32, 11.19, 14.07]
    
    x_dist = np.arange(len(distributions))
    
    ax4.plot(x_dist, lan_times, marker='s', linewidth=3, markersize=5,
             color='#90EE90', label='LAN', linestyle='-', 
             markerfacecolor='white', markeredgecolor='#90EE90', markeredgewidth=1.5)  # Hollow squares
    ax4.plot(x_dist, wan_times, marker='^', linewidth=3, markersize=5,
             color='#FFB6C1', label='WAN', linestyle='-',
             markerfacecolor='white', markeredgecolor='#FFB6C1', markeredgewidth=1.5)  # Hollow triangles
    
    ax4.set_xlabel('Distribution (local-remote)', fontsize=11)
    ax4.set_ylabel('Query time (seconds)', fontsize=11)
#     ax4.set_title('(d) Network distribution', fontsize=12, fontweight='bold')
    ax4.set_xticks(x_dist)
    ax4.set_xticklabels(distributions, rotation=45, fontsize=10)
    ax4.grid(True, alpha=0.3)
    ax4.legend(fontsize=7, loc='upper left')
    ax4.set_ylim(2, 15)  # Start y-axis from 2
    
    # Remove top and right spines for cleaner look
    for ax in [ax1, ax2, ax3, ax4]:
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
    
    # Adjust layout with proper spacing
    plt.tight_layout(pad=2.0)
    
    return fig

def create_individual_panels():
    """
    Create individual panel figures for subcaption usage
    """
    # Panel (a) - With Index
    fig_a, ax1 = plt.subplots(1, 1, figsize=(3.5, 2.5))
    
    x_labels = ['100M', '200M', '300M', '400M']
    x = np.arange(len(x_labels))
    width = 0.35
    with_index_vanilla = [0.025, 0.027, 0.027, 0.028]
    with_index_sgx = [0.033, 0.036, 0.036, 0.037]
    x1 = x - width/2
    x2 = x + width/2
    
    ax1.bar(x1, with_index_vanilla, width, label='Vanilla', color='#98FB98', 
            edgecolor='black', linewidth=0.5)
    ax1.bar(x2, with_index_sgx, width, label='SGX (MtDB)', color='#ADD8E6', 
            edgecolor='black', linewidth=0.5)
    ax1.set_xlabel('Database size (rows)', fontsize=12)
    ax1.set_ylabel('Query time (seconds)', fontsize=12)
    ax1.set_xticks(x)
    ax1.set_xticklabels(x_labels)
    ax1.legend(fontsize=7, loc='upper left')
    ax1.grid(True, alpha=0.3, axis='y')
    ax1.set_ylim(0, 0.045)
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)
    plt.tight_layout()
    plt.savefig('performance_evaluation_four_panels_a.pdf', dpi=300, bbox_inches='tight', 
                facecolor='white', edgecolor='none')
    plt.close(fig_a)
    
    # Panel (b) - Without Index
    fig_b, ax2 = plt.subplots(1, 1, figsize=(3.5, 2.5))
    
    without_index_vanilla = [9.118, 18.980, 28.529, 33.344]
    without_index_sgx = [17.293, 35.275, 47.580, 62.818]
    
    ax2.bar(x1, without_index_vanilla, width, label='Vanilla', color='#F0E68C', 
            edgecolor='black', linewidth=0.5)
    ax2.bar(x2, without_index_sgx, width, label='SGX', color='#FFB6C1', 
            edgecolor='black', linewidth=0.5)
    ax2.set_xlabel('Database size (rows)', fontsize=12)
    ax2.set_ylabel('Query time (seconds)', fontsize=12)
    ax2.set_xticks(x)
    ax2.set_xticklabels(x_labels)
    ax2.legend(fontsize=7, loc='upper left')
    ax2.grid(True, alpha=0.3, axis='y')
    ax2.set_ylim(0, 70)
    ax2.spines['top'].set_visible(False)
    ax2.spines['right'].set_visible(False)
    plt.tight_layout()
    plt.savefig('performance_evaluation_four_panels_b.pdf', dpi=300, bbox_inches='tight', 
                facecolor='white', edgecolor='none')
    plt.close(fig_b)
    
    # Panel (c) - Scalability vs CIDs
    fig_c, ax3 = plt.subplots(1, 1, figsize=(3.5, 2.5))
    
    cids = [25, 50, 75, 100]
    query_times = [0.87, 1.52, 2.04, 2.56]
    
    ax3.plot(cids, query_times, marker='o', linewidth=3, markersize=6,
             color='#87CEEB', label='Query time',
             markerfacecolor='white', markeredgecolor='#87CEEB', markeredgewidth=1.5)
    ax3.fill_between(cids, query_times, alpha=0.3, color='#B0E0E6')
    ax3.set_xlabel('Number of CIDs retrieved', fontsize=12)
    ax3.set_ylabel('Query time (seconds)', fontsize=12)
    ax3.grid(True, alpha=0.3)
    ax3.legend(fontsize=7, loc='upper left')
    ax3.set_ylim(0, 2.8)
    ax3.set_xlim(20, 105)
    ax3.set_xticks([25, 50, 75, 100])
    ax3.spines['top'].set_visible(False)
    ax3.spines['right'].set_visible(False)
    plt.tight_layout()
    plt.savefig('performance_evaluation_four_panels_c.pdf', dpi=300, bbox_inches='tight', 
                facecolor='white', edgecolor='none')
    plt.close(fig_c)
    
    # Panel (d) - Network Distribution
    fig_d, ax4 = plt.subplots(1, 1, figsize=(3.5, 2.5))
    
    distributions = ['100-0', '75-25', '50-50', '25-75', '0-100']   
    lan_times = [2.56, 2.84, 3.12, 3.40, 3.64]
    wan_times = [2.56, 5.89, 8.32, 11.19, 14.07]
    x_dist = np.arange(len(distributions))
    
    ax4.plot(x_dist, lan_times, marker='s', linewidth=3, markersize=5,
             color='#90EE90', label='LAN', linestyle='-', 
             markerfacecolor='white', markeredgecolor='#90EE90', markeredgewidth=1.5)
    ax4.plot(x_dist, wan_times, marker='^', linewidth=3, markersize=5,
             color='#FFB6C1', label='WAN', linestyle='-',
             markerfacecolor='white', markeredgecolor='#FFB6C1', markeredgewidth=1.5)
    ax4.set_xlabel('CID distribution (local-remote)', fontsize=12)
    ax4.set_ylabel('Query time (seconds)', fontsize=12)
    ax4.set_xticks(x_dist)
    ax4.set_xticklabels(distributions, rotation=45, fontsize=10)
    ax4.grid(True, alpha=0.3)
    ax4.legend(fontsize=7, loc='upper left')
    ax4.set_ylim(2, 15)
    ax4.spines['top'].set_visible(False)
    ax4.spines['right'].set_visible(False)
    plt.tight_layout()
    plt.savefig('performance_evaluation_four_panels_d.pdf', dpi=300, bbox_inches='tight', 
                facecolor='white', edgecolor='none')
    plt.close(fig_d)

# Generate the four-panel figure
if __name__ == "__main__":
    fig = create_four_panel_figure()
    
    # Save the complete combined figure
    plt.savefig('performance_evaluation_four_panels.png', dpi=300, bbox_inches='tight', 
                facecolor='white', edgecolor='none')
    plt.savefig('performance_evaluation_four_panels.pdf', dpi=300, bbox_inches='tight', 
                facecolor='white', edgecolor='none')
    
    # Save individual panels for subcaption usage
    fig_individual = create_individual_panels()
    
    plt.show()
    
    print("Four-panel performance evaluation figure generated successfully!")
    print("Files saved:")
    print("- performance_evaluation_four_panels.png")
    print("- performance_evaluation_four_panels.pdf")
    print("- performance_evaluation_four_panels_a.pdf (With Index)")
    print("- performance_evaluation_four_panels_b.pdf (Without Index)")
    print("- performance_evaluation_four_panels_c.pdf (Scalability vs CIDs)")
    print("- performance_evaluation_four_panels_d.pdf (Network Distribution)")
    print("\nThis figure includes:")
    print("(a) With Index - Query execution time with indexing (Vanilla vs SGX)")
    print("(b) Without Index - Query execution time without indexing (Vanilla vs SGX)")
    print("(c) Scalability vs CIDs - Performance vs number of CIDs retrieved")
    print("(d) Network Distribution - LAN vs WAN performance comparison")
