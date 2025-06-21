import matplotlib.pyplot as plt
import numpy as np

# Set the bins for the histogram
bins = range(-800, 1700, 50)

# Pivot the data to get cumulative counts for each "grela_source"
cumulative_data = {}
for source in grela_works_metadata["grela_source"].unique():
    source_data = grela_works_metadata[grela_works_metadata["grela_source"] == source]["not_before"]
    cumulative_data[source], _ = np.histogram(source_data, bins=bins)

# Get the unique sources to maintain order
sources = grela_works_metadata["grela_source"].unique()

# Create the plot
fig, ax = plt.subplots(figsize=(10, 6))

# Plot each source's histogram, stacking them
bottom = np.zeros(len(bins) - 1)
for source in sources:
    ax.bar(
        bins[:-1], 
        cumulative_data[source], 
        width=50, 
        bottom=bottom, 
        label=source, 
        alpha=0.7
    )
    bottom += cumulative_data[source]

# Add labels and legend
ax.set_xlabel("Years")
ax.set_ylabel("Cumulative Count")
ax.set_title("Cumulative Histogram of Works by Source")
ax.legend(title="Grela Source")
plt.tight_layout()
plt.show()
