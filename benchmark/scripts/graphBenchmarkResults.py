import sys
from natsort import natsorted
import matplotlib.pyplot as plt

if len(sys.argv) != 3:
    print(f"Usage: {sys.argv[0]} <benchmarkResults1.csv> <benchmarkResults2.csv>")
    sys.exit(1)

file1 = sys.argv[1]
file2 = sys.argv[2]

# Function to parse a file using the first 2 CSV columns as key/value,
# and the last column as a hash
def parse_table(filename):
    data = {}
    hashes = {}
    with open(filename, 'r') as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            cols = line.split(',')
            # Remove the suffix (e.g., "-v2.4") from the key
            clean_key = cols[0].split('-')[0]
            data[clean_key] = float(cols[1])
            hashes[clean_key] = cols[-1]
    return data, hashes

# Read data from files
data1, hashes1 = parse_table(file1)
data2, hashes2 = parse_table(file2)

# Find keys common to both tables
all_keys = [set(data1.keys()), set(data2.keys())]
common_keys = natsorted(set.intersection(*all_keys))

# Report keys that are not common
for i, filename in enumerate([file1, file2]):
    table_keys = all_keys[i]
    uncommon_keys = table_keys - set(common_keys)
    if uncommon_keys:
        print(f"Keys in {filename} but not in the other: {', '.join(uncommon_keys)}")

# Compare hash values for common keys
mismatches = []
for key in common_keys:
    if hashes1[key] != hashes2[key]:
        mismatches.append((key, hashes1[key], hashes2[key]))

if mismatches:
    print(f"\nHash mismatches ({len(mismatches)}):")
    for key, h1, h2 in mismatches:
        print(f"  {key}: {file1}={h1}  {file2}={h2}")
else:
    print("All hashes match.")

# Function to plot data with no more than 34 points each
def plot_data_in_chunks(common_keys, chunk_size=34):
    plot_number = 1
    for i in range(0, len(common_keys), chunk_size):
        chunk_keys = common_keys[i:i + chunk_size]

        # Prepare the plot
        plt.figure(figsize=(10, 6))

        # Plot each table's data using only the chunk of common keys
        for label, data in zip([file1, file2], [data1, data2]):
            x = [key for key in chunk_keys]
            y = [data[key] for key in chunk_keys]
            plt.plot(x, y, marker='o', label=label)

        # Customize the plot
        plt.xlabel('Queries')
        plt.ylabel('Seconds')
        plt.title(f'Benchmarks - Plot {plot_number}')
        plt.legend()
        plt.grid(True)

        # Display the plot
        plt.show()

        # Increment the plot number
        plot_number += 1

# Plot data in chunks of up to 34 keys
plot_data_in_chunks(common_keys, chunk_size=35)
