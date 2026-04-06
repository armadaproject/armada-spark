from natsort import natsorted
import matplotlib.pyplot as plt

# Function to parse a file using the first 2 CSV columns as key/value
def parse_table(filename):
    data = {}
    with open(filename, 'r') as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            cols = line.split(',')
            # Remove the suffix (e.g., "-v2.4") from the key
            clean_key = cols[0].split('-')[0]
            data[clean_key] = float(cols[1])
    return data

# Read data from files
dynamic_data = parse_table('/home/gbj/results/k8s/r1')
armada_data = parse_table('/home/gbj/results/armada/r1')

# Find keys common to both tables
all_keys = [set(dynamic_data.keys()), set(armada_data.keys())]
common_keys = natsorted(set.intersection(*all_keys))

# Report keys that are not common
for i, table_name in enumerate(['Dynamic K8S', 'Armada']):
    table_keys = all_keys[i]
    uncommon_keys = table_keys - set(common_keys)
    if uncommon_keys:
        print(f"Keys in {table_name} but not in all tables: {', '.join(uncommon_keys)}")

# Function to plot data with no more than 34 points each
def plot_data_in_chunks(common_keys, chunk_size=34):
    plot_number = 1
    for i in range(0, len(common_keys), chunk_size):
        chunk_keys = common_keys[i:i + chunk_size]

        # Prepare the plot
        plt.figure(figsize=(10, 6))

        # Plot each table's data using only the chunk of common keys
        for label, data in zip(['Dynamic K8S', 'Armada'], [dynamic_data, armada_data]):
            x = [key for key in chunk_keys]
            y = [data[key] for key in chunk_keys]
            plt.plot(x, y, marker='o', label=label)  # Using marker='o' for better visibility of points

        # Customize the plot
        plt.xlabel('Queries')
        plt.ylabel('Seconds')
        plt.title(f'C3 Benchmarks - Plot {plot_number}')
        plt.legend()
        plt.grid(True)
        
        # Display the plot
        plt.show()

        # Increment the plot number
        plot_number += 1

# Plot data in chunks of up to 34 keys
plot_data_in_chunks(common_keys, chunk_size=35)
