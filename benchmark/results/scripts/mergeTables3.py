def read_table(file_path):
    table = {}
    with open(file_path, 'r') as file:
        for line in file:
            key, value = line.strip().split(',')
            table[key] = int(value)
    return table

def merge_tables(dynamic, non_dynamic, non_shuffle, armada):
    # Create a set of all keys from each table
    all_keys = set(dynamic.keys()).union(non_dynamic.keys(), non_shuffle.keys(), armada.keys())

    # Prepare the header
    print(f"{'Key':<10} | {'Dynamic':<10} | {'Static K8S ':<12} | {'Armada':<10}")
    print("-" * 64)

    # Iterate over all keys and print values from each table or -1 if missing
    for key in sorted(all_keys):
        dynamic_value = dynamic.get(key, -1)
        non_dynamic_value = non_dynamic.get(key, -1)
        non_shuffle_value = non_shuffle.get(key, -1)
        armada_value = armada.get(key, -1)

        print(f"{key:<10} | {dynamic_value:<10} | {non_dynamic_value:<12} | {armada_value:<10}")

# File paths
dynamic_file = 'dynamic'
non_dynamic_file = 'non-dynamic.txt'
non_shuffle_file = 'non-shuffle.txt'
armada_file = 'a28'

# Read tables from files
dynamic = read_table(dynamic_file)
non_dynamic = read_table(non_dynamic_file)
non_shuffle = read_table(non_shuffle_file)
armada = read_table(armada_file)

# Call the function with the data read from files
merge_tables(dynamic, non_dynamic, non_shuffle, armada)
