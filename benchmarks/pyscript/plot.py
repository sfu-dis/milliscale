import os
import pandas as pd
import matplotlib.pyplot as plt

def plot_combined_metric(titles, metric, base_dir='./'):
    plt.figure(figsize=(10, 6))

    combined_data = {}

    for title in titles:
        file_path = os.path.join(base_dir, title, 'summary.csv')
        if not os.path.exists(file_path):
            print(f"[Error] File not exist: {file_path}")
            continue

        df = pd.read_csv(file_path)
        if metric not in df.columns:
            print(f"[Error] '{metric}' is not in: {file_path}")
            continue

        combined_data[title] = df[metric].values

    if combined_data:
        sample_title = titles[0]
        sample_path = os.path.join(base_dir, sample_title, 'summary.csv')
        threads = pd.read_csv(sample_path)['threads'].values

        for title, values in combined_data.items():
            plt.plot(threads, values, marker='o', label=title)

        plt.xlabel('Threads')
        plt.ylabel(metric)
        plt.title(f'{metric} vs Threads (Combined)')
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.show()
    else:
        print("!!!!!!!!!!!!!!! No Data")

titles = ['sync-ycsb-update-ssd', 'sync-ycsb-update-onezone', 'sync-ycsb-update-ebs']
metric = 'avg_latency_ms'
plot_combined_metric(titles, metric)
