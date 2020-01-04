import os
import pandas as pd
import matplotlib.pyplot as plt

fig = plt.figure(frameon=False)
fig.set_size_inches(16,9)

logs_dir = 'logs'
log_files = sorted(os.listdir(logs_dir))
save_dir = 'plots'
os.makedirs(save_dir, exist_ok=True)




## Analyze Sequential Reading
print('Analyzing logs for Exp1: Sequential Reading')
log_seq_files = [x for x in log_files if 'sequential' in x]
for log_seq_file in log_seq_files:
    file_path = os.path.join(logs_dir, log_seq_file)
    df = pd.read_csv(file_path)
    legend_label = file_path.replace('.csv','')
    tokens = legend_label.split('_')
    if len(tokens) == 4:
        label = tokens[1] + '_' + tokens[-1]
    else:
        label = tokens[1]
    plt.plot(df['file_size'],df['avg_time'], label=label)
plt.xlabel('file_size')
plt.ylabel('time (ms)')
plt.title('Performance of Different Read Streams for Sequential Reading')
plt.legend(loc="upper left")
save_path = os.path.join(save_dir, 'exp1_sequential_reading.png')
plt.savefig(save_path)
print(f"plots saved in {save_path}")

## Analyze Random Reading Plots
print('\nAnalyzing logs for Exp2: Random Reading')
log_seq_files = [x for x in log_files if 'random' in x]
jumps = [100,1000,10000]
for jump in jumps:
    fig = plt.figure(frameon=False)
    fig.set_size_inches(16,9)
    for log_seq_file in log_seq_files:
        file_path = os.path.join(logs_dir, log_seq_file)
        df = pd.read_csv(file_path)
        legend_label = file_path.replace('.csv','')
        tokens = legend_label.split('_')
        if len(tokens) == 4:
            label = tokens[1] + '_' + tokens[-1]
        else:
            label = tokens[1]
        if label == 'byte':
            plt.plot(df['file_size'][:5],df[f'avg_time_{jump}'][:5], label=label)
        else:
            plt.plot(df['file_size'],df[f'avg_time_{jump}'], label=label)
    plt.xlabel('file_size')
    plt.ylabel('time (ms)')
    plt.title(f'Performance of Different Read Streams for Random Reading (NUM_READS={jump})')
    plt.legend(loc="upper left")
    save_path = os.path.join(save_dir, f'exp2_random_reading_{jump}.png')
    plt.savefig(save_path)
    print(f"plots saved in {save_path}")
    

