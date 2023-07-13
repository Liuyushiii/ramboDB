import numpy as np
import matplotlib.pyplot as plt
plt.style.use(['science','ieee'])

arr1 = [1209.836, 1319.821091, 1281.002824, 1319.821091, 1319.821091, 1350.514605, 1340.126031, 1350.514605]
arr2 = [7259.016, 14518.032, 21777.048, 29036.064, 43554.096, 58072.128, 87108.192, 116144.256]
arr0 = [8, 16 ,24 ,32 ,48 ,64 ,96 ,128]
def model(x, p):
   return x ** (2 * p + 1) / (1 + x ** (2 * p))
x = np.linspace(0.75, 1.25, 201)

with plt.style.context(['science', 'ieee']):
    fig, ax = plt.subplots(figsize=(4,2))
    ax.plot(arr0, arr2, '--', label='HyperLedger Fabric', color='blue')
    ax.plot(arr0, arr1, '-', label='BFT-Store', color='black')
    ax.plot(0, 1150)
    ax.set_yscale('log', base=2)
    ax.legend(title='Block Storage of System')
    ax.set(xlabel='Number of Nodes')
    ax.set(ylabel='Storage Overhead(GB)')
    ax.autoscale(tight=True)
    fig.savefig('figures/fig2.pdf')
    fig.savefig('figures/fig2.jpg', dpi=300)