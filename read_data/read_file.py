import pandas as pd

data = pd.read_csv('原始点导出.csv', header=None, encoding='utf-8')
print(data[0])
