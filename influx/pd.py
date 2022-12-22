import pandas as pd
a = [None, 1, 7, 2, None, 4, 7, None]
myvar = pd.Series(a)
myvar.interpolate(inplace=True)
print(myvar)