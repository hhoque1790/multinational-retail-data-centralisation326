import pandas as pd 
import numpy as np


import pickle
# df=pd.DataFrame({"A":[1,2],"B":[3,4]})
# df.to_pickle("lol")

df = pd.read_pickle("storedatafile")
print(df)


#EXPERIMENT 2 
# from tqdm import tqdm

# # You can add this to iterators like lists as well
# lst = [1,2,3,4,5,6,7,8]
# for i in tqdm(lst):
#     print(i)