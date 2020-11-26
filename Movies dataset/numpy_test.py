import numpy as np 
import collections 
import time 

start = time.perf_counter()

dataset = np.loadtxt('./Movies dataset/ml-100k/u.data')
dataset = dataset.astype('uint8')
ratings = dataset[:,2]

ratingCounter = collections.Counter(ratings)

for key, val in sorted(ratingCounter.items()):
    print("{}, {}".format(key, val))
    
print("time elapsed: ", time.perf_counter() - start , " secs")