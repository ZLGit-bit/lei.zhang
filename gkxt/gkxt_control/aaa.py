import pandas as pd

l = [{"id":"1","name":"2"}]
l2 =  [{"id":1,"name":2}]

p1 =pd.DataFrame(l)
p2=pd.DataFrame(l2)
print p1
print p2
print p1.merge(p2,on=['id'],how='inner')
