import pymongo
from pymongo import MongoClient


client = MongoClient('localhost', 27017)
db = client.test
yago = db.yago

data_list = []
with open('yagoThreeSimplified.txt', 'r', encoding='utf-8') as f:
    data = f.read().strip().split('\n')
for index, dt in enumerate(data):
    d = dt.strip().split(' ')
    if len(d) != 3:
        print(d)
    else:
        s, p, o = d[0], d[1], d[2]
        d = {'subject': s, 'predicate': p, 'object': o}
        data_list.append(d)

rs = yago.insert_many(data_list)
print('import yago dataset into MongoDB')

'''(1) 给定一个si，给出它所有的P和O，<si, P, O>'''
samples = yago.find({'subject': 'Richard_Stallman'})
for sample in samples:
    print(sample['predicate'], sample['object'])

'''(2) 给定一个oi, 给出它所有的S和P，<S, P,oi>'''
samples = yago.find({'object': '9th_New_York_Heavy_Artillery_Regiment'})
for sample in samples:
    print(sample['predicate'], sample['subject'])

'''(3) 给定两个p1,p2, 给出同时拥有它们的S，<S, p1, *>, <S, p2, *>'''
samples1 = yago.find({'predicate': 'owns'})
samples2 = yago.find({'predicate': 'isLeaderOf'})
samples1 = [sample['subject'] for sample in samples1]
samples2 = [sample['subject'] for sample in samples2]
samples = []
for s in samples1:
    if s in samples2 and s not in samples:
        samples.append(s)
print('; '.join(samples))

'''(4) 给定一个oi, 给出拥有这样oi最多的S'''
samples = yago.find({'object': '9th_New_York_Heavy_Artillery_Regiment'})
s_list = {}
for sample in samples:
    s = sample['subject']
    if s not in s_list:
        s_list[s] = 0
    s_list[s] += 1
s_list = [(k, v) for k, v in s_list.items()]
s_list.sort(key=lambda x:-x[1])
print(s_list[0][0])
