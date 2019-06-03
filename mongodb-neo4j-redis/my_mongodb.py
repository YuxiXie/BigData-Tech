import pymongo
from pymongo import MongoClient


with open('student.csv', 'r', encoding='utf-8') as f:
    data = f.read().strip().split('\n')
for index, line in enumerate(data):
    line = line.strip().split(';')
    line = [w.strip() for w in line]
    if index > 0:
        d = {}
        for i, w in enumerate(line):
            if w.startswith('"') and w.endswith('"'):
                line[i] = w.strip('"')
            else:
                line[i] = int(w)
            d[data[0][i]] = line[i]
        line = d
    data[index] = line
data = data[1:]

#连接MongoDB服务
client = MongoClient('localhost', 27017)
#选择数据库及集合
db = client.test
student = db.student
# 插入文档
rs = student.insert_many(data)
print('Multiple users: {0}'.format(rs.inserted_ids))
# 检索文档
student_tmp = student.find_one({'Mjob': 'at_home'})
print(student_tmp)
#条件查询
tmps = student.find({'romantic': 'yes'}).sort("id")
for tmp in tmps:
    print(tmp)
tmps = student.find({'age': {'$lt': 18}}).sort("name")
for tmp in tmps:
    print(tmp)
#更新文档
student.update({"id": "por605"}, {'$set': {"romantic": "yes"}})
# #删除文档
student.remove({"id": 'mat1'})
