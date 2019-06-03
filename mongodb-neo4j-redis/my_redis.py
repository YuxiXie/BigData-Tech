import redis


with open('student.csv', 'r', encoding='utf-8') as f:
    data = f.read().strip().split('\n')
data_dict = {}
head = data[0].strip().split(';')
for h in head[1:]:
    data_dict[h] = {}
data = data[1:]
id_list = []
for index, line in enumerate(data):
    line = line.strip().split(';')
    line = [w.strip().strip('"') for w in line]
    id = line[0]
    id_list.append(id)
    for i, w in enumerate(line[1:]):
        data_dict[head[i+1]][id] = w


#连接redis
r = redis.Redis(host='127.0.0.1', port=6379, db=0)
#查看服务运行状态
print(r.ping())

for id in id_list:
    r.lpush('id', id)

for k, v in data_dict.items():
    r.hmset(k, v)

list = r.lrange("id", 0, 2);
for i in list:
   print(i)
