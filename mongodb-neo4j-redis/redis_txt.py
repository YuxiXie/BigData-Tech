import redis


s_dict, o_dict, p_dict = {}, {}, {}
with open('yagoThreeSimplifiedShort.txt', 'r', encoding='utf-8') as f:
    for line in f.readlines():
        d = line.strip().split(' ')
        if len(d) != 3:
            print(d)
        else:
            s, p, o = d[0], d[1], d[2]
            if s not in s_dict:
                s_dict[s] = []
            s_dict[s].append(p + ' ' + o)
            if o not in o_dict:
                o_dict[o] = []
            o_dict[o].append(p + ' ' + s)
            if p not in p_dict:
                p_dict[p] = []
            p_dict[p].append(s)

r = redis.Redis(host='127.0.0.1', port=6379, db=0)
print(r.ping())

for k, v in s_dict.items():
    for vv in v:
        r.rpush('[subject]' + k, vv)
for k, v in o_dict.items():
    for vv in v:
        r.rpush('[object]' + k, vv)
for k, v in p_dict.items():
    for vv in v:
        r.sadd('[predicate]' + k, vv)

'''(1) 给定一个si，给出它所有的P和O，<si, P, O>'''
len_po = r.llen('[subject]Richard_Stallman')
list_po = r.lrange('[subject]Richard_Stallman', 0, len_po - 1)
list_po = [po.decode(encoding='utf-8') for po in list_po]
print('; '.join(list_po))

'''(2) 给定一个oi, 给出它所有的S和P，<S, P, oi>'''
len_ps = r.llen('[subject]Richard_Stallman')
list_ps = r.lrange('[object]9th_New_York_Heavy_Artillery_Regiment', 0, len_ps - 1)
list_ps = [ps.decode(encoding='utf-8') for ps in list_ps]
print('; '.join(list_ps))

'''(3) 给定两个p1,p2, 给出同时拥有它们的S，<S, p1, *>, <S, p2, *>'''
Ss = r.sinter(['[predicate]owns', '[predicate]isLeaderOf'])
print([s.decode(encoding='utf-8') for s in Ss])

'''(4) 给定一个oi, 给出拥有这样oi最多的S'''
len_ps = r.llen('[subject]Richard_Stallman')
list_ps = r.lrange('[object]9th_New_York_Heavy_Artillery_Regiment', 0, len_ps - 1)
s_dict = {}
for ps in list_ps:
    ps = ps.decode(encoding='utf-8')
    s = ps.split(' ')
    s = s[1]
    if s not in s_dict:
        s_dict[s] = 0
    s_dict[s] += 1
s_dict = [(k, v) for k, v in s_dict.items()]
s_dict.sort(key=lambda x:-x[1])
print(s_dict[0][0])
