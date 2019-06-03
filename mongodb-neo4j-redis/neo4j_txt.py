from neo4j import GraphDatabase


with open('yagoThreeSimplifiedShort.txt', 'r', encoding='utf-8') as f:
    data = f.read().strip().split('\n')
s_list, o_list, p_dict = [], [], {}
for dt in data:
    d = dt.strip().split(' ')
    if len(d) != 3:
        print(d)
    else:
        s, p, o = d[0], d[1], d[2]
        if s not in s_list:
            s_list.append('"'+s+'"')
        if o not in o_list:
            o_list.append('"'+o+'"')
        if p not in p_dict:
            p_dict[p] = []
        p_dict[p].append([s, o])

uri = 'bolt://localhost:7687'
driver = GraphDatabase.driver(uri, auth=('neo4j', '980617xyx'))

with driver.session() as session:
    for s in s_list:
        str = 'merge (n:subject{name:' + s + '});'
        session.run(str)
    for o in o_list:
        str = 'merge (n:object{name:' + o + '});'
        session.run(str)

def create_relation(tx, sname, oname, pname):
    tx.run('match (s:subject{name:$sname}),(o:object{name:$oname}) '
           'create (s)-[r:predicate{name:$pname}]->(o);',
           sname=sname, oname=oname, pname=pname);

with driver.session() as session:
    for k, v in r_dict:
        for vv in v:
            s, o = vv[0], vv[1]
            session.write_transaction(create_relation, s, o, k)


'''(1) 给定一个si，给出它所有的P和O，<si, P, O>'''
with driver.session(access_mode="read") as session:
    result = session.run('''match (s:subject{name:"Richard_Stallman"})-[r:predicate]->(o:object) '''
                         '''return r, o;''')
    for record in result.records():
        print(record['r']['name'], record['o']['name'])

'''(2) 给定一个oi, 给出它所有的S和P，<S, P,oi>'''
with driver.session(access_mode="read") as session:
    result = session.run('''match (s:subject)-[r:predicate]->(o:object{name:"9th_New_York_Heavy_Artillery_Regiment"}) '''
                         '''return r, s;''')
    for record in result.records():
        print(record['r']['name'], record['s']['name'])

'''(3) 给定两个p1,p2, 给出同时拥有它们的S，<S, p1, *>, <S, p2, *>'''
with driver.session(access_mode="read") as session:
    result = session.run('''match (s:subject)-[r:predicate{name:"owns"}]->(o:object) '''
                         '''return s;''')
    s1 = [record['s']['name'] for record in result.records()]
    result = session.run('''match (s:subject)-[r:predicate{name:"isLeaderOf"}]->(o:object) '''
                         '''return s;''')
    s2 = [record['s']['name'] for record in result.records()]
    S = []
    for s in s1:
        if s in s2 and s not in S:
            S.append(s)
    print('; '.join(S))

'''(4) 给定一个oi, 给出拥有这样oi最多的S'''
with driver.session(access_mode="read") as session:
    result = session.run('''match (s:subject)-[r:predicate]->(o:object{name:"9th_New_York_Heavy_Artillery_Regiment"}) '''
                         '''return s;''')
    s = [record['s']['name'] for record in result.records()]
    s_dict = {}
    for ss in s:
        if ss not in s_dict:
            s_dict[ss] = 0
        s_dict[ss] += 1
    s_dict = [(k, v) for k, v in s_dict.items()]
    s_dict.sort(key=lambda x:-x[1])
    print(s_dict[0][0])
