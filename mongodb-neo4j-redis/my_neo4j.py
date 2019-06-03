from neo4j import GraphDatabase
from neo4j import Node


with open('student.csv', 'r', encoding='utf-8') as f:
    data = f.read().strip().split('\n')
for index, line in enumerate(data):
    line = line.strip().split(';')
    line = [w.strip() for w in line]
    if index > 0:
        d = ''
        for i, w in enumerate(line):
            if i > 0:
                d += ', '
            d += data[0][i] + ': ' + w
        line = d
    data[index] = line
data = data[1:]


uri = 'bolt://localhost:7687'
driver = GraphDatabase.driver(uri, auth=('neo4j', '980617xyx'))

with driver.session() as session:
    for d in data:
        str = 'merge (n:student{' + d + '});'
        session.run(str)

def create_friendship(tx, sid, tid):
    tx.run('match (s{id:$sid}),(t{id:$tid}) '
           'create (s)-[r:friend]->(t);',
           sid=sid, tid=tid);

with driver.session() as session:
    session.write_transaction(create_friendship, 'mat64', 'por384')

with driver.session(access_mode="read") as session:
    result = session.run('''match (s:student) where s.romantic = "yes" return s.id;''');
    rt = [record['s.id'] for record in result.records()]
    print('; '.join(rt))

with driver.session(access_mode="read") as session:
    result = session.run('match (s)-[r]->(t) '
                         'return s.id, TYPE(r), t.id;');
    for record in result.records():
        print(record['s.id'], record['TYPE(r)'], record['t.id'])

with driver.session(access_mode="read") as session:
    session.run('match (s:student{id: "mat64"})-[r]->(t)'
                'delete r;')
