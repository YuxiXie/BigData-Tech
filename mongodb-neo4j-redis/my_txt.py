with open('yagoThreeSimplified.txt', 'r', encoding='utf-8') as f:
    data = f.read().strip().split('\n')

s_dict = {}
o_dict = {}
r_dict = {}

for d in data:
    d = d.strip().split(' ')
    s, r, o = d[0], d[1], d[2]

    if s not in s_dict:
        s_dict[s] = {}
    if r not in s_dict[s]:
        s_dict[s][r] = []
    s_dict[s][r].append(o)

    if o not in o_dict:
        o_dict[o] = {}
    if r not in o_dict[o]:
        o_dict[o][r] = []
    o_dict[o][r].append(s)

    if r not in r_dict:
        r_dict[r] = []
    r_dict[r].append([s, o])
