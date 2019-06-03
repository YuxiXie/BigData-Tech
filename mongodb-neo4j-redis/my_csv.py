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
head = data[0]
data = data[1:2]
print(data)
