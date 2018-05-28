import sys

opsagg = {'sum': 0, 'avg': 0, 'count': 0, 'stddev_samp': 0, 'max': 0, 'min': 0, 'distinct':0, 'rank':0}
opsoth = {'order by': 0, 'group by': 0, ' =': 0, ' >= ': 0, ' <= ': 0, ' < ': 0, ' > ': 0, ' exists ': 0, ' in ': 0,
          ' union all ': 0, 'having': 0, 'round': 0, ' limit ': 0, 'distinct': 0, 'substr':0}


def getStats(filename):
    for line in open(filename, 'r'):
        if line.startswith('--'):
            continue
        if line.__contains__('sum'):
            opsagg['sum'] += 1
        if line.__contains__('avg'):
            opsagg['avg'] += 1
        if line.__contains__('count'):
            opsagg['count'] += 1
        if line.__contains__('stddev_samp'):
            opsagg['stddev_samp'] += 1
        if line.__contains__('max'):
            opsagg['max'] += 1
        if line.__contains__('min'):
            opsagg['min'] += 1
        if line.__contains__('distinct'):
            opsagg['distinct'] += 1
        if line.__contains__('rank'):
            opsagg['rank'] += 1


        if line.__contains__('order by'):
            opsoth['order by'] += 1
        if line.__contains__('group by'):
            opsoth['group by'] += 1
        if line.__contains__(' <= '):
            opsoth[' <= '] += 1
        if line.__contains__(' >= '):
            opsoth[' >= '] += 1
        if line.__contains__(' < '):
            opsoth[' < '] += 1
        if line.__contains__(' > '):
            opsoth[' > '] += 1
        if line.__contains__(' ='):
            opsoth[' ='] += 1
        if line.__contains__(' exists '):
            opsoth[' exists '] += 1
        if line.__contains__(' in '):
            opsoth[' in '] += 1
        if line.__contains__(' union all '):
            opsoth[' union all '] += 1
        if line.__contains__(' having '):
            opsoth['having'] += 1
        if line.__contains__(' round '):
            opsoth['round'] += 1
        if line.__contains__(' limit '):
            opsoth[' limit '] += 1
        if line.__contains__('substr'):
            opsoth['substr'] += 1

    with open(filename.split('.')[0] + 'Stats.out', 'w+') as file:
        print("Os tais:")
        print(opsagg.__str__()) 
        print(str(sum(opsagg.values())) + " em " + str(sum(opsagg.values())+sum(opsoth.values())))
        file.write(opsagg.__str__())
        file.write('\n')
        file.write(opsoth.__str__())


if __name__ == '__main__':
    for file in sys.argv[1].split(" "):
        print(file)
        getStats(file)

