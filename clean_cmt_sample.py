from collections import defaultdict
from string import digits
from random import choice

infilenames = [
    'cmt/PPA_Trips_01012015_03312015.csv',
    'cmt/PPA_Trips_04012015_06302015.csv',
    'cmt/PPA_Trips_07012015_09302015.csv',
    'cmt/PPA_Trips_10012015_12312015.csv',
]
outfilenames = [
    'testdata/cmt1.csv',
    'testdata/cmt2.csv',
    'testdata/cmt3.csv',
    'testdata/cmt4.csv',
]

def digitstr(n):
    """Return a random string of n digits"""
    return ''.join(choice(digits) for _ in range(n))

shifts = defaultdict(lambda: digitstr(8))
trips = defaultdict(lambda: digitstr(5))
medallions = defaultdict(lambda: digitstr(4))
chauffers = defaultdict(lambda: digitstr(6))

def limit(iterable, n):
    """Iterate through at most n elements of an iterable"""
    for count, element in enumerate(iterable):
        if count >= n: break
        else: yield element

def parse_line(line):
    """Simple CSV line parser"""
    vals = []
    pos = comma = openq = closeq = 0
    while True:
        comma = line.find(',', pos)
        openq = line.find('"', pos)
        if comma < 1:
            vals.append(line[pos:])
            break
        elif openq == -1 or comma < openq:
            vals.append(line[pos:comma])
            pos = comma + 1
            continue
        else:
            closeq = line.find('"', openq + 1)
            vals.append(line[openq:closeq + 1])
            pos = closeq + 2
            continue
    return vals

for infn, outfn in zip(infilenames, outfilenames):
    with open(infn) as infile, open(outfn, 'w') as outfile:
        # Copy the first line as is
        line = next(infile)
        outfile.write(line)

        # Process the rest of the lines
        for line in limit(infile, 1500):
            cols = parse_line(line)
            if len(cols[0]) == 10:
                prefix, cols[0] = cols[0][0], cols[0][1:]
            else:
                prefix = ''

            # ID #s
            cols[0] = shifts[cols[0]]
            cols[1] = trips[cols[1]]
            cols[3] = '"' + medallions[cols[3]] + '"'
            cols[5] = chauffers[cols[5]]

            # lat/lngs
            cols[9] = cols[9][:4] + digitstr(6) + '"'
            cols[10] = cols[10][:5] + digitstr(6) + '"'
            cols[12] = cols[12][:4] + digitstr(6) + '"'
            cols[13] = cols[13][:5] + digitstr(6) + '"'

            outfile.write(prefix + ','.join(cols))