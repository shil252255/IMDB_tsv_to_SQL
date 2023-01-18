from time import monotonic as tn

ts = tn()


def print_time(s=''):
    global ts
    print(f'{"-"*100}\n{tn() - ts:.2f} - {s}')
    ts = tn()


DTYPE = {'tconst': 'string',
         'titleType': 'category',
         'primaryTitle': 'string',
         'originalTitle': 'string',
         'isAdult': 'boolean',
         'startYear': 'Int32',
         'endYear': 'Int64',
         'runtimeMinutes': 'Int64',
         }