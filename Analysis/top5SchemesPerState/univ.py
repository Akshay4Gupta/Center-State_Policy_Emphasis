import os

def make_dir(s):
    x = ''
    for i in s.split('/'):
        x += i + '/'
        if not os.path.exists(x):
            os.mkdir(x)
