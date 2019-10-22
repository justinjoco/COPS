#!/usr/bin/env python

import os, time, sys
from os.path import isfile, join
import shutil

os.system('./build')

test_output = 'test_output'
tests = 'tests'
if len(sys.argv) == 2:
    tests = sys.argv[1]
try:
    shutil.rmtree(test_output)
except:
    pass
os.mkdir(test_output)
num_tests = 0
num_pass = 0
for f in os.listdir(tests):
    abs_f = join(tests, f)
    if isfile(abs_f):
        if f[len(f) - len('.input'):] == '.input':
            fn = f[:len(f) - len('.input')]
            print fn,
            os.system('./master.py < ' + abs_f + \
                    ' 2> ' + join(test_output, fn+'.err') +\
                    ' > ' + join(test_output, fn+'.output'))
            num_tests += 1
            with open(join(test_output, fn+'.output')) as fi:
                    out = fi.read()
            with open(join(tests, fn+'.output')) as fi:
                    std = fi.read()
            if out == std:
                print 'correct'
                num_pass += 1
            else:
                print 'wrong'
            time.sleep(2)

if num_pass == num_tests:
    print("SUCCESS: All tests pass!")
else:
    print("FAIL: At least one test failed!")
