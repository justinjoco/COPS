#!/usr/bin/env python

import os, time, sys, re
from os.path import isfile, join
import shutil
import math
import time 

def main(test_directory = "tests"):
    os.system('./build')

    test_output = test_directory + '_output'
    tests = test_directory

    if len(sys.argv) == 2:
        tests = sys.argv[1]
    try:
        shutil.rmtree(test_output)
    except:
        pass
        
    os.mkdir(test_output)

    num_tests = 0
    num_failed = 0
    tests_failed = []

    print("RUNNING TESTS ...")
    print("=============================")

    start = time.time()
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
                    continue
                else:          
                    print 'wrong'
                    num_failed+=1
                    tests_failed.append(fn)
                    continue
                time.sleep(2)

    end = time.time()
    elapsed = end - start
    print("=============================")
    print("{:.1f}% tests passed! {} tests failed out of {}".format((float(num_tests - num_failed)/float(num_tests)*100), num_failed, num_tests))
    print("Total time elapsed: {} minutes, {:.2f} seconds".format(int(math.floor(elapsed/60)), elapsed%60))
    if num_failed == 0:
        print("Congratulations! All tests pass!")
    else:
        print("=============================")
        print("Tests failed:")
        for test in tests_failed:
            print("  " + test)
    print("")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        main()
    else:
        test_directory = sys.argv[1]
        main(test_directory)
