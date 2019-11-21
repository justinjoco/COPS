#!/usr/bin/env python
"""
Test program for CS5414 Paxos project with appropriate master.py.
Provided by the CS5414 course staff; test output format modifications by me (Justin)
"""
import os, time, sys
from os.path import isfile, join
import shutil
import time 
import math


def main(test_directory = "tests"):

    os.system('./stopall >/dev/null 2>/dev/null')
    os.system('./build >/dev/null 2>/dev/null')

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

                os.system('./stopall >/dev/null 2>/dev/null')
                num_tests += 1
                with open(join(test_output, fn+'.output')) as fi:
                        out = fi.read().strip().split('\n')
                with open(join(tests, fn+'.output')) as fi:
                        std = fi.read().strip().split('\n')
                result = True
                out_index = 0
                for s in std:
                    json = eval(s)
                    prev = None
                    for i in range(json['count']):
                        # check the number of lines in output
                        if out_index >= len(out):
                            result = False
                            break
                        # check if all the outputs are identical
                        if prev is not None and prev != out[out_index]:
                            result = False
                        prev = out[out_index]
                        # check if the output satisfies the requirement
                        out_data = set(out[out_index].split(','))
                        std_mandatory = set(json['mandatory'].split(','))
                        std_all = set(json['optional'].split(',')).union(std_mandatory)
                        if not out_data.issubset(std_all):
                            result = False
                        if not out_data.issuperset(std_mandatory):
                            result = False

                        out_index += 1

                if result:
                    print 'correct'
                else:
                    print 'wrong'
                    num_failed+=1
                    tests_failed.append(fn)
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

