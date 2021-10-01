#!/usr/bin/env python3
#----------------------------------------------------------------------------------------------------------------------#
#                                                                                                                      #
#                                       Tuplex: Blazing Fast Python Data Science                                       #
#                                                                                                                      #
#                                                                                                                      #
#  (c) 2017 - 2021, Tuplex team                                                                                        #
#  Created by Leonhard Spiegelberg first on 1/1/2021                                                                   #
#  License: Apache 2.0                                                                                                 #
#----------------------------------------------------------------------------------------------------------------------#

from unittest import TestCase
import tuplex
import time

class TestExceptions(TestCase):

    def setUp(self):
        self.conf = {"webui.enable" : False, "driverMemory" : "8MB", "partitionSize" : "256KB"}
        self.c = tuplex.Context(self.conf)

    def test_counts(self):

        def f(x):
            if x % 2 == 0:
                raise FileNotFoundError
            if x % 3 == 0:
                raise LookupError
            if x % 5 == 0:
                raise IndexError
            return x * x

        ds = self.c.parallelize([1, 2, 3, 4, 5]).map(f)

        self.assertEqual(ds.collect(), [1])

        d = ds.exceptions

        self.assertEqual(d['FileNotFoundError'], [2, 4])
        self.assertEqual(d['LookupError'], [3])
        self.assertEqual(d['IndexError'], [5])