#__author__ = 'jkail'

import requests
import pandas as p
import datetime as dt


class MyInteger(object):
    def __init__(self,newvalue):
        self.value = newvalue

    def add(self,another):
        return MyInteger(self.value + another.value)


if __name__ == '__main__':

    runner = MyInteger(1)


