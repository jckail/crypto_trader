"""
class className:

    def createName(self,name):
        self.name = name

    def displayName(self):
        return self.name

    def saying(self):
        print "hello %s" % self.name

className

first = className()
second = className()
first.createName('bucky')
second.createName('tony')

first.displayName()
first.saying()
"""

#!/usr/bin env python
# -*- coding: utf-8 -*-

#__init__
# self
# methods vs functions

class Human():
    def __init__(self,name,gender):

        self.name = name
        self.gender = gender

will = Human('William','Male')

print will.name