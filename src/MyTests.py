import numpy as np
class MyTests:
    def __init__(self,name,age,l):
        # take these parameters and print them
        self.name = name
        self.age = age
        self.l = l

    def printMyName(self):
        print("Hi my name is " + self.name)

    def printMyAge(self):
        print("Hi my age is " + str(self.age))

    def ptintL(self):
        print("The original list is : " + str(self.l))
        # get first and last element of list by creating a new list
        res = [ self.l[0], self.l[-2], self.l[-1] ]
        print("The first, second last and the last element of list are :" + str(res))

    def printAvg(self):
         av1 = np.mean(self.l)
         av = (sum(self.l)/len(self.l))
         av2 = np.std(self.l)
         print("Average is = " + str(round(av,2)) + " and " + str(av1) + " and standard deviation is " + str(av2))