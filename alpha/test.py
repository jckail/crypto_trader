
class Test:
    def __init__(self, x):
        self.x = x
        print x
        pass


    def f(self):
        x = self.x
        print x*x
        return x*x

    def main(self):
        a = Test(self.x)
        a.f()

if __name__ == '__main__':
    a = Test()
    a.main()
