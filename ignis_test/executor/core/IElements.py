import random


class IElements:

    def create(self, n, seed):
        pass


class IElementsInt(IElements):

    def create(self, n, seed):
        random.seed(seed)
        return [random.randint(0, n) for i in range(0, n)]


class IElementsBytes(IElements):

    def create(self, n, seed):
        random.seed(seed)
        return [random.randint(0, 255) for i in range(0, n)]


class IElementsStr(IElements):

    def create(self, n, seed):
        random.seed(seed)
        return [str(random.randint(0, n)) for i in range(0, n)]


class IElementsPair(IElements):

    def __init__(self, *args):
        if len(args) == 1:
            args = args[0]
        self.e0, self.e1 = args[0](), args[1]()

    def create(self, n, seed):
        return list(zip(self.e0.create(n, seed), self.e1.create(n, seed)))
