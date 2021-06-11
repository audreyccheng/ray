import functools

# val = ""
# def my_decorator_method(arg1):
def my_decorator(fn):
    print("my decorator initialized")

    # @functools.wraps(fn)
    def wrapped_fn(self, x):
        print("xxx", fn)
        wrapped_fn.count = wrapped_fn.count + 1
        wrapped_fn.list.append(x)
        print(len(wrapped_fn.list))
        print(wrapped_fn.count)
        result = fn(self, x)
        # print(arg1)
        print("done")
        # wrapped_fn.val += x
        # val += x
        return result
    # wrapped_fn.val = ""
    # print(wrapped_fn.val)
    wrapped_fn.count = 0
    wrapped_fn.list = []
    # print(val)
    return wrapped_fn
    # return my_decorator

class Actor:
    def __init__(self):
        pass
    # @my_decorator_method("hello")
    @my_decorator
    def decorated_method(self, x):
        return x
    def undecorated_method(self, x):
        return x

a = Actor()
print(a.undecorated_method("first"))
print(a.decorated_method("second"))
print(a.decorated_method("second"))