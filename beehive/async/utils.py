from functools import wraps


class AsyncContextManager:
    def __init__(self, gen):
        self.gen = gen

    async def __aenter__(self):
        return self.gen.__aiter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_val is not None:
            raise exc_type(exc_val).with_traceback(exc_tb)
        else:
            self.gen.aclose()


class AsyncGenerator:
    def __init__(self, f, *args, **kwargs):
        self.f = f
        self.args = args
        self.kwargs = kwargs

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.f(*self.args, **self.kwargs)


def async_generator(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        return AsyncGenerator(f, *args, **kwargs)
    return decorated_function
