import importlib
from contextlib import contextmanager
from time import perf_counter


@contextmanager
def catchtime() -> float:
    start = perf_counter()
    yield lambda: (perf_counter() - start) * 1000


def call_from_path(path, *args, **kwargs):
    """
    Call the function/constructor at the given path
    with args and kwargs
    """
    module_name, callable_name = path.rsplit('.', 1)
    module = importlib.import_module(module_name)
    func = getattr(module, callable_name)
    return func(*args, **kwargs)

