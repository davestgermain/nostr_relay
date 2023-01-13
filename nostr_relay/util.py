import importlib


def call_from_path(path, *args, **kwargs):
    """
    Call the function/constructor at the given path
    with args and kwargs
    """
    module_name, callable_name = path.rsplit('.', 1)
    module = importlib.import_module(module_name)
    func = getattr(module, callable_name)
    return func(*args, **kwargs)

