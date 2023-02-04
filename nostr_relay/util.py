import asyncio
import importlib
from contextlib import contextmanager, asynccontextmanager, suppress
from time import perf_counter

if hasattr(asyncio, "timeout"):
    timeout = asyncio.timeout
else:
    # python < 3.11 does not have asyncio.timeout
    # rather than re-implement it, we'll just do nothing
    @asynccontextmanager
    async def timeout(duration):
        yield


class catchtime:
    __slots__ = ("start", "count", "duration")

    def __enter__(self):
        self.start = perf_counter()
        self.count = 0
        return self

    def __exit__(self, type, value, traceback):
        self.duration = perf_counter() - self.start

    def __add__(self, value):
        self.count += value
        return self

    def throughput(self):
        return self.count / self.duration


def object_from_path(path):
    module_name, callable_name = path.rsplit(".", 1)
    module = importlib.import_module(module_name)
    func = getattr(module, callable_name)
    return func


def call_from_path(path, *args, **kwargs):
    """
    Call the function/constructor at the given path
    with args and kwargs
    """
    return object_from_path(path)(*args, **kwargs)


class Periodic:
    """
    A periodic async task
    """

    def __init__(self, interval):
        self.interval = interval
        self.running = False
        self._task = None

    async def start(self):
        if not self.running:
            self.running = True
            # Start task to call func periodically:
            self._task = asyncio.ensure_future(self._run())

    async def stop(self):
        if self.running:
            self.running = False
            # Stop task and await it stopped:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task

    async def wait_function(self):
        await asyncio.sleep(self.interval)

    async def _run(self):
        while self.running:
            await self.wait_function()
            await self.run_once()


@contextmanager
def easy_profiler():
    import cProfile, pstats, io
    from pstats import SortKey

    pr = cProfile.Profile()
    pr.enable()
    yield
    pr.disable()
    s = io.StringIO()
    sortby = SortKey.CUMULATIVE
    ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
    # ps.print_stats("nostr_relay")
    ps.print_stats()
    print(s.getvalue())
