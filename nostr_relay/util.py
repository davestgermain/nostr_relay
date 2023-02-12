import asyncio
import importlib
import statistics
import collections
import logging

from contextlib import contextmanager, asynccontextmanager, suppress
from time import perf_counter

try:
    import rapidjson as json
except ImportError:
    import json


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

    _pending_tasks = []
    _running_tasks = []

    @staticmethod
    def register(periodic_task):
        Periodic._pending_tasks.append(periodic_task.start())

    @staticmethod
    async def start_pending():
        while Periodic._pending_tasks:
            task = Periodic._pending_tasks.pop()
            await task

    @classmethod
    def cancel_running(cls):
        for task in cls._running_tasks:
            task.cancel()
        cls._running_tasks.clear()

    def __init__(self, interval, run_at_start=False, swallow_exceptions=False):
        self.interval = interval
        self.running = False
        self._task = None
        self._run_at_start = run_at_start
        self._swallow_exceptions = swallow_exceptions

    async def start(self):
        if not self.running:
            self.running = True
            # Start task to call func periodically:
            self._task = asyncio.ensure_future(self._run())
            self._running_tasks.append(self._task)

    async def stop(self):
        if self.running:
            self.running = False
            # Stop task and await it stopped:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._running_tasks.remove(self._task)

    async def wait_function(self):
        await asyncio.sleep(self.interval)

    async def _run(self):
        if self._run_at_start:
            try:
                await self.run_once()
            except Exception:
                if not self._swallow_exceptions:
                    raise
                elif hasattr(self, "log"):
                    self.log.exception("run_once")

        while self.running:
            await self.wait_function()
            try:
                await self.run_once()
            except Exception:
                if not self._swallow_exceptions:
                    raise
                elif hasattr(self, "log"):
                    self.log.exception("run_once")


class StatsCollector(Periodic):
    def __init__(self, interval):
        super().__init__(interval, swallow_exceptions=True)
        self.stats = collections.defaultdict(lambda: collections.deque(maxlen=1000))
        self.counts = collections.defaultdict(int)
        self.log = logging.getLogger("nostr_relay.stats")
        self.iteration = 0

    @contextmanager
    def timeit(self, statname):
        start = perf_counter()
        counter = {"count": 0}
        yield counter
        duration = perf_counter() - start
        counter["duration"] = duration
        if counter["count"]:
            self.add(statname, duration)

    def add(self, stat, timing):
        self.stats[stat].append(timing)
        self.counts[stat] += 1

    async def run_once(self):
        # self.iteration += 1

        for stat, values in self.stats.items():
            if len(values) > 2:
                median = statistics.median(values) * 1000
                # avg = statistics.fmean(values) * 1000
                p90 = statistics.quantiles(values, n=10)[-1] * 1000
                count = self.counts[stat]
                self.log.info(
                    "Stats for %(stat)-8s median: %(median)6.2fms  p90: %(p90)6.2fms  count:%(count)8d",
                    locals(),
                )


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
