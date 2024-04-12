import time
from typing import *
from xmlrpc.client import ServerProxy

from .pipeline import pipeline_run


class RetryWrapper:
    def __init__(self, rpc: ServerProxy):
        self.__rpc = rpc

    def __getattr__(self, name):
        func = getattr(self.__rpc, name)

        def retry_func(*args, **kwargs):
            for t in range(5):
                try:
                    return func(*args, **kwargs)
                except ConnectionResetError:
                    time.sleep(t)
            raise ConnectionResetError("Connection reset for all 5 retries")

        return retry_func

    def __call__(self, *args, **kwargs):
        return self.__rpc.__call__(*args, **kwargs)

    def __enter__(self):
        return self.__rpc.__enter__()

    def __exit__(self, *args):
        return self.__rpc.__exit__(*args)


def run_simple_worker(uri: str, handler: Callable[[Any], Any]):
    with get_rpc_object(uri) as rpc:
        for work in iterate_sched(uri):
            try:
                handler(work)
            except Exception as exc:
                rpc.error(work, repr(exc))
            else:
                rpc.done(work)


def iterate_sched(uri: str):
    with get_rpc_object(uri) as rpc:
        while True:
            work = rpc.get()
            if work is None:
                break
            yield work


def get_rpc_object(uri: str):
    return RetryWrapper(ServerProxy(uri, allow_none=True))


def run_pipelined_worker(uri: str, pipeline_specs: Sequence[Tuple[Callable[[Any], Any], int]]):

    def _input_iter():
        for work in iterate_sched(uri):
            yield (work, work, None)

    def _wrap(fn):

        def _fn(x):
            work, arg, error = x
            if error is not None:
                return work, arg, error
            try:
                return work, fn(arg), None
            except Exception as exc:
                return work, fn, exc

        return _fn

    def _report(x):
        work, arg, error = x
        with get_rpc_object(uri) as rpc:
            if error is None:
                rpc.done(work)
            else:
                rpc.error(work, repr(error) + " in " + repr(arg))

    pipeline_run(_input_iter(), [(_wrap(f), n) for f, n in pipeline_specs] + [(_report, 2)])
