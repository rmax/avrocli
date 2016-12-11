import contextlib

try:
    from fastavro._writer import BLOCK_WRITERS
except ImportError:
    from fastavro.writer import BLOCK_WRITERS


def get_codecs():
    """Returns registered codecs from fastavro."""
    # This assumes readers and writers are the same.
    return list(BLOCK_WRITERS.keys())


@contextlib.contextmanager
def ignoring(*exceptions):
    """Ignore given exceptions in contenxt."""
    try:
        yield
    except exceptions:
        pass
