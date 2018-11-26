import datetime


class Singleton(type):
    """
    Metaclass for Singleton pattern
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)

        return cls._instances[cls]


def time_in_seconds(dt):
    current_time = datetime.datetime.utcfromtimestamp(0)
    return int((dt - current_time).total_seconds())


def generate_ranges(timestamp, interval=None):
    interval = datetime.timedelta(days=365) if not interval else interval
    target_seconds = time_in_seconds(timestamp)
    interval_seconds = interval.total_seconds()

    current_bin = int(target_seconds / interval_seconds)

    start_dt = datetime.datetime.fromtimestamp(current_bin * interval_seconds)
    finish_dt = datetime.datetime.fromtimestamp((current_bin + 1) * interval_seconds)

    return dict(start=start_dt, finish=finish_dt)


def from_timestamp(ts):
    ts = int(ts) if type(ts) is str else ts

    return datetime.datetime.fromtimestamp(ts)
