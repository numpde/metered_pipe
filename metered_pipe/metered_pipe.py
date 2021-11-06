# RA, 2021-11-05

"""
A Pipe analog that records the timestamps of writes and reads.
"""

import collections
import contextlib
import typing

import queue
import time
import multiprocessing

import matplotlib.pyplot
import pandas
import numpy

import inclusive
import plox

INTERVAL_ON_QUEUE_FULL = 1e-5  # seconds


class CW:
    def __init__(self, q):
        self.q = q
        self.n = 0

    def send(self, obj):
        s0 = time.time()
        while True:
            try:
                s1 = time.time()
                self.q.put((obj, self.n, s0, s1), block=False)
            except queue.Full:
                time.sleep(INTERVAL_ON_QUEUE_FULL)
            else:
                self.n += 1  # objects sent
                break

    @property
    def writable(self):
        return True


class CR:
    def __init__(self, q):
        self.q = q
        self._log = collections.deque(maxlen=(2 ** 32))

    def recv(self):
        t0 = time.time()
        (obj, n, s0, s1) = self.q.get(block=True)
        t1 = time.time()

        assert (n == len(self._log))
        self._log.append({'s0': s0, 's1': s1, 't0': t0, 't1': t1})

        return obj

    def flush_log_using(self, f: typing.Callable):
        while self._log:
            f(self._log.popleft())

    @property
    def readable(self):
        return True


def MeteredPipe(duplex=True, q=None):
    if duplex:
        raise NotImplementedError

    PIPE_BUFFER_SIZE = 2 ** 12

    if not q:
        # `maxsize` is the number of objects
        q = multiprocessing.Manager().Queue(maxsize=PIPE_BUFFER_SIZE)

        # Something like this would also work and may be a little faster
        # OBJ_SIZE = 1024
        # q = faster_fifo.Queue(PIPE_BUFFER_SIZE * OBJ_SIZE)

    return (CR(q), CW(q))


@contextlib.contextmanager
def visualize(cr_logs: list, decimals=3) -> plox.Plox:
    np = numpy

    df = pandas.DataFrame(data=cr_logs)
    assert list(df.columns) == ['s0', 's1', 't0', 't1']

    tt = df.s0

    # offset each record by its `sent` timestamp
    df = (df.T - df.s0).T

    # print(df.to_markdown())

    # round to `decimals` and offset by the same
    df = df.applymap(
        lambda x:
        0 if (x == 0) else
        np.sign(x) * (1 + int(max(0, np.ceil(np.log10(np.abs(x)) + decimals))))
    )

    from plox import rcParam

    style = {
        rcParam.Figure.figsize: (16, 1),
        rcParam.Axes.linewidth: 0.01,
        rcParam.Font.size: 6,
        rcParam.Figure.dpi: 720,
    }

    with plox.Plox(style) as px:
        v = int(np.ceil(df.abs().max().max()))
        im = px.a.imshow(df.T, aspect='auto', interpolation='none', vmin=(-v * 1.1), vmax=(v * 1.1), cmap='coolwarm')

        px.a.set_yticks(np.arange(len(df.columns)))
        px.a.set_yticklabels(df.columns)

        px.a.set_xlabel("Event")

        px.a.invert_yaxis()

        cb = px.f.colorbar(im, aspect=3)
        cb.set_ticks(inclusive.range[-v, v])
        # note the reverse offset by `decimals`
        labels = [
            fr"$10^{{{np.abs(x) - decimals - 1}}}$s"
            if x else "0"
            for x in cb.get_ticks()
        ]
        cb.ax.set_yticklabels(labels=labels, fontsize=5)

        cb.ax.text(0.5, +v, "behind", ha='right', va='top', rotation=-90)
        cb.ax.text(0.5, -v, "ahead", ha='right', va='bottom', rotation=-90)

        # How many events within the last `k` seconds
        nn = np.zeros_like(tt)
        k = 3e-3  # timescale (seconds)
        nn[0] = 1

        for ((m, ta), (n, tb)) in zip(enumerate(tt), enumerate(tt[1:], start=1)):
            nn[n] = 1 + (nn[m] * np.exp(-(tb - ta) / k))

        nn *= (1e-3 / k)

        ax: matplotlib.pyplot.Axes = px.a.twinx()
        ax.plot(tt.index, nn, c='k', lw=0.2)
        ax.set_ylim(-1, max(nn) + 2)
        ax.set_ylabel("Events / ms")

        # Example usage:
        # px.f.savefig(Path(__file__).with_suffix('.png'), dpi=720)

        yield px
