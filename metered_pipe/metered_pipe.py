# RA, 2021-11-05

"""
A Pipe analog that records the timestamps of writes and reads.

NB: May require a call to flush() when done.
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
RETRY_FLUSH_ON_QUEUE_FULL = 10

# Pipe will invoke the system send at most at this frequency,
# and buffer locally otherwise. A reasonable value is between
# 1e-6 (system-limited) and 1e-4 (max speed) seconds/send.
SYSTEM_SEND_MAX_FREQUENCY = 1e-4  # seconds / send


class CW:
    def __init__(self, q: queue.Queue):
        self.q = q
        self.n = 0
        self.buffer = []
        self.last_successful_put = 0

    def flush(self):
        for retry in range(RETRY_FLUSH_ON_QUEUE_FULL):
            try:
                self.q.put(
                    [
                        (obj, n, s0, time.time())
                        for (n, (obj, s0)) in enumerate(self.buffer, start=self.n)
                    ],
                    block=False
                )
            except queue.Full:
                time.sleep(INTERVAL_ON_QUEUE_FULL)
            else:
                self.last_successful_put = time.time()
                self.n += len(self.buffer)
                self.buffer = []
                return

        # Didn't flush this time because the queue is full.
        pass

    def send(self, obj):
        s0 = time.time()
        self.buffer.append((obj, s0))

        if (s0 < (self.last_successful_put + SYSTEM_SEND_MAX_FREQUENCY)):
            pass
        else:
            self.flush()

    @property
    def writable(self):
        return True

    def __del__(self):
        if self.buffer:
            raise RuntimeError("Pipe `send` buffer is not empty. Call flush() on it.")


class CR:
    def __init__(self, q):
        self.q = q
        self.buffer = []
        self._log = collections.deque(maxlen=(2 ** 32))

    def fetch(self):
        t0 = time.time()

        for (obj, n, s0, s1) in self.q.get(block=True):
            self.buffer.append(obj)
            t1 = time.time()

            assert (n == len(self._log))
            self._log.append({'s0': s0, 's1': s1, 't0': t0, 't1': t1})

        self.q.task_done()

    def recv(self):
        if not self.buffer:
            self.fetch()

        obj = self.buffer[0]
        self.buffer = self.buffer[1:]

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


def speed_test():
    """
    Number of pure .send() calls per second.
    while .recv() is running in parallel.
    """

    (cr, cw) = MeteredPipe(duplex=False)

    def recv(cr):
        while True:
            if cr.recv() is None:
                break

    # Prepare consumer process
    p = multiprocessing.Process(target=recv, args=(cr,))
    p.start()

    test_interval = 2

    t0 = time.time()
    while (time.time() < t0 + test_interval):
        cw.send(0)

    cw.send(None)
    cw.flush()

    # Consumer process
    p.join(timeout=(test_interval + 1))

    return (cw.n - 1) / test_interval


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
    rounding = np.ceil
    df = df.applymap(
        lambda x:
        0 if (x == 0) else
        np.sign(x) * (1 + max(0, rounding(np.log10(np.abs(x)) + decimals)))
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


if __name__ == '__main__':
    print(f"MeteredPipe speed test: {speed_test()} sends per second.")
