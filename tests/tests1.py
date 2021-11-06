# RA, 2021-11-05
#
import multiprocessing
from unittest import TestCase

import numpy.random
import pandas
import time

from metered_pipe import MeteredPipe
from metered_pipe import visualize


class TestMeteredPipe(TestCase):
    def test_sanity(self):
        (cr, cw) = MeteredPipe(duplex=False)
        assert cr.readable and cw.writable

    def test_io_inprocess_1(self):
        (cr, cw) = MeteredPipe(duplex=False)

        cw.send(43)
        self.assertEqual(cr.recv(), 43)

    def test_io_inprocess_2(self):
        (cr, cw) = MeteredPipe(duplex=False)

        items = [43, 121]

        for i in items:
            cw.send(i)

        self.assertTrue(all((cr.recv() == i) for i in items))

    def test_log(self):
        (cr, cw) = MeteredPipe(duplex=False)

        n = 2

        for x in range(n):
            cw.send(x)

        for _ in range(n):
            cr.recv()

        log = []
        cr.flush_log_using(log.append)

        self.assertEqual(len(log), 2)

        for data in log:
            self.assertIsInstance(data, dict)
            self.assertEqual(len(data), 4)

    def test_multiprocess(self):
        n = 3

        (cr, cw) = MeteredPipe(duplex=False)

        def send(n, cw):
            for x in range(n):
                cw.send(x)

        def recv(n, cr):
            self.assertListEqual([cr.recv() for _ in range(n)], list(range(n)))

        from multiprocessing import Process
        processes = {Process(target=send, args=(n, cw)), Process(target=recv, args=(n, cr))}

        for p in processes:
            p.start()

        for p in processes:
            p.join()

    def test_visualize_logs(self):

        (cr, cw) = MeteredPipe(duplex=False)

        n = 64

        def send():
            for x in range(n):
                cw.send(x)
                time.sleep((2 * numpy.random.rand()) * 1e-3)

        def recv(ret):
            time.sleep(3e-3)

            for _ in range(n):
                cr.recv()
                time.sleep(5e-4)  # pretend to be busy

            ret.put(cr)

        ret = multiprocessing.Manager().Queue()

        from multiprocessing import Process
        processes = {Process(target=send, args=()), Process(target=recv, args=(ret,))}

        [p.start() for p in processes]
        [p.join() for p in processes]

        cr = ret.get(block=False)

        log = []
        cr.flush_log_using(log.append)

        df = pandas.DataFrame(log)
        df = (df.T - df.s0).T

        from inspect import currentframe
        from pathlib import Path

        with visualize(log, decimals=3) as px:
            filename = Path(__file__).with_suffix(f".{currentframe().f_code.co_name}.png")

            for (i, _) in enumerate(df.index):
                for (j, _) in enumerate(df.columns):
                    t = f"{df.iloc[i, j]:.2}"
                    px.a.text(i, j, t, fontsize=2, ha='center', va='center', rotation=90)

            px.f.savefig(filename, dpi=720)
