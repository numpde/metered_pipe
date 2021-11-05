# RA, 2021-11-05

from unittest import TestCase

import pandas

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

        n = 32

        for x in range(n):
            cw.send(x)

        for _ in range(n):
            cr.recv()

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
                    px.a.text(j, i, t, fontsize=2, ha='center', va='center')

            px.f.savefig(filename, dpi=720)
