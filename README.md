# Metered Pipe

This interprocess pipe records the timestamps
when

s0) `send` is called,

s1) `send` succeeds,

t0) `recv` is called, and

t1) `recv` succeeds.

Thus, it can be used to diagnose communication bottlenecks.

The included visualization routine
produces the following summary plot
where 
for each event the delay from s0 is shown
as a heatmap
and
the graph is a running estimate of 
events / ms.

![summary](tests/tests1.test_visualize_logs.png)
