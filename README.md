# log-ingestor

Writer: the writer is sync. It could be nice to implement an async writer. This would imply not using rayon. I left it like that becuase of time constraints.

Stats limitations:

- Simple implementation because of time constraints. It can potentially show stats for more than 10 secs. 1 in 0, 2 in 15 -> will show 1 and 2 in a 15 timeframe instead of 1 in first 10,  and 2 in second 10.
- No order assured in the stats.

Buffer: is fully configurable. Swallows csv parsing errors, just traces them.

Alerts: possible optimizations there.

Tracing enabled.
