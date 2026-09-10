from dataclasses import dataclass, field


@dataclass(slots=True)
class BenchmarkMetrics:
    target_rate: int
    duration_seconds: float

    produced: int = 0
    received: int = 0
    written: int = 0

    delivery_failed: int = 0
    producer_queue_full: int = 0

    max_queue_depth: int = 0

    latency_samples_ms: list[float] = field(
        default_factory=list
    )

    wall_seconds: float = 0.0
    cpu_seconds: float = 0.0
    max_rss_mb: float = 0.0

    @staticmethod
    def percentile(
        values: list[float],
        percentile: float,
    ) -> float:
        if not values:
            return 0.0

        ordered = sorted(values)

        index = int(
            (len(ordered) - 1)
            * percentile
        )

        return ordered[index]

    @property
    def throughput(self) -> float:
        if self.wall_seconds <= 0:
            return 0.0

        return (
            self.written
            / self.wall_seconds
        )

    @property
    def producer_rate(self) -> float:
        if self.duration_seconds <= 0:
            return 0.0

        return (
            self.produced
            / self.duration_seconds
        )

    @property
    def p50_ms(self) -> float:
        return self.percentile(
            self.latency_samples_ms,
            0.50,
        )

    @property
    def p95_ms(self) -> float:
        return self.percentile(
            self.latency_samples_ms,
            0.95,
        )

    @property
    def p99_ms(self) -> float:
        return self.percentile(
            self.latency_samples_ms,
            0.99,
        )

    @property
    def cpu_percent(self) -> float:
        if self.wall_seconds <= 0:
            return 0.0

        return (
            self.cpu_seconds
            / self.wall_seconds
            * 100
        )

    def markdown_row(self) -> str:
        return (
            f"| {self.target_rate:,} "
            f"| {self.producer_rate:,.0f} "
            f"| {self.throughput:,.0f} "
            f"| {self.produced:,} "
            f"| {self.received:,} "
            f"| {self.written:,} "
            f"| {self.delivery_failed:,} "
            f"| {self.p50_ms:.3f} "
            f"| {self.p95_ms:.3f} "
            f"| {self.p99_ms:.3f} "
            f"| {self.cpu_percent:.1f}% "
            f"| {self.max_rss_mb:.1f} "
            f"| {self.max_queue_depth:,} "
            f"| {self.producer_queue_full:,} |"
        )