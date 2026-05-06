class BraidError(Exception):
    pass


class ConfigError(BraidError):
    pass


class PayloadTooLargeError(BraidError):
    def __init__(self, size_bytes: int, limit_bytes: int, what: str = "function"):
        if what == "function":
            message = (
                f"Serialized function is {size_bytes / 1024:.1f}KB, limit is {limit_bytes / 1024:.1f}KB. "
                "Reduce closure size or move large objects to S3."
            )
        else:
            message = (
                f"Serialized {what} is {size_bytes / 1024:.1f}KB, limit is {limit_bytes / 1024:.1f}KB. "
                "Try reducing batch_size or passing S3 keys for large inputs."
            )
        super().__init__(message)
        self.size_bytes = size_bytes
        self.limit_bytes = limit_bytes
        self.what = what


class LayerBuildError(BraidError):
    pass


class JobTimeoutError(BraidError):
    def __init__(self, job_id: str, timeout_s: int, received: int, expected: int):
        super().__init__(
            f"Job {job_id} timed out after {timeout_s}s: received {received}/{expected} results. "
            "Check CloudWatch logs for worker errors, or increase timeout_s."
        )
        self.job_id = job_id
        self.timeout_s = timeout_s
        self.received = received
        self.expected = expected


class WorkerError(BraidError):
    """Raised when one or more remote workers fail."""

    def __init__(self, failures: list[dict]):
        # failures: [{"idx": int, "error": str, "traceback": str}]
        n = len(failures)
        first = failures[0]
        detail = f"idx={first['idx']}: {first['error']}"
        tail = f" (and {n - 1} more)" if n > 1 else ""
        super().__init__(
            f"{n} worker(s) failed. First failure — {detail}{tail}\n"
            f"Remote traceback:\n{first['traceback']}"
        )
        self.failures = failures

    @property
    def idx(self) -> int:
        return self.failures[0]["idx"]

    @property
    def original(self) -> str:
        return self.failures[0]["error"]

    @property
    def remote_traceback(self) -> str:
        return self.failures[0]["traceback"]


class ThrottleError(BraidError):
    pass


class JobFailedError(BraidError):
    """A detached job stopped without producing every result."""

    def __init__(self, job_id: str, batch_state: str, received: int, expected: int):
        super().__init__(
            f"Job {job_id} ended in Batch state {batch_state} with {received}/{expected} "
            "results. Workers were likely killed (spot reclaim, OOM, or container crash), "
            "or the job aged out of Batch's ~24h describe_jobs retention while incomplete. "
            "Check CloudWatch logs, then resubmit the missing inputs."
        )
        self.job_id = job_id
        self.batch_state = batch_state
        self.received = received
        self.expected = expected
