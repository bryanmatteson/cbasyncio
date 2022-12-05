class BrokenResourceError(Exception):
    pass


class BrokenWorkerProcess(Exception):
    pass


class BusyResourceError(Exception):
    def __init__(self, action: str):
        super().__init__(f"Another task is already {action} this resource")


class ClosedResourceError(Exception):
    pass


class DelimiterNotFound(Exception):
    def __init__(self, max_bytes: int) -> None:
        super().__init__(f"The delimiter was not found among the first {max_bytes} bytes")


class EndOfStream(Exception):
    """Raised when trying to read from a stream that has been closed from the other end."""


class IncompleteRead(Exception):
    def __init__(self) -> None:
        super().__init__("The stream was closed before the read operation could be completed")


class WouldBlock(Exception):
    pass
