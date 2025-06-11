"""Pytest-dash errors."""


class PytestDashError(Exception):
    """Base error for pytest-dash."""


class InvalidDriverError(PytestDashError):
    """An invalid selenium driver was specified."""


class NoAppFoundError(PytestDashError):
    """No `app` was found in the file."""


class DashAppLoadingError(PytestDashError):
    """The dash app failed to load"""


class MissingBehaviorError(PytestDashError):
    """A behavior was missing from the """


class ServerCloseError(PytestDashError):
    """Pytest-dash had trouble closing a server."""
