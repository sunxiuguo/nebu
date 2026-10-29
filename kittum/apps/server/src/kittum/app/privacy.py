"""Keep the configured source contact out of diagnostics and lookup audit data.

Requests and successful source content remain unchanged. Lookup requests are rebuilt from
settings on a retry; an audit placeholder is never expanded into an executable URL.
"""

import copy
import functools
import json
import logging
import re
import traceback
from contextlib import contextmanager, suppress
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

from kittum.core import sha256_hex
from kittum.gateway import CONTACT_PLACEHOLDER, HttpRequest, HttpResponse, SourceGateway
from kittum.runs import PermanentJobError

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

    from kittum.gateway import Transport

_ACTIVE: ContextVar[ContactPrivacy | None] = ContextVar("source_contact_privacy", default=None)
_HTTP_LOGGERS = (
    "httpx2",
    "httpcore2.connection",
    "httpcore2.http11",
    "httpcore2.http2",
    "httpcore2.proxy",
    "httpcore2.socks",
    "stamina",
)


class _ContactFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool | logging.LogRecord:
        privacy = _ACTIVE.get()
        if privacy is None:
            return True
        # A replacement record leaves other threads' and callers' objects alone. Rendering the
        # arguments first also handles request objects without mutating what goes on the wire.
        safe = copy.copy(record)
        safe.msg, safe.args = privacy.text(record.getMessage()), ()
        if record.exc_info:
            safe.exc_text = privacy.text("".join(traceback.format_exception(*record.exc_info)))
            safe.exc_info = None
        elif record.exc_text:
            safe.exc_text = privacy.text(record.exc_text)
        if record.stack_info:
            safe.stack_info = privacy.text(record.stack_info)
        for name, value in record.__dict__.items():
            if name.startswith("stamina."):
                safe.__dict__[name] = privacy.value(value)
        return safe


_FILTER = _ContactFilter()


class _ContactFailure(RuntimeError):
    """A diagnostic whose original exception renders a read-only private value."""


class ContactPrivacy:
    def __init__(self, contact: str | None) -> None:
        # Recognize plain, percent-encoded and nested-percent-encoded spellings, including
        # encoded letters and case variants. Only the configured value is matched.
        atoms = []
        for char in contact or "":
            spellings = {re.escape(char)}
            for variant in {char.lower(), char.upper()}:
                spellings.add("".join(f"%(?:25)*{byte:02x}" for byte in variant.encode()))
            atoms.append("(?:" + "|".join(sorted(spellings)) + ")")
        self._pattern = re.compile("".join(atoms), re.IGNORECASE) if atoms else None

    def text(self, text: str) -> str:
        return self._pattern.sub(CONTACT_PLACEHOLDER, text) if self._pattern else text

    def value(self, value: Any) -> Any:
        if isinstance(value, str):
            return self.text(value)
        if isinstance(value, bytes):
            return self.body(value)
        if isinstance(value, tuple):
            return tuple(self.value(v) for v in value)
        if isinstance(value, list):
            return [self.value(v) for v in value]
        if isinstance(value, dict):
            return {self.value(k): self.value(v) for k, v in value.items()}
        if value is None or isinstance(value, int | float | bool):
            return value
        return self.text(repr(value))

    def body(self, body: bytes) -> bytes:
        # Called for diagnostic response bodies, before source_message truncates an address.
        if self._pattern is None:
            return body
        pattern = re.compile(self._pattern.pattern.encode(), re.IGNORECASE)
        body = pattern.sub(CONTACT_PLACEHOLDER.encode(), body)
        try:
            data = json.loads(body)
        except ValueError:
            return body
        cleaned = self.value(data)
        # JSON may spell an address with Unicode escapes. Clean decoded strings before the
        # source-message reader can truncate them into an identifying partial address.
        return json.dumps(cleaned, ensure_ascii=False).encode() if cleaned != data else body

    def exception(self, error: BaseException, seen: set[int] | None = None) -> None:
        seen = seen if seen is not None else set()
        if id(error) in seen:
            return
        seen.add(id(error))
        if self.text(str(error)) != str(error):
            error.args = tuple(self.value(v) for v in error.args)
        if notes := getattr(error, "__notes__", None):
            error.__notes__ = [self.text(n) for n in notes]
        for name in ("reason", "message", "url", "strerror", "filename", "filename2"):
            value = getattr(error, name, None)
            if isinstance(value, str) and self.text(value) != value:
                # Read-only properties generally derive from the cleaned args.
                with suppress(AttributeError):
                    setattr(error, name, self.text(value))
        if isinstance(error, BaseExceptionGroup):
            for child in error.exceptions:
                self.exception(child, seen)
        for cause in (error.__cause__, error.__context__):
            if cause is not None:
                self.exception(cause, seen)

    @contextmanager
    def logs(self) -> Generator[None]:
        if self._pattern is None:
            yield
            return
        for name in _HTTP_LOGGERS:
            logging.getLogger(name).addFilter(_FILTER)
        token = _ACTIVE.set(self)
        try:
            yield
        finally:
            _ACTIVE.reset(token)

    def guard[**P, R](self, operation: Callable[P, R]) -> Callable[P, R]:
        if self._pattern is None:
            return operation

        @functools.wraps(operation)
        def run(*args: P.args, **kwargs: P.kwargs) -> R:
            with self.logs():
                try:
                    return operation(*args, **kwargs)
                except Exception as error:
                    # Preserve exception types and therefore PermanentJobError's retry policy.
                    self.exception(error)
                    detail = "".join(traceback.format_exception(error))
                    if self.text(detail) != detail:
                        # Some exceptions render read-only properties rather than args. Do not
                        # leave that exception (or a private cause) in a worker's traceback.
                        kind = (
                            PermanentJobError
                            if isinstance(error, PermanentJobError)
                            else _ContactFailure
                        )
                        raise kind(f"{type(error).__name__}: {self.text(str(error))}") from None
                    raise

        return run


class ContactTransport:
    def __init__(self, inner: Transport, privacy: ContactPrivacy) -> None:
        self._inner, self._privacy = inner, privacy

    def send(self, request: HttpRequest) -> HttpResponse:
        response = self._privacy.guard(self._inner.send)(request)
        body = (
            self._privacy.body(response.body) if not 200 <= response.status < 300 else response.body
        )
        headers = tuple((k, self._privacy.text(v)) for k, v in response.headers)
        if body == response.body and headers == response.headers:
            return response
        return response.model_copy(
            update={
                "body": body,
                "body_hash": sha256_hex(body) if body != response.body else response.body_hash,
                "headers": headers,
            }
        )


class ContactGateway(SourceGateway):
    def __init__(self, *, privacy: ContactPrivacy, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._privacy = privacy

    def fetch(self, source: str, request: HttpRequest, *, use_cache: bool = True) -> HttpResponse:
        # Retries and their structured log fields occur outside Transport.send.
        return self._privacy.guard(super().fetch)(source, request, use_cache=use_cache)
