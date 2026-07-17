"""Type stub for babamul.consumer."""

from collections.abc import Iterator
from typing import Any

from .models import LsstAlert, ZtfAlert

class AlertConsumer:
    def __init__(
        self,
        topics: str | list[str] = ...,
        username: str | None = ...,
        password: str | None = ...,
        server: str | None = ...,
        group_id: str | None = ...,
        offset: str = ...,
        timeout: float | None = ...,
        auto_commit: bool = ...,
        as_raw: bool = ...,
    ) -> None: ...
    def __iter__(self) -> Iterator[ZtfAlert | LsstAlert | dict[str, Any]]: ...
    def __enter__(self) -> AlertConsumer: ...
    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: object) -> None: ...
    def close(self) -> None: ...
    @property
    def topics(self) -> list[str]: ...
    @property
    def group_id(self) -> str: ...
