from __future__ import annotations

import asyncio
import enum
from dataclasses import dataclass
from typing import Any, Dict, Generic, Literal, Set, Union, overload
from weakref import WeakKeyDictionary

from ._types import R, T

_run_vars: WeakKeyDictionary[Any, Dict[str, Any]] = WeakKeyDictionary()


@dataclass(frozen=True)
class _TokenWrapper:
    __slots__ = "_token", "__weakref__"
    _token: object


class _NoValueSet(enum.Enum):
    NO_VALUE_SET = enum.auto()


class RunvarToken(Generic[T]):
    __slots__ = "_var", "_value", "_redeemed"

    def __init__(self, var: RunVar[T], value: Union[T, Literal[_NoValueSet.NO_VALUE_SET]]):
        self._var = var
        self._value: Union[T, Literal[_NoValueSet.NO_VALUE_SET]] = value
        self._redeemed = False


class RunVar(Generic[T]):
    __slots__ = "_name", "_default"

    NO_VALUE_SET: Literal[_NoValueSet.NO_VALUE_SET] = _NoValueSet.NO_VALUE_SET

    _token_wrappers: Set[_TokenWrapper] = set()

    def __init__(
        self, name: str, default: Union[T, Literal[_NoValueSet.NO_VALUE_SET]] = NO_VALUE_SET,
    ):
        self._name = name
        self._default = default

    @property
    def _current_vars(self) -> Dict[str, T]:
        token = asyncio.get_running_loop()
        while True:
            try:
                return _run_vars[token]
            except TypeError:
                token = _TokenWrapper(token)
                self._token_wrappers.add(token)
            except KeyError:
                run_vars = _run_vars[token] = {}
                return run_vars

    @overload
    def get(self, default: R) -> Union[T, R]:
        ...

    @overload
    def get(self) -> T:
        ...

    def get(self, default: Union[R, Literal[_NoValueSet.NO_VALUE_SET]] = NO_VALUE_SET) -> Union[T, R]:
        try:
            return self._current_vars[self._name]
        except KeyError:
            if default is not RunVar.NO_VALUE_SET:
                return default
            elif self._default is not RunVar.NO_VALUE_SET:
                return self._default

        raise LookupError(f'Run variable "{self._name}" has no value and no default set')

    def set(self: RunVar[Any], value: T) -> RunvarToken[T]:
        current_vars = self._current_vars
        token = RunvarToken(self, current_vars.get(self._name, RunVar.NO_VALUE_SET))
        current_vars[self._name] = value
        return token

    def reset(self, token: RunvarToken[T]) -> None:
        if token._var is not self:
            raise ValueError("This token does not belong to this RunVar")

        if token._redeemed:
            raise ValueError("This token has already been used")

        if token._value is _NoValueSet.NO_VALUE_SET:
            try:
                del self._current_vars[self._name]
            except KeyError:
                pass
        else:
            self._current_vars[self._name] = token._value

        token._redeemed = True

    def __repr__(self) -> str:
        return f"<RunVar name={self._name!r}>"
