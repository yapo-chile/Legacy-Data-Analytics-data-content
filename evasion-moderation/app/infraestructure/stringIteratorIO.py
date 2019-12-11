from typing import Iterator, Dict, Any, Optional
import io


class StringIteratorIO(io.TextIOBase):

    def __init__(self, iter: Iterator[str]):
        self._iter = iter
        self._buff = ''

    def _getLargeStr(self, n: Optional[int] = None) -> str:
        while not self._buff:
            try:
                self._buff = next(self._iter)
            except StopIteration:
                break
        ret = self._buff[:n]
        self._buff = self._buff[len(ret):]
        return ret

    def read(self, n: Optional[int] = None) -> str:
        line = []
        if n is None or n < 0:
            while True:
                m = self._getLargeStr()
                if not m:
                    break
                line.append(m)
        else:
            while n > 0:
                m = self._getLargeStr(n)
                if not m:
                    break
                n -= len(m)
                line.append(m)
        return ''.join(line)


def cleanCsvValue(value: Optional[Any]) -> str:
    if value is None:
        return r'\N'
    return str(value).replace('\n', '\\n')


def cleanStrValue(value: str) -> str:
    return value.replace('\\', '')
