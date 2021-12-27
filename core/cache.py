from collections import UserDict
from typing import Union


class Cache(UserDict):
    def _follow_key(self, key_path: Union[list, tuple], create: bool = False):
        data = self
        for key in key_path:
            try:
                if data is self:
                    data = super().__getitem__(key)
                else:
                    data = data[key]
            except KeyError:
                if not create:
                    raise
                new_data = {}
                if data is self:
                    super().__setitem__(key, new_data)
                else:
                    data[key] = new_data
                data = new_data
        return data

    def _normalize_key_path(self, key_path) -> Union[list, tuple]:
        if isinstance(key_path, tuple):
            if super().__contains__(key_path):
                return [key_path]
        elif not isinstance(key_path, list):
            return [key_path]
        return key_path

    def __delitem__(self, key_path):
        key_path = self._normalize_key_path(key_path)
        key = key_path[-1]
        data = self._follow_key(key_path[:-1])
        if data is self:
            super().__delitem__(key)
        else:
            del data[key]

    def __getitem__(self, key_path):
        key_path = self._normalize_key_path(key_path)
        return self._follow_key(key_path) or None

    def __setitem__(self, key_path, value):
        key_path = self._normalize_key_path(key_path)
        key = key_path[-1]
        data = self._follow_key(key_path[:-1], create=True)
        if data is self:
            super().__setitem__(key, value)
        else:
            data[key] = value

    def __contains__(self, key_path):
        key_path = self._normalize_key_path(key_path)
        try:
            self._follow_key(key_path)
            return True
        except KeyError:
            return False
