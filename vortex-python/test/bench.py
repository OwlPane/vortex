# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

"""
Simple benchmarks
"""

from __future__ import annotations

from collections.abc import Callable

import pytest

import vortex as vx


@pytest.fixture(params=[10, 100])
def vortex_array(request) -> vx.Array:
    rows: list[dict[str, list[int | None]]] = []
    for _ in range(1_000):
        r: dict[str, list[int | None]] = {}
        for col in range(request.param):
            # Create large arrays of length 100 for each column.
            r[f"col{col}"] = [1, 2, None, 4] * 25
        rows.append(r)
    return vx.array(rows)


def test_compress_vortex(
    benchmark: Callable[[Callable[[], None]], None],
    vortex_array: vx.Array,
) -> None:
    def compress() -> None:
        _ = vx.compress(vortex_array)

    benchmark(compress)
