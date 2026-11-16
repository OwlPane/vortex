# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors


import numpy as np
import pyarrow as pa

import vortex


def test_primitive_compress() -> None:
    a = pa.array([0, 0, 0, 0, 9, 9, 9, 9, 1, 5])
    arr_compressed = vortex.compress(vortex.array(a))
    assert not isinstance(arr_compressed, vortex.PrimitiveArray)
    assert arr_compressed.nbytes < a.nbytes
    assert arr_compressed.to_arrow_array() == a


def test_for_compress() -> None:
    a = pa.array(np.arange(10_000) + 10_000_000)
    arr_compressed = vortex.compress(vortex.array(a))
    assert not isinstance(arr_compressed, vortex.PrimitiveArray)
    assert arr_compressed.to_arrow_array() == a


def test_arrange_encode() -> None:
    expected = pa.array(np.arange(10_000), type=pa.uint32())
    a = vortex.array(expected)
    compressed = vortex.compress(a)
    assert isinstance(compressed, vortex.FastLanesDeltaArray | vortex.SequenceArray)
    assert compressed.nbytes < a.nbytes
    assert compressed.to_arrow_array() == expected


def test_zigzag_encode() -> None:
    a = vortex.array(pa.array([-1, -1, 0, -1, 1, -1]))
    zarr = vortex.ZigZagArray.encode(a)
    assert isinstance(zarr, vortex.ZigZagArray)
    assert zarr.to_arrow_array() == pa.array([-1, -1, 0, -1, 1, -1])


def test_chunked_encode() -> None:
    chunked = pa.chunked_array([pa.array([0, 1, 2]), pa.array([3, 4, 5])])
    encoded = vortex.array(chunked)
    arrow = encoded.to_arrow_array()
    assert isinstance(arrow, pa.ChunkedArray)
    assert arrow.combine_chunks() == pa.array([0, 1, 2, 3, 4, 5])


def test_table_encode() -> None:
    table = pa.table(  # ty: ignore[no-matching-overload]
        {
            "number": pa.chunked_array([pa.array([0, 1, 2]), pa.array([3, 4, 5])]),
            "string": pa.chunked_array(
                [pa.array(["a", "b", "c"], type=pa.string_view()), pa.array(["d", "e", "f"], type=pa.string_view())]
            ),
        }
    )
    assert isinstance(table, pa.Table)

    encoded = vortex.array(table)
    arrow = encoded.to_arrow_array()
    assert isinstance(arrow, pa.ChunkedArray)
    assert arrow.combine_chunks() == pa.StructArray.from_arrays(
        [pa.array([0, 1, 2, 3, 4, 5]), pa.array(["a", "b", "c", "d", "e", "f"], type=pa.string_view())],
        names=["number", "string"],
    )
