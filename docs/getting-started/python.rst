:tocdepth: 1

Python
======

Install
-------

::

   pip install vortex-data

Convert
-------

You can either use your own Arrow data or download the `example file used here
<https://docs.vortex.dev/_static/example.arrow>`__ (an Arrow IPC file).

Use Arrow to read the file and then use :func:`~vortex.array` to construct an uncompressed
Vortex array:

.. doctest::

   >>> import pyarrow.feather as feather
   >>> import vortex as vx
   >>> table = feather.read_table("_static/example.arrow")
   >>> vtx = vx.array(table)
   >>> vtx.nbytes
   141024

Write
-----

Use :func:`~vortex.io.write` to write the Vortex array to disk:

.. doctest::

   >>> import vortex as vx
   >>>
   >>> vx.io.write(vtx, "example.vortex")  # doctest: +SKIP

Small Vortex files (this one is just 71KiB) currently have substantial overhead relative to their
size. This will be addressed shortly. On files with at least tens of megabytes of data, Vortex
compresses well relative to other columnar formats.

.. doctest::

   >>> from os.path import getsize
   >>> getsize("example.vortex") / getsize("_static/example.arrow") # doctest: +SKIP
   2.4...

Read
----

Use :func:`~vortex.open` to open and read the Vortex array from disk:

.. doctest::

   >>> import vortex as vx
   >>>
   >>> cvtx = vx.open("example.vortex").scan().read_all()  # doctest: +SKIP


Vortex is architected to achieve fast random access, in many cases hundreds of times faster
than what row-group oriented formats achieve.

If you have an external index that gives you specific rows to pull out of the Vortex file, you can skip a lot more
IO and decoding and read just the data that is relevant to you:

.. doctest::

    >>> import vortex as vx
    >>>
    >>> vf = vx.open("example.vortex")  # doctest: +SKIP
    >>> # row indices must be ordered and unique
    >>> indices = vx.array([1, 2, 10])
    >>> result = vf.scan(indices=indices).read_all()  # doctest: +SKIP
    >>> assert len(result) == 3  # doctest: +SKIP
