# Create a Vortex file

If you haven't already, download the sample data (see [Install](install.md#sample-data)):

```bash
curl -O https://docs.vortex.dev/_static/example.arrow
```

Write it to Vortex with the Python library, which applies compression automatically:

```bash
pip install vortex-data pyarrow
python -c "import pyarrow.feather as feather, vortex as vx; \
vx.io.write(feather.read_table('example.arrow'), 'example.vortex')"
```

This produces `example.vortex` in the same directory, compressed with the default BtrBlocks
strategy. Any Arrow table or record batch reader can be written the same way; see the
[Python quickstart](python.rst) and the [PyArrow guide](../user-guide/pyarrow.md).
