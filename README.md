## Local step

1. Install `uv` follow [this](https://github.com/astral-sh/uv?tab=readme-ov-file#installation) or simply run `brew install uv`. `uv` managed both python version and dependencies.
2. Run `uv sync`. This should create a virtual environment in the `.venv` folder and install all dependencies.

## Download g-naf data

1. Go to https://data.gov.au/dataset/ds-dga-19432f89-dc3a-4ef3-b943-5326ef1dbecc/details
2. Download `format icon NOV 24 - Geoscape G-NAF - GDA2020(ZIP)`
3. Go to https://data.gov.au/dataset/ds-dga-bdcf5b09-89bc-47ec-9281-6b8e9ee147aa/details
4. Download `NOV24 - Geoscape Admin Boundaries - ESRI Shapefile - GDA2020(ZIP)`
5. Unzip both files and put them in the `data-sources` folder.
6. See [data-sources/README.md](data-sources/README.md#example-files) for more information.

## Import data

```shell
uv run import-gnaf.py
```

## Query data

```shell
uv run read-gnaf.py
```
