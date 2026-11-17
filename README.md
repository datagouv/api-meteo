# API météo climatologique

CSV extraction of Météo-France climatological data published on data.gouv.fr, via the [tabular-api](https://tabular-api.data.gouv.fr).

## Running

```shell
uv sync
uv run gunicorn api_aio:app_factory --bind 0.0.0.0:3030 --worker-class aiohttp.GunicornWebWorker --workers 4
```

## Usage

```shell
curl "http://localhost:3030/api/clim/base_mens/13/csv/?num_postes=13054001&anneemin=2022&anneemax=2022"
```

- `dataset` (here `base_mens`): one of `base_mens`, `base_decad`, `base_decadagro`, `base_quot_vent`, `base_quot_autres`, `base_hor`, `base_min`
- `dep` (here `13`): département code
- `num_postes`: station ID(s), comma-separated
- `anneemin`, `anneemax`: period, bounds included

All parameters are required.
