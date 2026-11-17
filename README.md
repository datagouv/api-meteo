# API météo climatologique

Extraction en CSV des données de la base climatologique de Météo-France publiées sur data.gouv.fr, via la [tabular-api](https://tabular-api.data.gouv.fr).

## Lancement

```shell
uv venv && uv pip sync requirements.txt
.venv/bin/gunicorn api_aio:app_factory --bind 0.0.0.0:3030 --worker-class aiohttp.GunicornWebWorker --workers 4
```

## Utilisation

```shell
curl "http://localhost:3030/api/clim/base_mens/13/csv/?num_postes=13054001&anneemin=2022&anneemax=2022"
```

`dataset` (ici `base_mens`) parmi `base_mens`, `base_decad`, `base_decadagro`, `base_quot_vent`, `base_quot_autres`, `base_hor`, `base_min`.

## Dépendances

Déclarées dans `pyproject.toml`, figées (avec hashes) dans `requirements.txt`. Après modification :

```shell
uv pip compile pyproject.toml --generate-hashes --universal -o requirements.txt
```
