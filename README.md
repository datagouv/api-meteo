# API for meteo clim

- API of data from "base de données climatologiques"

```
gunicorn api_aio:app_factory --bind 0.0.0.0:3030 --worker-class aiohttp.GunicornWebWorker --workers 4
```

