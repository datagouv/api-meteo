import re

from aiohttp import web, ClientSession
from datagouv import Dataset


TABULAR_BASE_URL = "https://tabular-api.data.gouv.fr/api"

routes = web.RouteTableDef()

CLIM_INFOS = {
    "base_mens": {
        "date_column": "AAAAMM",
        "dataset_id": "6569b3d7d193b4daf2b43edc",
    },
    "base_decad": {
        "date_column": "AAAAMM",
        "dataset_id": "6569b4a48a4161faec6b2779",
    },
    "base_decadagro": {
        "date_column": "AAAAMM",
        "dataset_id": "6569af36ba0c3d2f9d4bf98c",
    },
    "base_quot_vent": {
        "date_column": "AAAAMMJJ",
        "dataset_id": "6569b51ae64326786e4e8e1a",
    },
    "base_quot_autres": {
        "date_column": "AAAAMMJJ",
        "dataset_id": "6569b51ae64326786e4e8e1a",
    },
    "base_hor": {
        "date_column": "AAAAMMJJHH",
        "dataset_id": "6569b4473bedf2e7abad3b72",
    },
    "base_min": {
        "date_column": "AAAAMMJJHHMN",
        "dataset_id": "6569ad61106d1679c93cdf77",
    }
}


@routes.get("/health/")
async def get_health(request):
    return web.HTTPOk()


def get_file_period(title: str, has_suffix: bool) -> tuple[int, int]:
    period = title.split("_")[-2 if has_suffix else -1].split("-")
    return int(period[0]), int(period[1])


def format_period(year: str, date_col: str, date_col_type: str) -> str:
    if date_col_type == "date":
        return year + "-01-01"
    elif date_col_type == "datetime":
        return year + "-01-01T00:00:00"
    return year + "0" * (len(date_col) - len(year))


async def fetch_data(request):
    table_group = request.match_info["dataset"]
    if table_group not in CLIM_INFOS:
        return web.HTTPBadRequest(reason=f"{table_group} is not a valid dataset")
    dep = request.match_info["dep"]
    num_postes = request.query.get("num_postes")
    anneemin = request.query.get("anneemin")
    anneemax = request.query.get("anneemax")
    if not dep or not num_postes or not anneemin or not anneemax:
        return web.HTTPBadRequest(reason="Missing required query parameters")
    anneemaxfile = f"-{anneemax}" if anneemin != anneemax else ""
    
    dataset = Dataset(CLIM_INFOS[table_group]["dataset_id"])
    of_interest = [
        res
        for res in dataset.resources
        if res.type == "main"
        and re.search(f"departement_{dep}", res.title)
        and (
            re.search(
                "_RR-T-Vent" if table_group == "base_quot_vent" else "autres-parametres",
                res.title,
            )
            if table_group.startswith("base_quot")
            else True
        )
        and (period := get_file_period(res.title, table_group.startswith("base_quot")))[1] >= int(anneemin)
        and period[0] <= int(anneemax)
    ]
    if not of_interest:
        return web.HTTPBadRequest(reason="Could not retrieve data for the requested period")
    date_col = CLIM_INFOS[table_group]["date_column"]
    content = ""
    wrote_headers = False
    for res in of_interest:
        tabular_url = f"https://tabular-api.data.gouv.fr/api/resources/{res.id}/"
        profile = await request.app["csession"].get(tabular_url + "profile/")
        profile.raise_for_status()
        profile = await profile.json()
        date_col_type = profile["profile"]["columns"][date_col]["python_type"]
        data_url = (
            f"{tabular_url}data/csv/?NUM_POSTE__in={num_postes}"
            f"&{date_col}__greater={format_period(anneemin, date_col, date_col_type)}"
            f"&{date_col}__less={format_period(str(int(anneemax) + 1), date_col, date_col_type)}"
        )
        resp = await request.app["csession"].get(data_url)
        resp.raise_for_status()
        if not wrote_headers:
            content += await resp.text()
            wrote_headers = True
        else:
            text = await resp.text()
            content += "\n".join(text.split("\n")[1:])  # popping headers
    return web.Response(
        text=content,
        headers={
            "Content-Disposition": f'attachment; filename="clim-{table_group}-{dep}-{anneemin}{anneemaxfile}.csv"',
            "Content-Type": "text/csv",
        }
    )


@routes.get('/api/clim/{dataset}/{dep}/csv/')
async def get_data_csv(request):
    return await fetch_data(request)


async def app_factory():
    async def on_startup(app):
        app["csession"] = ClientSession()

    async def on_cleanup(app):
        await app["csession"].close()

    app = web.Application()
    app.add_routes(routes)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    return app


def run():
    web.run_app(app_factory(), path="0.0.0.0", port="3030")


if __name__ == "__main__":
    run()
