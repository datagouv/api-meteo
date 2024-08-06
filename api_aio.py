from aiohttp import web, ClientSession
import json
import os

PGREST_ENDPOINT = f"http://{os.getenv('PGREST_ENDPOINT')}"
API_ENDPOINT = f"http://{os.getenv('API_ENDPOINT')}"

routes = web.RouteTableDef()

CLIM_INFOS = {
    "base_mens": {
        "date_column": "aaaamm",
        "accepted_columns": [
            "num_poste", "nom_usuel", "lat", "lon", "alti", "aaaamm", "rr", "qrr", "nbrr", "rr_me", "rrab", "qrrab", "rrabdat", "nbjrr1", "nbjrr5", "nbjrr10", "nbjrr30", "nbjrr50", "nbjrr100", "pmerm", "qpmerm", "nbpmerm", "pmerminab", "qpmerminab", "pmerminabdat", "tx", "qtx", "nbtx", "tx_me", "txab", "qtxab", "txdat", "txmin", "qtxmin", "txmindat", "nbjtx0", "nbjtx25", "nbjtx30", "nbjtx35", "nbjtxi20", "nbjtxi27", "nbjtxs32", "tn", "qtn", "nbtn", "tn_me", "tnab", "qtnab", "tndat", "tnmax", "qtnmax", "tnmaxdat", "nbjtn5", "nbjtn10", "nbjtni10", "nbjtni15", "nbjtni20", "nbjtns20", "nbjtns25", "nbjgelee", "tamplim", "qtamplim", "tampliab", "qtampliab", "tampliabdat", "nbtampli", "tm", "qtm", "nbtm", "tmm", "qtmm", "nbtmm", "nbjtms24", "tmmin", "qtmmin", "tmmindat", "tmmax", "qtmmax", "tmmaxdat", "unab", "qunab", "unabdat", "nbun", "uxab", "quxab", "uxabdat", "nbux", "umm", "qumm", "nbum", "tsvm", "qtsvm", "nbtsvm", "etp", "qetp", "fxiab", "qfxiab", "dxiab", "qdxiab", "fxidat", "nbjff10", "nbjff16", "nbjff28", "nbfxi", "fxi3sab", "qfxi3sab", "dxi3sab", "qdxi3sab", "fxi3sdat", "nbjfxi3s10", "nbjfxi3s16", "nbjfxi3s28", "nbfxi3s", "fxyab", "qfxyab", "dxyab", "qdxyab", "fxyabdat", "nbjfxy8", "nbjfxy10", "nbjfxy15", "nbfxy", "ffm", "qffm", "nbffm", "inst", "qinst", "nbinst", "nbsigma0", "nbsigma20", "nbsigma80", "glot", "qglot", "nbglot", "dift", "qdift", "nbdift", "dirt", "qdirt", "nbdirt", "hneigeftot", "qhneigeftot", "hneigefab", "qhneigefab", "hneigefdat", "nbhneigef", "nbjneig", "nbjhneigef1", "nbjhneigef5", "nbjhneigef10", "nbjsolng", "neigetotm", "qneigetotm", "neigetotab", "qneigetotab", "neigetotabdat", "nbjneigetot1", "nbjneigetot10", "nbjneigetot30", "nbjgrel", "nbjorag", "nbjbrou"
        ]
    },
    "base_decad": {
        "date_column": "aaaamm",
        "accepted_columns": [
            "num_poste", "nom_usuel", "lat", "lon", "alti", "aaaamm", "num_decade", "rr", "qrr", "nbrr", "rrab", "qrrab", "rrabdat", "nbjrr1", "nbjrr5", "nbjrr10", "nbjrr30", "nbjrr50", "nbjrr100", "pmerm", "qpmerm", "nbpmerm", "pmerminab", "qpmerminab", "pmerminabdat", "tx", "qtx", "nbtx", "txab", "qtxab", "txdat", "txmin", "qtxmin", "txmindat", "nbjtx0", "nbjtx25", "nbjtx30", "nbjtx35", "nbjtxi20", "nbjtxi27", "nbjtxs32", "tn", "qtn", "nbtn", "tnab", "qtnab", "tndat", "tnmax", "qtnmax", "tnmaxdat", "nbjtn5", "nbjtn10", "nbjtni10", "nbjtni15", "nbjtni20", "nbjtns20", "nbjtns25", "nbjgelee", "tamplim", "qtamplim", "tampliab", "qtampliab", "tampliabdat", "nbtampli", "tm", "qtm", "nbtm", "tmm", "qtmm", "nbtmm", "nbjtms24", "tmmin", "qtmmin", "tmmindat", "tmmax", "qtmmax", "tmmaxdat", "unab", "qunab", "unabdat", "nbun", "uxab", "quxab", "uxabdat", "nbux", "umm", "qumm", "nbum", "tsvm", "qtsvm", "nbtsvm", "fxiab", "qfxiab", "dxiab", "qdxiab", "fxidat", "nbjff10", "nbjff16", "nbjff28", "nbfxi", "fxi3sab", "qfxi3sab", "dxi3sab", "qdxi3sab", "fxi3sdat", "nbjfxi3s10", "nbjfxi3s16", "nbjfxi3s28", "nbfxi3s", "fxyab", "qfxyab", "dxyab", "qdxyab", "fxyabdat", "nbjfxy8", "nbjfxy10", "nbjfxy15", "nbfxy", "ffm", "qffm", "nbffm", "inst", "qinst", "nbinst", "nbsigma0", "nbsigma20", "nbsigma80", "glot", "qglot", "nbglot", "dift", "qdift", "nbdift", "dirt", "qdirt", "nbdirt", "hneigeftot", "qhneigeftot", "hneigefab", "qhneigefab", "hneigefdat", "nbhneigef", "nbjneig", "nbjhneigef1", "nbjhneigef5", "nbjhneigef10", "nbjsolng", "neigetotm", "qneigetotm", "neigetotab", "qneigetotab", "neigetotabdat", "nbjneigetot1", "nbjneigetot10", "nbjneigetot30", "nbgrel", "nbjorag", "nbjbrou"
        ]
    },
    "base_decadagro": {
        "date_column": "aaaamm",
        "accepted_columns": [
            "num_poste", "nom_usuel", "lat", "lon ", "alti", "aaaamm", "num_decade", "rr", "crr", "tn", "ctn", "tx", "ctx", "ffm", "cffm", "tsvm", "ctsvm", "inst", "cinst", "glot", "cglot", "etp", "de"
        ]
    },
    "base_quot_vent": {
        "date_column": "aaaammjj",
        "accepted_columns": [
            "num_poste", "num_poste", "nom_usuel", "lat", "lon", "alti", "aaaammjj", "rr", "qrr", "tn", "qtn", "htn", "qhtn", "tx", "qtx", "htx", "qhtx", "tm", "qtm", "tntxm", "qtntxm", "tampli", "qtampli", "tnsol", "qtnsol", "tn50", "qtn50", "dg", "qdg", "ffm", "qffm", "ff2m", "qff2m", "fxy", "qfxy", "dxy", "qdxy", "hxy", "qhxy", "fxi", "qfxi", "dxi", "qdxi", "hxi", "qhxi", "fxi2", "qfxi2", "dxi2", "qdxi2", "hxi2", "qhxi2", "fxi3s", "qfxi3s", "dxi3s", "qdxi3s", "hxi3s", "qhxi3s", "drr", "qdrr"
        ]
    },
    "base_quot_autres": {
        "date_column": "aaaammjj",
        "accepted_columns": [
           "num_poste", "nom_usuel", "lat", "lon", "alti", "aaaammjj", "dhumec", "qdhumec", "pmerm", "qpmerm", "pmermin", "qpmermin", "inst", "qinst", "glot", "qglot", "dift", "qdift", "dirt", "qdirt", "infrart", "qinfrart", "uv", "quv", "uv_indicex", "quv_indicex", "sigma", "qsigma", "un", "qun", "hun", "qhun", "ux", "qux", "hux", "qhux", "um", "qum", "dhumi40", "qdhumi40", "dhumi80", "qdhumi80", "tsvm", "qtsvm", "etpmon", "qetpmon", "etpgrille", "qetpgrille", "ecoulementm", "qecoulementm", "hneigef", "qhneigef", "neigetotx", "qneigetotx", "neigetot06", "qneigetot06", "neig", "qneig", "brou", "qbrou", "orag", "qorag", "gresil", "qgresil", "grele", "qgrele", "rosee", "qrosee", "verglas", "qverglas", "solneige", "qsolneige", "gelee", "qgelee", "fumee", "qfumee", "brume", "qbrume", "eclair", "qeclair", "nb300", "qnb300", "ba300", "qba300", "tmermin", "qtmermin", "tmermax", "qtmermax"
        ]
    },
    "base_hor": {
        "date_column": "aaaammjjhh",
        "accepted_columns": [
           "num_poste", "nom_usuel", "lat", "lon", "alti", "aaaammjjhh", "rr1", "qrr1", "drr1", "qdrr1", "ff", "qff", "dd", "qdd", "fxy", "qfxy", "dxy", "qdxy", "hxy", "qhxy", "fxi", "qfxi", "dxi", "qdxi", "hxi", "qhxi", "ff2", "qff2", "dd2", "qdd2", "fxi2", "qfxi2", "dxi2", "qdxi2", "hxi2", "qhxi2", "fxi3s", "qfxi3s", "dxi3s", "qdxi3s", "hfxi3s", "qhfxi3s", "t", "qt", "td", "qtd", "tn", "qtn", "htn", "qhtn", "tx", "qtx", "htx", "qhtx", "dg", "qdg", "t10", "qt10", "t20", "qt20", "t50", "qt50", "t100", "qt100", "tnsol", "qtnsol", "tn50", "qtn50", "tchaussee", "qtchaussee", "dhumec", "qdhumec", "u", "qu", "un", "qun", "hun", "qhun", "ux", "qux", "hux", "qhux", "dhumi40", "qdhumi40", "dhumi80", "qdhumi80", "tsv", "qtsv", "pmer", "qpmer", "pstat", "qpstat", "pmermin", "qpermin", "geop", "qgeop", "n", "qn", "nbas", "qnbas", "cl", "qcl", "cm", "qcm", "ch", "qch", "n1", "qn1", "c1", "qc1", "b1", "qb1", "n2", "qn2", "c2", "qc2", "b2", "qcb2", "n3", "qn3", "c3", "qc3", "b3", "qb3", "n4", "qn4", "c4", "qc4", "b4", "qb4", "vv", "qvv", "dvv200", "qdvv200", "ww", "qww", "w1", "qw1", "w2", "qw2", "sol", "qsol", "solng", "qsolng", "tmer", "qtmer", "vvmer", "qvvmer", "etatmer", "qetatmer", "dirhoule", "qdirhoule", "hvague", "qhvague", "pvague", "qpvague", "hneigef", "qhneigef", "neigetot", "qneigetot", "tsneige", "qtsneige", "tubeneige", "qtubeneige", "hneigefi3", "qhneigefi3", "hneigefi1", "qhneigefi1", "esneige", "qesneige", "chargeneige", "qchargeneige", "glo", "qglo", "glo2", "qglo2", "dir", "qdir", "dir2", "qdir2", "dif", "qdif", "dif2", "qdif2", "uv", "quv", "uv2", "quv2", "uv_indice", "quv_indice", "infrar", "qinfrar", "infrar2", "qinfrar2", "ins", "qins", "ins2", "qins2", "tlagon", "qtlagon", "tvegetaux", "qtvegetaux", "ecoulement", "qecoulement"
        ]
    },
    "base_min": {
        "date_column": "aaaammjjhhmn",
        "accepted_columns": [
           "num_poste", "nom_usuel", "lat", "lon", "alti", "aaaammjjhhmn", "rr", "qrr"
        ]
    }
}


@routes.get("/")
async def get_health(request):
    return web.HTTPOk()


def process_total(raw_total: str) -> int:
    # The raw total looks like this: '0-49/21777'
    _, str_total = raw_total.split("/")
    return int(str_total)


async def get_total(
        session: ClientSession,
        url: str
    ):
    res = await session.head(f"{url}&limit=1&", headers={"Prefer": "count=exact"})
    total = process_total(res.headers.get("Content-Range"))
    return total


async def get_resource_data_streamed(
        session: ClientSession,
        url: str,
        accept_format: str = "text/csv",
    ):
    total = await get_total(        
        session,
        url
    )
    for i in range(0, total, 5000):
        async with session.get(
            url=f"{url}&limit=5000&offset={i}", headers={"Accept": accept_format}
        ) as res:
            async for chunk in res.content.iter_chunked(1024):
                yield chunk
            yield b'\n'


async def fetch_data(request, response_type="json"):
    dataset = request.match_info["dataset"]
    dep = request.match_info["dep"]
    num_postes = request.query.get("num_postes")
    anneemin = request.query.get("anneemin")
    anneemax = request.query.get("anneemax")
    columns = request.query.get("columns", "*")

    if not num_postes or not anneemin or not anneemax:
        return web.HTTPBadRequest(reason="Missing required query parameters")
    
    try:
        anneemin_int = int(anneemin)
        anneemax_int = int(anneemax)
    except ValueError:
        return web.HTTPBadRequest(reason="anneemin and anneemax must be valid integers")

    if (anneemax_int - anneemin_int > 5):
        return web.HTTPBadRequest(reason="The range between anneemin and anneemax should not exceed 5 years")

    if anneemin == anneemax:
        anneemax = str(int(anneemin) + 1)

    if dataset not in CLIM_INFOS:
        return web.HTTPBadRequest(reason="Bad dataset provided")

    if num_postes == "*":
        return web.HTTPBadRequest(reason="value * not permitted for num_postes")
    else:
        query_num_postes = []
        for num_poste in num_postes.split(","):
                query_num_postes.append(f"num_poste.eq.{num_poste}")
        query_num_poste = f"&or=({','.join(query_num_postes)})"
    
    if columns != "*":
        for column in columns.split(","):
            if column not in CLIM_INFOS[dataset]["accepted_columns"]:
                return web.HTTPBadRequest(reason="Bad columns provided")
    else:
        columns = ",".join(CLIM_INFOS[dataset]["accepted_columns"])

    url = f"{PGREST_ENDPOINT}/{dataset}_{dep}?select={columns}{query_num_poste}&{CLIM_INFOS[dataset]['date_column']}:=gte.{anneemin}&{CLIM_INFOS[dataset]['date_column']}:=lt.{anneemax}"

    if response_type == "json":
        total = await get_total(
            request.app["csession"],
            url
        )
        page = int(request.query.get("page", 1))
        offset = (page - 1) * 100
        url += f"&limit=100&offset={offset}"
    
    async with request.app["csession"].get(url) as res:
        if response_type == "json":
            data = await res.json()
            links = {
                "next": f"http://{API_ENDPOINT}/api/clim/{dataset}/{dep}/?num_postes={num_postes}&anneemin={anneemin}&anneemax={anneemax}&page={page+1}" if offset + 100 < total else None,
                "prev": f"http://{API_ENDPOINT}/api/clim/{dataset}/{dep}/?num_postes={num_postes}&anneemin={anneemin}&anneemax={anneemax}&page={page-1}" if (page) > 1 else None
            }
            if columns:
                links["next"] = links["next"].split("/?")[0] + f"/?columns={columns}&" + links["next"].split("/")[1] if links["next"] is not None else None
                links["prev"] = links["prev"].split("/?")[0] + f"/?columns={columns}&" + links["prev"].split("/")[1] if links["prev"] is not None else None
            metadata = {
                "links": links,
                "meta": {
                    "page": page,
                    "page_size": 100,
                    "total": total
                }
            }
            return web.json_response(text=json.dumps({"data": data, **metadata}, default=str))
        
        elif response_type == "csv":
            response_headers = {
                "Content-Disposition": f'attachment; filename="clim-{dataset}-{dep}-{anneemin}-{anneemax}.csv"',
                "Content-Type": "text/csv",
            }
            response = web.StreamResponse(headers=response_headers)
            await response.prepare(request)

            async for chunk in get_resource_data_streamed(request.app["csession"], url):
                await response.write(chunk)

            await response.write_eof()
            return response


@routes.get('/api/clim/{dataset}/{dep}/')
async def get_data(request):
    return await fetch_data(request, response_type="json")


@routes.get('/api/clim/{dataset}/{dep}/csv/')
async def get_data_csv(request):
    return await fetch_data(request, response_type="csv")


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
