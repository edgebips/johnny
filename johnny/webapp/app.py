#!/usr/bin/env python3
"""Web application for all the files.
"""

__copyright__ = "Copyright (C) 2021  Martin Blais"
__license__ = "GNU GPLv2"

from decimal import Decimal
from functools import partial
from os import path
from typing import Any, Dict, List, Mapping, NamedTuple, Optional, Tuple
import io
import datetime
import functools
import itertools
import os
import re
import threading
import logging
import tempfile

import dateutil.parser
import numpy as np
import networkx as nx
import seaborn as sns

try:
    from johnny.exports import beanjohn
except ImportError:
    beanjohn = None

sns.set()
from matplotlib import pyplot
import matplotlib

matplotlib.use("Agg")
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from more_itertools import first

import flask

from johnny.base import chains as chainslib
from johnny.base import config as configlib
from johnny.base import instrument
from johnny.base import mark
from johnny.base import transactions as txnlib
from johnny.base import match
from johnny.base import recap as recaplib
from johnny.base.etl import petl, Table, Record

ChainStatus = chainslib.ChainStatus
Chain = configlib.Chain


ZERO = Decimal(0)
Q = Decimal("0.01")


approot = path.dirname(__file__)
app = flask.Flask(
    "buff",
    static_folder=path.join(approot, "static"),
    template_folder=path.join(approot, "templates"),
)
app.logger.setLevel(logging.INFO)


class State(NamedTuple):
    """Application state."""

    transactions: Table
    positions: Table
    chains: Table
    chains_map: Mapping[str, configlib.Chain]
    config: configlib.Config


def get_dict_attribute(mapping: Mapping[str, Any], attr: str, key: str) -> Any:
    value = mapping.get(key, None)
    if value is None:
        return None
    return getattr(value, attr, None)


def Initialize():
    # Make sure we have a configuration to work from.
    #
    # Note: We're reading the clean config produced by the import.
    config_filename = os.getenv("JOHNNY_CONFIG")
    if not config_filename:
        logging.error(
            "Error: No configuration file set set. Please set JOHNNY_CONFIG to "
            "your .pbtxt file with a text-formatted config.proto."
        )
        raise SystemExit

    # TODO(blais): Restore usage of the ledger. TODO(blais): Use a field from
    # the config file, centralize processing with the data preparation, from the
    # config.
    ledger: str = os.getenv("JOHNNY_LEDGER")

    global STATE
    with _STATE_LOCK:
        if STATE is None:
            app.logger.info(
                f"Initializing application state from '{config_filename}'..."
            )

            # Get the imported transactions.
            config = configlib.ParseFile(config_filename)

            # Read chains table.
            # Note: You have to use the output chains (as opposed to the input
            # chain), because they have to match the imported table of data.
            # Otherwise, brand new trades wouldn't show a corresponding chain
            # object.
            chains_db = configlib.ReadChains(config.output.chains_db)
            chains_map = {c.chain_id: c for c in chains_db.chains}

            # Filter the chains table removing ignored groups.
            ignore_groups = set(config.presentation.ignore_groups)
            ignore_tags = set(config.presentation.ignore_tags)
            ignore_chains = set(
                chain.chain_id
                for chain in chains_db.chains
                if (chain.group in ignore_groups or set(chain.tags) & ignore_tags)
            )
            chains_table = petl.frompickle(config.output.chains_pickle).selectnotin(
                "chain_id", ignore_chains
            )

            if config.presentation.ignore_mindate:
                mindate = dateutil.parser.parse(
                    config.presentation.ignore_mindate
                ).date()
                chains_table = chains_table.selectge("maxdate", mindate)

            chain_ids = set(chains_table.values("chain_id"))

            # Filter the transactions table removing ignored chains (from above).
            transactions = petl.frompickle(config.output.transactions_pickle).selectin(
                "chain_id", chain_ids
            )

            # Extract current positions from marks.
            positions = transactions.selecteq("rowtype", txnlib.Type.Mark)

            STATE = State(transactions, positions, chains_table, chains_map, config)
            app.logger.info("Done.")

    return STATE


STATE = None
_STATE_LOCK = threading.Lock()


def ToHtmlString(ftable: Table, cls: str, ids: List[str] = None) -> bytes:
    # Convert some set of tables to percents for display.
    fieldnames = set(ftable.fieldnames())
    table = ftable
    for name, fmtfunc in [
        ("vol_real", "{:.1%}".format),
        ("return_real", "{:.1%}".format),
        ("stdev_real", lambda v: v or ZERO),
        ("vol_impl", "{:.1%}".format),
        ("return_impl", "{:.1%}".format),
        ("stdev_impl", lambda v: v or ZERO),
    ]:
        if name in fieldnames:
            table = table.convert(name, fmtfunc)

    sink = petl.MemorySource()
    table.tohtml(sink, vrepr=vrepr)
    html = sink.getvalue().decode("utf8")
    html = re.sub(
        "class='petl'", f"class='display compact nowrap cell-border' id='{cls}'", html
    )

    # Add class to <th> tags.
    fnames = iter(table.fieldnames())
    html = re.sub(
        "^<th>", lambda match: "<th class={}>".format(next(fnames)), html, flags=re.M
    )

    # Add column ids for each column of the header. We use this in JS to
    # identify columns by name.
    if ids:
        iter_ids = itertools.chain(["header"], iter(ids))
        html = re.sub("<tr>", lambda _: '<tr id="{}">'.format(next(iter_ids)), html)

    # Add a footer, for partial summaries.
    buf = io.StringIO()
    pr = partial(print, file=buf)
    pr(
        "<tfoot>",
    )
    pr(
        "<tr>",
    )
    for fname in table.fieldnames():
        pr(f'<th class="footcol-{fname}"></th>')
    pr("</tr>")
    pr("</tfoot>")
    html = re.sub("</table>", "{}</table>".format(buf.getvalue()), html)

    return html


def vrepr(value: Any) -> str:
    """Universal rendering function, rendering decimals with commas."""
    if isinstance(value, Decimal):
        sign, digits, exp = value.normalize().as_tuple()
        return "{value:,.{width}f}".format(width=max(2, -exp), value=value)
    return str(value)


def GetNavigation() -> Dict[str, str]:
    """Get navigation bar."""
    return {
        "page_active": flask.url_for("active"),
        "page_expiring": flask.url_for("expiring"),
        "page_recap": flask.url_for("recap_today"),
        "page_leverage": flask.url_for("leverage"),
        "page_chains": flask.url_for("chains"),
        "page_transactions": flask.url_for("transactions"),
        "page_positions": flask.url_for("positions"),
        "page_stats": flask.url_for("stats"),
        "page_timeline": flask.url_for("timeline"),
    }


def AddUrl(endpoint: str, kwdarg: str, value: Any) -> str:
    if value is not None:
        url = flask.url_for(endpoint, **{kwdarg: value})
        return "<a href={}>{}</a>".format(url, value)
    else:
        return value


def FilterChains(table: Table) -> Table:
    """Filter down the list of chains from the params."""
    selected_chain_ids = flask.request.args.get("chain_ids")
    if selected_chain_ids:
        selected_chain_ids = selected_chain_ids.split(",")
        table = table.selectin("chain_id", selected_chain_ids)
    return table


# TODO(blais): Remove threshold, exclude non-trades from input.
def RatioDistribution(num, denom, threshold=1000):
    """Compute a P/L percent distribution."""
    mask = denom > 1e-6
    num, denom = num[mask], denom[mask]
    mask = (num < threshold) & (num > -threshold)
    num, denom = num[mask], denom[mask]
    return num / denom * 100


def RenderHistogram(data: np.array, title: str) -> bytes:
    fig, ax = pyplot.subplots(figsize=(6, 3))
    fig.tight_layout()
    ax.set_title(title)
    ax.hist(data, bins="auto", edgecolor="black", linewidth=0.5)
    buf = io.BytesIO()
    FigureCanvas(fig).print_png(buf)
    return buf.getvalue()


# -------------------------------------------------------------------------------
# Resource handlers. The various available web pages are defined here.


@app.route("/")
def home():
    return flask.redirect(flask.url_for("active"))


@app.route("/favicon.ico")
def favicon():
    return flask.redirect(flask.url_for("static", filename="favicon.ico"))


@app.errorhandler(404)
def resource_not_found(e):
    return flask.jsonify(error=str(e)), 404


def render_chains(chains: Table) -> flask.Response:
    ids = chains.values("chain_id")
    chains = chains.convert("chain_id", partial(AddUrl, "chain", "chain_id"))
    return flask.render_template(
        "chains.html", table=ToHtmlString(chains, "chains", ids), **GetNavigation()
    )


@app.route("/active")
def active():
    return render_chains(STATE.chains.selectin("status", {"ACTIVE"}))


@app.route("/expiring")
def expiring():
    days = int(flask.request.args.get("days", 20))
    today = datetime.date.today()
    min_dte = (
        STATE.transactions.selecteq("rowtype", txnlib.Type.Mark)
        .applyfn(instrument.Expand, "symbol")
        .selecttrue("expiration")
        .addfield("dte", lambda r: (r.expiration - today).days)
        .selectle("dte", days)
        .aggregate("chain_id", {"min_dte": ("dte", min)})
    )
    return render_chains(STATE.chains.join(min_dte, "chain_id").movefield("min_dte", 1))


@app.route("/chains")
def chains():
    return render_chains(STATE.chains)


@app.route("/chain/<chain_id>")
def chain(chain_id: str):
    # Get the chain object from the configuration.
    chain_obj = STATE.chains_map.get(chain_id)
    if chain_obj is None:
        flask.abort(404, description=f"Chain '{chain_id}' not found")

    # Isolate the chain summary data.
    chain = STATE.chains.selecteq("chain_id", chain_id)

    # Isolate the chain transactional data.
    txns = STATE.transactions.selecteq("chain_id", chain_id)
    txns = instrument.Expand(txns, "symbol")

    # Get the corresponding set of matches.
    matches = match.GetChainMatchesFromTransactions(
        txns, match.ShortBasisReportingMethod.INVERT
    )

    # Split up P/L from static and dynamic deltas.
    static, dynamic = txns.biselect(
        lambda r: r.instype in {"Equity", "Future", "Crypto"}
    )

    def agg_field(table, fieldname):
        agg_table = table.aggregate(None, {fieldname: (fieldname, sum)})
        values = agg_table.values(fieldname)
        if not values:
            return ZERO
        return next(iter(values)).quantize(Q)

    pnl_static = agg_field(static, "cost")
    pnl_dynamic = agg_field(dynamic, "cost")
    pnl_cash = agg_field(txns, "cash")

    # TODO(blais): Implement SVG and isolate its rendering to a function.
    history_html = RenderHistoryText(txns) if 1 else RenderHistorySVG(txns)

    args = dict(
        chain_id=chain_id,
        comment=chain_obj.comment if chain_obj else "",
        chain=ToHtmlString(chain, "chain_summary"),
        chain_proto=flask.url_for("chain_proto", chain_id=chain_id),
        transactions=ToHtmlString(txns, "chain_transactions"),
        matches=ToHtmlString(matches, "chain_matches"),
        history=history_html,
        graph=flask.url_for("chain_graph", chain_id=chain_id),
        xrefs=chain_obj.xrefs,
        pnl_static=pnl_static,
        pnl_dynamic=pnl_dynamic,
        pnl_cash=pnl_cash,
        **GetNavigation(),
    )
    if beanjohn:
        args.update(bchain=flask.url_for("bchain", chain_id=chain_id))
    return flask.render_template("chain.html", **args)


# Note: This will never be called if we couldn't import Beancount, i.e., if
# Beancount isn't installed. Beancount installation is optional to Johnny.
def bchain(chain_id: str):
    """Render the chain to beancount."""

    # Get the chain object from the configuration.
    chain_obj = STATE.chains_map.get(chain_id)

    # Isolate the chain summary data.
    chain = STATE.chains.selecteq("chain_id", chain_id)
    chain = next(iter(chain.records()))

    # Isolate the chain transactional data.
    txns = STATE.transactions.selecteq("chain_id", chain_id)
    txns = instrument.Expand(txns, "symbol")

    # Render to text.
    buf = io.StringIO()
    pr = partial(print, file=buf)
    pr(";; Chain\n")
    source = ""
    beanjohn.RenderChainToBeancount(STATE.config, chain, source, buf)
    pr("")
    pr(";; Transactions\n")
    beanjohn.RenderTransactionsToBeancount(STATE.config, chain, txns, source, buf)

    response = flask.make_response(buf.getvalue(), 200)
    response.mimetype = "text/plain"
    return response


if beanjohn:
    bchain = app.route("/bchain/<chain_id>")(bchain)


@app.route("/chain_proto/<chain_id>")
def chain_proto(chain_id: str):
    # Get the chain object from the configuration.
    chain_obj = STATE.chains_map.get(chain_id)
    if chain_obj is None:
        chain_obj = configlib.Chain()
        chain_obj.chain_id = chain_id

    chains_db = configlib.Chains()
    chains_db.chain.add().CopyFrom(chain_obj)
    response = flask.make_response(configlib.ToText(chains_db), 200)
    response.mimetype = "text/plain"
    return response


@app.route("/chain_protos")
def chain_protos():
    chains_table = FilterChains(STATE.chains)
    chains_db = configlib.Chains()
    for rec in chains_table.records():
        chains_db.chains.add().CopyFrom(STATE.chains_map.get(rec.chain_id))
    response = flask.make_response(configlib.ToText(chains_db), 200)
    response.mimetype = "text/plain"
    return response


@app.route("/chain_names")
def chain_names():
    chains_table = FilterChains(STATE.chains)
    buf = io.StringIO()
    pr = functools.partial(print, file=buf)
    for rec in chains_table.sort("underlyings").records():
        pr(rec.chain_id)
    response = flask.make_response(buf.getvalue(), 200)
    response.mimetype = "text/plain"
    return response


def RenderHistoryText(txns: Table) -> str:
    """Render trade history to text."""
    buf = io.StringIO()

    fmt = "{r.instruction}/{r.effect} {r.quantity} {r.symbol} @ {r.price}"

    def RenderStatic(rows):
        return "; ".join(fmt.format(r=row) for row in rows if row.putcall is None)

    def RenderPuts(rows):
        return "; ".join(
            fmt.format(r=row) for row in rows if row.putcall and row.putcall[0] == "P"
        )

    def RenderCalls(rows):
        return "; ".join(
            fmt.format(r=row) for row in rows if row.putcall and row.putcall[0] == "C"
        )

    def Accrue(prv, cur, _) -> Decimal:
        last = prv.accr if prv else ZERO
        return last + cur.cost

    agg = {
        "static": (None, RenderStatic),
        "puts": (None, RenderPuts),
        "calls": (None, RenderCalls),
        "cost": ("cost", sum),
    }
    rendered_rows = txns.aggregate(["datetime", "order_id"], agg).addfieldusingcontext(
        "accr", Accrue
    )
    pr = functools.partial(print, file=buf)
    pr("<pre>")
    pr(rendered_rows.lookallstr())
    pr("</pre>")

    return buf.getvalue()


def RenderHistorySVG(txns: Table) -> str:
    """Render an SVG version of the chains history."""

    # Figure out parameters to scale for rendering.
    clean_txns = txns.sort(["datetime", "strike"]).cut(
        "datetime", "description", "strike", "cost"
    )
    strikes = {strike for strike in clean_txns.values("strike") if strike is not None}
    if not strikes:
        return "No transactions."
    min_strike = min(strikes)
    max_strike = max(strikes)
    diff_strike = max_strike - min_strike
    if diff_strike == 0:
        diff_strike = 1
    width = 1000

    svg = io.StringIO()
    pr = partial(print, file=svg)

    pr(f'<svg viewBox="-150 0 1300 1500" xmlns="http://www.w3.org/2000/svg">')
    pr("<style>")
    pr(
        """
            .small { font-size: 7px; }
            .normal { font-size: 9px; }
    """
    )
    pr("</style>")

    # TODO(blais): Render this better, it's ugly.
    pr(
        f'<line x1="0" y1="4" x2="1000" y2="4" style="stroke:#cccccc;stroke-width:0.5" />'
    )
    for strike in sorted(strikes):
        x = int((strike - min_strike) / diff_strike * width)
        pr(
            f'<line x1="{x}" y1="2" x2="{x}" y2="6" style="stroke:#333333;stroke-width:0.5" />'
        )
        pr(f'<text text-anchor="middle" x="{x}" y="12" class="small">{strike}</text>')
    pr()

    y = 20
    prev_time = None
    for r in clean_txns.sort("datetime").records():
        if prev_time is not None and prev_time != r.datetime:
            y += 30
        # print(rec, file=svg)
        prev_time = r.datetime

        x = int((r.strike - min_strike) / diff_strike * width)
        pr(
            f'<text text-anchor="middle" x="{x}" y="{y}" class="normal">{r.description}</text>'
        )
        y += 12

    pr("</svg>")
    return svg.getvalue()


@app.route("/chain/<chain_id>/graph.png")
def chain_graph(chain_id: str):
    txns = STATE.transactions.selecteq("chain_id", chain_id)
    txns = instrument.Expand(txns, "symbol")
    graph = chainslib.CreateGraph(txns, [STATE.chains_map[chain_id]])

    for name in graph.nodes:
        node = graph.nodes[name]
        if node["type"] == "txn":
            rec = node["rec"]
            node["label"] = "{}\n{}".format(rec.datetime, rec.description)
        elif node["type"] == "order":
            node["label"] = "order\n{}".format(name)
        elif node["type"] == "match":
            node["label"] = "match\n{}".format(name)

    agraph = nx.nx_agraph.to_agraph(graph)
    agraph.layout("dot")
    with tempfile.NamedTemporaryFile(suffix=".png", mode="w") as tmp:
        agraph.draw(tmp.name)
        tmp.flush()
        with open(tmp.name, "rb") as infile:
            contents = infile.read()
    return flask.Response(contents, mimetype="image/png")


@app.route("/transactions")
def transactions():
    table = STATE.transactions.convert("chain_id", partial(AddUrl, "chain", "chain_id"))
    return flask.render_template(
        "transactions.html",
        table=ToHtmlString(table, "transactions"),
        **GetNavigation(),
    )


@app.route("/positions")
def positions():
    return flask.render_template(
        "positions.html",
        table=ToHtmlString(STATE.positions, "positions"),
        **GetNavigation(),
    )


# TODO(blais): We need to handle all types.
def GetNotional(rec: Record) -> Decimal:
    """Compute an estimate of the notional."""
    if rec.instype in {"EquityOption", "FutureOption"}:
        if rec.putcall[0] == "P":
            notional = rec.quantity * rec.multiplier * rec.strike
        else:
            notional = ZERO
    elif rec.instype in {"Equity", "Future"}:
        notional = rec.quantity * rec.multiplier * rec.price
    else:
        raise ValueError(f"Invalid instrument type: {rec.instype}")
    return notional.quantize(Q)


@app.route("/stats/")
def stats():
    # Compute stats on winners and losers.
    orig_chains = FilterChains(STATE.chains)

    def PctCr(rec: Record):
        return 0 if rec.init == 0 else rec.pnl_chain / rec.init

    chains = orig_chains.addfield("pct_cr", PctCr)
    win, los = chains.biselect(lambda r: r.pnl_chain > 0)
    pnl = np.array(chains.values("pnl_chain"))
    pnl_win = np.array(win.values("pnl_chain"))
    pnl_los = np.array(los.values("pnl_chain"))
    init_cr = np.array(chains.values("init"))

    pct_cr = np.array(chains.values("pct_cr"))
    pct_cr_win = np.array(win.values("pct_cr"))
    pct_cr_los = np.array(los.values("pct_cr"))

    def Quantize(value):
        return Decimal(value).quantize(Decimal("0"))

    rows = [
        ["Description", "Stat", "Stat%", "Description"],
        [
            "P/L",
            "${}".format(Quantize(np.sum(pnl) if pnl.size else ZERO)),
            "",
            "",
        ],
        [
            "# of wins",
            "{}/{}".format(len(pnl_win), len(pnl)),
            "{:.1%}".format(len(pnl_win) / len(pnl)),
            "% of wins",
        ],
        [
            "Avg init credits",
            "${}".format(Quantize(np.mean(init_cr))),
            "",
            "",
        ],
        [
            "Avg P/L per trade",
            "${}".format(Quantize(np.mean(pnl) if pnl.size else ZERO)),
            "{:.1%}".format(np.mean(pct_cr) if pct_cr.size else ZERO),
            "Avg %cr per trade",
        ],
        [
            "Avg P/L win",
            "${}".format(Quantize(np.mean(pnl_win) if pnl_win.size else ZERO)),
            "{:.1%}".format(np.mean(pct_cr_win) if pct_cr_win.size else ZERO),
            "Avg %cr win",
        ],
        [
            "Avg P/L loss",
            "${}".format(Quantize(np.mean(pnl_los) if pnl_los.size else ZERO)),
            "{:.1%}".format(np.mean(pct_cr_los) if pct_cr_los.size else ZERO),
            "Avg %cr loss",
        ],
        [
            "Max win",
            "${}".format(Quantize(np.max(pnl_win) if pnl_win.size else ZERO)),
            ""  # '{:.1%}'.format(Quantize(np.max(pct_cr_win) if pct_cr_win.size else ZERO)),
            "",  # 'Max %cr win'
        ],
        [
            "Max loss",
            "${}".format(Quantize(np.min(pnl_los) if pnl_los.size else ZERO)),
            "",  # '{:.1%}'.format(Quantize(np.max(pct_cr_los) if pct_cr_los.size else ZERO)),
            "",  #'Max %cr los'
        ],
    ]
    stats_table = petl.wrap(rows)

    chain_ids = flask.request.args.get("chain_ids")
    return flask.render_template(
        "stats.html",
        stats_table=ToHtmlString(stats_table, "stats"),
        chains=ToHtmlString(orig_chains, "chains"),
        pnlhist=flask.url_for("stats_pnlhist", chain_ids=chain_ids),
        pnlpctinit=flask.url_for("stats_pnlpctinit", chain_ids=chain_ids),
        pnlinit=flask.url_for("stats_pnlinit", chain_ids=chain_ids),
        **GetNavigation(),
    )


@app.route("/stats/pnlhist.png")
def stats_pnlhist():
    chains = FilterChains(STATE.chains)
    pnl = np.array(chains.values("pnl_chain"))
    pnl = [v for v in pnl if -10000 < v < 10000]
    image = RenderHistogram(pnl, "P/L ($)")
    return flask.Response(image, mimetype="image/png")


@app.route("/stats/pnlpctinit.png")
def stats_pnlpctinit():
    chains = FilterChains(STATE.chains)
    pnl = np.array(chains.values("pnl_chain")).astype(float)
    creds = np.array(chains.values("init")).astype(float)
    data = RatioDistribution(pnl, creds)
    image = RenderHistogram(data, "P/L (%/Initial Credits)")
    return flask.Response(image, mimetype="image/png")


@app.route("/stats/pnlinit.png")
def stats_pnlinit():
    chains = FilterChains(STATE.chains)
    init = np.array(chains.values("init")).astype(float)
    image = RenderHistogram(init, "Initial Credits ($)")
    return flask.Response(image, mimetype="image/png")


@app.route("/recap")
def recap_today():
    return flask.redirect(
        flask.url_for("recap", date=datetime.date.today().isoformat())
    )


@app.route("/recap/<date>")
def recap(date: str):
    date = dateutil.parser.parse(date).date()
    chains = recaplib.get_chains_at_date(
        STATE.transactions, STATE.chains, STATE.chains_map, date
    ).convert("chain_id", partial(AddUrl, "chain", "chain_id"))
    summary = recaplib.get_summary(chains)
    params = GetNavigation()
    params["date"] = date
    day = datetime.timedelta(days=1)
    params["prev_day"] = flask.url_for("recap", date=(date - day).isoformat())
    params["next_day"] = flask.url_for("recap", date=(date + day).isoformat())
    params["weekday"] = date.strftime("%A")
    params["summary"] = ToHtmlString(summary, "summary")
    if 1:
        for action in recaplib.ACTIONS:
            action_table = chains.selecteq("action", action).cutout("action")
            params[action] = (
                ToHtmlString(action_table, action) if action_table.nrows() > 0 else ""
            )
    else:
        # Single table rendering.
        params["chains"] = ToHtmlString(chains, "chains")

    return flask.render_template("recap.html", **params)


@app.route("/timeline")
def timeline():
    return flask.render_template(
        "timeline.html",
        timeline_group_png=flask.url_for("timeline_group_png"),
        timeline_strategy_png=flask.url_for("timeline_strategy_png"),
        timeline_account_png=flask.url_for("timeline_account_png"),
        **GetNavigation(),
    )


def get_timeline_chains():
    """Return a table of chains suitable to plotting a timeline."""
    return STATE.chains.convert("pnl_chain", float).cut(
        "maxdate", "account", "group", "strategy", "pnl_chain"
    )


def plot_timeline(chains: Table, fieldname: str) -> flask.Response:
    """Plot a timeline of one of a few supported breakdown types."""

    # Build a pivot table by date.
    total = chains.cut("maxdate", "pnl_chain").aggregate(
        "maxdate", {"pnl_day": ("pnl_chain", sum)}
    )
    pivot = chains.pivot("maxdate", fieldname, "pnl_chain", sum).replaceall(None, 0)

    # Convert to Pandas and plot.
    df_total = total.todataframe().set_index("maxdate").cumsum()
    df_pivot = pivot.todataframe().set_index("maxdate").cumsum()

    fig, ax = pyplot.subplots()
    pyplot.tight_layout()
    df_total.plot(ax=ax, figsize=(24, 12), linewidth=3)
    df_pivot.plot(ax=ax, figsize=(24, 8))
    buf = io.BytesIO()
    FigureCanvas(fig).print_png(buf)
    return flask.Response(buf.getvalue(), mimetype="image/png")


@app.route("/timeline_group.png")
def timeline_group_png():
    chains = get_timeline_chains()
    return plot_timeline(chains, "group")


@app.route("/timeline_strategy.png")
def timeline_strategy_png():
    chains = get_timeline_chains()
    return plot_timeline(chains, "strategy")


@app.route("/timeline_account.png")
def timeline_account_png():
    chains = get_timeline_chains()
    return plot_timeline(chains, "account")


def get_signed_quantity(r: Record) -> Decimal:
    """Convert mark transaction to position (investing the sign)."""
    sign = +1 if r.instruction == "SELL" else -1
    return sign * r.quantity


def get_notional(r: Record) -> Decimal:
    """Calculate the notional risk of the position."""
    mquantity = r.squantity * r.multiplier
    if r.instype in {"EquityOption", "FutureOption"}:
        return (mquantity * r.strike).quantize(Q)
    elif r.instype in {"Equity"}:
        return -mquantity * r.price
    elif r.instype in {"Future"}:
        return -mquantity * r.price
    else:
        return ZERO


def get_downside_notional(r: Record) -> Decimal:
    """Return downside (upside) risk notiona."""
    if r.putcall in {"P", "C"}:
        return r.notional if r.putcall == "P" else ZERO
    return r.notional


def get_upside_notional(r: Record) -> Decimal:
    """Return downside (upside) risk notiona."""
    if r.putcall in {"P", "C"}:
        return r.notional if r.putcall == "C" else ZERO
    return -r.notional


@app.route("/leverage")
def leverage():
    # Calculate notional equivalent exposure for all positions.
    notional = (
        STATE.positions
        # Convert Mark rows to position quantities.
        .applyfn(instrument.Expand, "symbol")
        .addfield("squantity", get_signed_quantity)
        .cutout("instruction", "quantity")
        # Compute the put/call notional risks associated with the position.
        .addfield("notional", get_notional)
        # Join in the group and strategy.
        .leftjoin(STATE.chains.cut("chain_id", "group", "strategy"), "chain_id")
    )

    # Aggregate all put and call notional risk per (account, underlying).
    per_underlying = (
        notional.addfield("down", get_downside_notional)
        .addfield("up", get_upside_notional)
        .aggregate(
            ["account", "underlying", "instype", "group", "strategy"],
            {
                "price": ("price", first),
                "notional_down": ("down", lambda g: sum(g).quantize(Q)),
                "notional_up": ("up", lambda g: sum(g).quantize(Q)),
            },
        )
        .sort(["account", "underlying", "instype", "group"])
        .cut(
            "account",
            "underlying",
            "price",
            "instype",
            "group",
            "strategy",
            "notional_down",
            "notional_up",
        )
    )

    def aggregate(field: str):
        return per_underlying.aggregate(
            field,
            {
                "notional_down": ("notional_down", sum),
                "notional_up": ("notional_up", sum),
            },
        ).sort([field])

    # Aggregate all notional per account, group and strategy.
    per_account = aggregate("account")
    per_group = aggregate("group")
    per_strategy = aggregate("strategy")

    return flask.render_template(
        "leverage.html",
        per_underlying=ToHtmlString(per_underlying, "per_underlying"),
        per_account=ToHtmlString(per_account, "per_account"),
        per_group=ToHtmlString(per_group, "per_group"),
        per_strategy=ToHtmlString(per_strategy, "per_strategy"),
        **GetNavigation(),
    )


# Trigger the initialization on load (before even the first request).
Initialize()
