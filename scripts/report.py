"""Build, validate, and deploy the GitHub activity Power BI report."""

import argparse
import base64
import hashlib
import json
import re
import urllib.request

from fabric_api import Fabric
import project

SCHEMAS = "https://developer.microsoft.com/json-schemas/fabric/item/report/"
NAME = "GitHub Team Activity"
WIDTH, HEIGHT = 1280, 840
BLUE, GREEN, PURPLE = "#1F6FEB", "#238636", "#8250DF"
INK, MUTED, BACKGROUND = "#172B4D", "#526277", "#F5F7FB"


def schema(kind, version, properties=False):
    prefix = "" if properties else "definition/"
    return f"{SCHEMAS}{prefix}{kind}/{version}/schema.json"


def literal(value):
    if isinstance(value, bool):
        text = str(value).lower()
    elif isinstance(value, (int, float)):
        text = f"{value}D"
    else:
        text = "'" + value.replace("'", "''") + "'"
    return {"expr": {"Literal": {"Value": text}}}


def color(value):
    return {"solid": {"color": literal(value)}}


def field(table, name, measure=False):
    return {
        "field": {
            "Measure" if measure else "Column": {
                "Expression": {"SourceRef": {"Entity": table}}, "Property": name,
            }
        },
        "queryRef": f"{table}.{name}", "nativeQueryRef": name,
    }


def metric(table, name):
    return field(table, name, True)


REPOSITORY = field("Repository", "Repository")
MONTH = field("Date", "Year Month")
ACCOUNT = field("Author", "GitHub Login")
COMMITS = metric("Commits", "Total Commits")
AUTHORS = metric("Commits", "Commit Authors")
ISSUES = metric("Issues", "Total Issues")
PRS = metric("Pull Requests", "Total Pull Requests")
MERGED = metric("Pull Requests", "Merged Pull Requests")
OPEN_PRS = metric("Pull Requests", "Open Pull Requests")
RATE = metric("Pull Requests", "Merge Rate")
MERGE_HOURS = metric("Pull Requests", "Average Hours to Merge")


def identifier(text):
    return hashlib.sha256(text.encode()).hexdigest()[:20]


def filter_item(name, table, column, condition, filter_type="Advanced"):
    return {
        "name": name, "field": field(table, column)["field"], "type": filter_type,
        "howCreated": "User",
        "filter": {
            "Version": 2, "From": [{"Name": "s", "Entity": table, "Type": 0}],
            "Where": [{"Condition": condition}],
        },
    }


def source_column(name):
    return {"Column": {"Expression": {"SourceRef": {"Source": "s"}}, "Property": name}}


def exclude_values(name, table, column, values):
    return filter_item(name, table, column, {
        "Not": {"Expression": {"In": {
            "Expressions": [source_column(column)],
            "Values": [[{"Literal": {"Value": value}}] for value in values],
        }}},
    }, "Categorical")


def visual(page, title, kind, box, roles=None, sort=None):
    x, y, width, height = box
    result = {
        "$schema": schema("visualContainer", "2.5.0"),
        "name": identifier(page + ":" + title),
        "position": {
            "x": x, "y": y, "width": width, "height": height,
            "z": y * WIDTH + x, "tabOrder": y * WIDTH + x,
        },
        "visual": {
            "visualType": kind, "drillFilterOtherVisuals": True,
            "visualContainerObjects": {
                "title": [{"properties": {
                    "show": literal(True), "text": literal(title),
                    "fontSize": literal(12), "bold": literal(True),
                    "fontColor": color(INK), "titleWrap": literal(True),
                }}],
                "background": [{"properties": {
                    "show": literal(True), "color": color("#FFFFFF"),
                    "transparency": literal(0),
                }}],
                "border": [{"properties": {
                    "show": literal(True), "color": color("#E4E9F0"),
                    "radius": literal(8),
                }}],
            },
        },
    }
    if roles:
        query = {"queryState": {
            role: {"projections": projections} for role, projections in roles.items()
        }}
        if sort:
            projection, direction = sort
            query["sortDefinition"] = {"sort": [{
                "field": projection["field"], "direction": direction,
            }]}
        result["visual"]["query"] = query
    return result


def text(page, title, value, box, size=11, bold=False):
    result = visual(page, title, "textbox", box)
    result["visual"]["visualContainerObjects"] = {
        "background": [{"properties": {"show": literal(False)}}],
        "title": [{"properties": {"show": literal(False)}}],
    }
    result["visual"]["objects"] = {"general": [{"properties": {
        "paragraphs": [{"textRuns": [{"value": value, "textStyle": {
            "fontFamily": "Segoe UI", "fontSize": f"{size}pt",
            "fontWeight": "600" if bold else "normal",
            "color": INK if bold else MUTED,
        }}]}],
    }}]}
    return result


def card(page, title, projection, box, accent=BLUE, size=28):
    result = visual(page, title, "card", box, {"Values": [projection]})
    result["visual"]["objects"] = {
        "labels": [{"properties": {
            "color": color(accent), "fontSize": literal(size),
            "labelDisplayUnits": literal(0),
        }}],
        "categoryLabels": [{"properties": {"show": literal(False)}}],
    }
    return result


def chart(page, title, kind, box, category, values):
    result = visual(page, title, kind, box, {"Category": [category], "Y": values},
                    (category, "Ascending") if kind != "barChart"
                    else (values[0], "Descending"))
    result["visual"]["objects"] = {
        "categoryAxis": [{"properties": {"fontSize": literal(10)}}],
        "valueAxis": [{"properties": {
            "fontSize": literal(10), "labelDisplayUnits": literal(0),
        }}],
        "legend": [{"properties": {
            "show": literal(len(values) > 1), "position": literal("Top"),
            "fontSize": literal(10),
        }}],
    }
    return result


def table(page, title, box, projections):
    result = visual(page, title, "tableEx", box, {"Values": projections})
    result["visual"]["objects"] = {
        "columnHeaders": [{"properties": {"fontSize": literal(10)}}],
        "values": [{"properties": {"fontSize": literal(10)}}],
        "grid": [{"properties": {"rowPadding": literal(5)}}],
    }
    return result


def common(page, title, subtitle):
    result = [
        text(page, "heading", title, (24, 16, 1232, 44), 24, True),
        text(page, "subtitle", subtitle, (24, 62, 1232, 30), 11),
    ]
    for label, projection, x, width, mode in [
        ("UTC dates", field("Date", "Date"), 24, 284, "Between"),
        ("Repository", REPOSITORY, 320, 450, "Dropdown"),
        ("Language", field("Repository", "Language"), 782, 180, "Dropdown"),
        ("GitHub account", ACCOUNT, 974, 282, "Dropdown"),
    ]:
        slicer = visual(page, label, "slicer", (x, 104, width, 62),
                        {"Values": [projection]})
        slicer["visual"]["objects"] = {
            "data": [{"properties": {"mode": literal(mode)}}],
            "selection": [{"properties": {"singleSelect": literal(False)}}],
        }
        slicer["visual"]["syncGroup"] = {
            "groupName": label, "fieldChanges": True, "filterChanges": True,
        }
        result.append(slicer)
    result.append(card(
        page, "Last capture in selected dates (UTC)",
        metric("Repository Daily", "Last Captured At"),
        (24, 764, 400, 56), MUTED, 12,
    ))
    result.append(text(
        page, "caveats",
        "Public, owned, non-fork repos | Default-branch commits | UTC author/creation dates\n"
        "Current states, not historical backlog. Counts include automation; not productivity or DORA.",
        (440, 764, 816, 60), 9,
    ))
    return result


def cards(page, definitions):
    width = (1232 - 16 * (len(definitions) - 1)) / len(definitions)
    return [
        card(page, title, projection, (24 + i * (width + 16), 184, width, 84),
             [BLUE, GREEN, PURPLE][i % 3])
        for i, (title, projection) in enumerate(definitions)
    ]


def report_files(model_id):
    files = {}

    def put(path, content):
        files[path] = json.dumps(content, indent=2).encode() + b"\n"

    put("definition.pbir", {
        "$schema": schema("definitionProperties", "2.0.0", True), "version": "4.0",
        "datasetReference": {"byConnection": {"connectionString": f"semanticmodelid={model_id}"}},
    })
    put("definition/version.json", {
        "$schema": schema("versionMetadata", "1.0.0"), "version": "2.0.0",
    })
    theme_name = "GitHubTeamActivity"
    put(f"StaticResources/RegisteredResources/{theme_name}.json", {
        "name": theme_name, "dataColors": [BLUE, GREEN, PURPLE, "#099692", "#D97706"],
        "background": "#FFFFFF", "foreground": INK, "tableAccent": BLUE,
        "textClasses": {
            "title": {"fontFace": "Segoe UI Semibold", "fontSize": 12, "color": INK},
            "label": {"fontFace": "Segoe UI", "fontSize": 10, "color": MUTED},
        },
    })
    put("definition/report.json", {
        "$schema": schema("report", "3.1.0"),
        "themeCollection": {
            "baseTheme": {
                "name": "CY25SU12", "type": "SharedResources",
                "reportVersionAtImport": {"visual": "2.5.0", "report": "3.1.0", "page": "2.0.0"},
            },
            "customTheme": {
                "name": theme_name + ".json", "type": "RegisteredResources",
                "reportVersionAtImport": {"visual": "2.5.0", "report": "3.1.0", "page": "2.0.0"},
            },
        },
        "resourcePackages": [{
            "name": "SharedResources", "type": "SharedResources",
            "items": [{"name": "CY25SU12", "path": "BaseThemes/CY25SU12.json", "type": "BaseTheme"}],
        }, {
            "name": "RegisteredResources", "type": "RegisteredResources",
            "items": [{"name": theme_name + ".json", "path": theme_name + ".json", "type": "CustomTheme"}],
        }],
        "filterConfig": {"filters": [
            exclude_values("known-repositories", "Repository", "Repository", ["null"]),
            filter_item("exclude-future-calendar", "Date", "Date", {"Comparison": {
                "ComparisonKind": 4, "Left": source_column("Date"), "Right": {"Now": {}},
            }}),
        ]},
        "settings": {
            "useEnhancedTooltips": True, "allowChangeFilterTypes": True,
            "defaultFilterActionIsDataFilter": True,
        },
    })
    pages = {
        "overview": ("01 | Repository overview", common(
            "overview", "GitHub | Repository activity",
            "Explore adoption and activity across the tracked portfolio. Use the date range to zoom into recent work.",
        )),
        "workflow": ("02 | Pull request flow", common(
            "workflow", "GitHub | Pull request flow",
            "Creation-date cohorts: merged/open metrics describe the current outcomes of PRs created in the selected dates.",
        )),
        "contributions": ("03 | Contribution patterns", common(
            "contributions", "GitHub | Contribution patterns",
            "Observed GitHub accounts, including automation. Accounts are not unique people; no team membership is collected.",
        )),
    }
    overview = pages["overview"][1]
    overview += cards("overview", [
        ("Default-branch commits", COMMITS), ("Linked commit accounts", AUTHORS),
        ("Pull requests created", PRS), ("Currently open PRs", OPEN_PRS),
        ("PR cohort merge rate", RATE), ("Mean hours to merge", MERGE_HOURS),
    ])
    overview += [
        chart("overview", "Activity by creation month", "lineChart", (24, 286, 600, 222),
              MONTH, [COMMITS, PRS]),
        chart("overview", "Commits by repository", "barChart", (640, 286, 616, 222),
              REPOSITORY, [COMMITS]),
        table("overview", "Repository comparison | select a row to filter",
              (24, 526, 1232, 220), [
                  REPOSITORY, field("Repository", "Language"), COMMITS, AUTHORS, PRS,
                  metric("Issues", "Open Issues"), RATE, MERGE_HOURS,
              ]),
    ]
    workflow = pages["workflow"][1]
    workflow += cards("workflow", [
        ("Pull requests created", PRS), ("Currently open PRs", OPEN_PRS),
        ("Merged PRs in cohort", MERGED), ("PR cohort merge rate", RATE),
        ("Mean hours to merge", MERGE_HOURS),
        ("Mean issue close hours", metric("Issues", "Average Hours to Close")),
    ])
    workflow += [
        chart("workflow", "PR creation cohorts | current merged outcomes", "lineChart",
              (24, 286, 600, 222), MONTH, [PRS, MERGED]),
        chart("workflow", "Issue creation cohorts | current state", "clusteredColumnChart",
              (640, 286, 616, 222), MONTH,
              [metric("Issues", "Open Issues"), metric("Issues", "Closed Issues")]),
    ]
    open_prs = table(
        "workflow", "Currently open PRs created within the selected dates",
        (24, 526, 1232, 220), [
            REPOSITORY, field("Pull Requests", "Pull Request Number"),
            field("Pull Requests", "Title"), ACCOUNT, field("Pull Requests", "Date"),
            field("Pull Requests", "Is Draft"),
        ],
    )
    open_prs["filterConfig"] = {"filters": [filter_item(
        "open-prs-only", "Pull Requests", "State", {"In": {
            "Expressions": [source_column("State")],
            "Values": [[{"Literal": {"Value": "'open'"}}]],
        }}, "Categorical",
    )]}
    workflow.append(open_prs)
    contributions = pages["contributions"][1]
    contributions += cards("contributions", [
        ("Default-branch commits", COMMITS), ("Linked commit accounts", AUTHORS),
        ("Issues created", ISSUES), ("Pull requests created", PRS),
    ])
    contributions += [
        chart("contributions", "Commits by GitHub account | includes automation", "barChart",
              (24, 286, 600, 222), ACCOUNT, [COMMITS]),
        chart("contributions", "Commit author dates by UTC weekday", "clusteredColumnChart",
              (640, 286, 616, 222), field("Date", "Day of Week"), [COMMITS]),
        table("contributions", "Account activity | not a productivity ranking",
              (24, 526, 1232, 220), [ACCOUNT, COMMITS, ISSUES, PRS, MERGED]),
    ]
    for name, (display_name, visuals) in pages.items():
        page = {
            "$schema": schema("page", "2.0.0"), "name": name, "displayName": display_name,
            "displayOption": "FitToPage", "width": WIDTH, "height": HEIGHT,
            "objects": {"background": [{"properties": {
                "color": color(BACKGROUND), "transparency": literal(0),
            }}]},
        }
        if name == "contributions":
            page["filterConfig"] = {"filters": [exclude_values(
                "identified-accounts", "Author", "GitHub Login", ["null", "'unlinked'"],
            )]}
        put(f"definition/pages/{name}/page.json", page)
        for item in visuals:
            put(f"definition/pages/{name}/visuals/{item['name']}/visual.json", item)
    put("definition/pages/pages.json", {
        "$schema": schema("pagesMetadata", "1.0.0"),
        "pageOrder": list(pages),
    })
    return files


def validate(files):
    from jsonschema import Draft7Validator
    from referencing import Registry, Resource

    version = json.loads(files["definition/version.json"])["version"]
    if version != "2.0.0":
        raise ValueError(
            "Unsupported PBIR content version; use 2.0.0. "
            "The definition.pbir entry-point version 4.0 is a separate format."
        )
    resources = {}

    def retrieve(uri):
        if not uri.startswith(SCHEMAS):
            raise ValueError(f"Unexpected schema reference: {uri}")
        if uri not in resources:
            with urllib.request.urlopen(uri, timeout=60) as response:
                resources[uri] = Resource.from_contents(json.load(response))
        return resources[uri]

    registry = Registry(retrieve=retrieve)
    for path, content in files.items():
        document = json.loads(content)
        if "$schema" not in document:
            continue
        resource = retrieve(document["$schema"])
        registry = registry.with_resource(document["$schema"], resource)
        Draft7Validator(resource.contents, registry=registry).validate(document)
    print(f"Validated {len(files)} PBIR definition parts against Microsoft schemas", flush=True)


def build(state):
    files = report_files(state["semantic_model_id"])
    validate(files)
    destination = project.ROOT / ".local" / "report"
    for path, content in files.items():
        target = destination.joinpath(*path.split("/"))
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    return files


def deploy(args):
    state = project.load()
    files = build(state)
    api = Fabric("powerbi-report")
    workspace = f"/workspaces/{state['workspace_id']}"
    existing = project.exact(api.list(workspace + "/reports"), NAME)
    if existing and not args.replace:
        raise RuntimeError("Report already exists; --replace explicitly replaces its definition")
    definition = {"parts": [{
        "path": path, "payloadType": "InlineBase64",
        "payload": base64.b64encode(content).decode(),
    } for path, content in files.items()]}
    if existing:
        report = existing
        api.call("POST", workspace + f"/reports/{report['id']}/updateDefinition",
                 {"definition": definition})
    else:
        report = api.call("POST", workspace + "/reports", {
            "displayName": NAME,
            "description": "Repository activity, contribution patterns and PR creation-cohort outcomes.",
            "definition": definition,
        })
    state["report_id"] = report["id"]
    project.save(state)
    verify_report(state, files)


def verify_parts(files, returned_parts, model_id):
    if set(files) - set(returned_parts):
        raise RuntimeError("Published report is missing definition parts")
    for path, expected in files.items():
        actual = json.loads(base64.b64decode(returned_parts[path]["payload"]))
        expected = json.loads(expected)
        if path == "definition.pbir":
            # Service exports expand the short model reference into an XMLA connection string.
            connection = actual["datasetReference"]["byConnection"]["connectionString"]
            match = re.search(r"(?:^|;)\s*semanticmodelid\s*=\s*([^;]+)", connection, re.I)
            if not match or match[1].strip().lower() != model_id.lower():
                raise RuntimeError("Published definition references the wrong semantic model")
            if actual["version"] != expected["version"]:
                raise RuntimeError("Published report changed its definition version")
            continue
        if path == "definition/report.json":
            # Theme package exports may omit the .json extension while retaining the same resource.
            for document in (actual, expected):
                for package in document.get("resourcePackages", []):
                    for item in package.get("items", []):
                        if item["type"] == "CustomTheme":
                            item["path"] = item["path"].removesuffix(".json")
        if actual != expected:
            raise RuntimeError(f"Published definition differs from source: {path}")


def verify_report(state, files):
    api = Fabric("powerbi-report")
    workspace = f"/workspaces/{state['workspace_id']}"
    report_id = state["report_id"]
    returned = api.call("POST", workspace + f"/reports/{report_id}/getDefinition", {})
    returned_parts = {part["path"]: part for part in returned["definition"]["parts"]}
    verify_parts(files, returned_parts, state["semantic_model_id"])
    url = (f"https://api.powerbi.com/v1.0/myorg/groups/{state['workspace_id']}"
           f"/reports/{report_id}")
    metadata = api.request(
        "GET", url, resource="https://analysis.windows.net/powerbi/api",
    )[2]
    if metadata["datasetId"] != state["semantic_model_id"]:
        raise RuntimeError("Published report is bound to the wrong semantic model")
    binding = base64.b64decode(returned_parts["definition.pbir"]["payload"])
    desktop_path = project.ROOT / ".local" / "report" / "definition-liveConnect.pbir"
    desktop_path.parent.mkdir(parents=True, exist_ok=True)
    desktop_path.write_bytes(binding)
    state["report_url"] = metadata["webUrl"]
    project.save(state)
    print(json.dumps(metadata, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["build", "deploy", "verify"])
    parser.add_argument("--replace", action="store_true")
    args = parser.parse_args()
    if args.action == "build":
        build(project.load())
    elif args.action == "deploy":
        deploy(args)
    else:
        state = project.load()
        verify_report(state, build(state))
