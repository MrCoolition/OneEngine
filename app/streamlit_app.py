from __future__ import annotations

"""
ONE ENGINE — Snowflake-native compliance rules platform
========================================================

A single-file Streamlit application that ports the supplied Compliance Rules
codebase to Snowpark/Snowflake. It includes:

* Snowflake persistence against a separately provisioned backend schema
* Bundled DAF rule catalog seeding
* CSV/XLSX workbook ingestion without a separate service
* Ordered predicate/action rule execution with stop-processing and traces
* Dry runs, full runs, and selected-row runs
* Analyst workbench and auditable overrides
* Rule catalog creation, editing, enable/disable, deletion, and simulation
* Compliance bucket reporting and CSV/XLSX exports
* Live-build source proof and downloadable root-cause diagnostics

Deploy this file as the main file of a Streamlit in Snowflake application.
Provision the backend first with ``compliance_rules_backend.sql``. The app never
creates tables at runtime. Its owner role needs USAGE on the warehouse/database/
schema and SELECT, INSERT, UPDATE, and DELETE on the seven target tables.
"""

import base64
import csv
import gzip
import hashlib
import io
import json
import math
import platform
import re
import sys
import traceback
import uuid
import zipfile
from contextlib import contextmanager
from importlib import metadata as importlib_metadata
from time import perf_counter
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Iterator, Mapping, MutableMapping, Sequence
from xml.etree import ElementTree as ET
from xml.sax.saxutils import escape as xml_escape

try:
    import pandas as pd
except Exception as exc:  # pragma: no cover - pandas is available in Streamlit runtimes
    pd = None  # type: ignore[assignment]
    PANDAS_IMPORT_ERROR = exc
else:
    PANDAS_IMPORT_ERROR = None

try:
    import streamlit as st
except Exception:  # Enables import-based engine tests outside Streamlit.
    st = None  # type: ignore[assignment]

try:
    from snowflake.snowpark.context import get_active_session
except Exception:  # Enables import-based engine tests outside Snowflake.
    get_active_session = None  # type: ignore[assignment]


APP_TITLE = "ONE ENGINE"
APP_VERSION = "2026.07.26-one-engine-snowpark-v1"
SESSION_STATE_SCHEMA_VERSION = 7
WORKBOOK_PARSER_VERSION = "2026.07.24-v7-uncached"
MAX_DIAGNOSTIC_EVENTS = 50
DEPLOYMENT_SENTINEL = "ONE_ENGINE_SNOWFLAKE_LIVE_20260726"
LIVE_BUILD_BADGE = "ONE ENGINE · SNOWFLAKE · LIVE"
TARGET_ROLE = "FOODBUY_AXIOM_COMPLIANCE_PROD"
TARGET_WAREHOUSE = "COMPLIANCE_PROD_WH"
TARGET_DATABASE = "FOODBUY_MASALA_PROD"
TARGET_SCHEMA = "COMPLIANCE_LAB"
TABLE_PREFIX = "COMPLIANCE_RULES"
COMPILER_VERSION = "2026-06-01.daf-logic-v2-snowpark"
USER_RULE_PRIORITY_FLOOR = 900_000
MAX_JSON_BATCH_ROWS = 250

TABLE_SUFFIXES = {
    "batches": "BATCHES",
    "rows": "WORKFLOW_ROWS",
    "rules": "RULES",
    "runs": "RUNS",
    "results": "ROW_RESULTS",
    "audit": "AUDIT_EVENTS",
    "references": "REFERENCE_LISTS",
}

HEADER_ALIASES = {
    "case #": "Case#",
    "case": "Case#",
    "case#": "Case#",
    "subcategory": "Sub Category",
    "sub category": "Sub Category",
    "buy smart action": "Buysmart Action",
    "buysmartaction": "Buysmart Action",
    "buysmart action": "Buysmart Action",
    "if in-stock action": "If In Stock: Action",
    "if in stock action": "If In Stock: Action",
    "if in stock: action": "If In Stock: Action",
    "dstdin": "DSTDIN",
}

EXPECTED_HEADERS = [
    "Business",
    "Type",
    "Case#",
    "Date Created",
    "Sector",
    "Division",
    "Unit Name",
    "Unit Number",
    "Vendor",
    "DIN",
    "MIN",
    "Manufacturer",
    "Brand",
    "Description",
    "Parent Category",
    "Sub Category",
    "Usage",
    "One-Time or Permanent",
    "Reason for request",
    "DPL",
    "Meets Criteria",
    "In CAT",
    "On MOG",
    "Pantry",
    "K12 APL",
    "Compass APL",
    "Conversion DIN",
    "Conversion VA%",
    "ACTION",
    "If In Stock: Action",
    "Buysmart Action",
]
EXPECTED_HEADER_LOOKUP = {header.lower(): header for header in EXPECTED_HEADERS}

FIELD_LABELS = {
    "business_key": "Business",
    "request_type_key": "Request type",
    "vendor_lc": "Vendor",
    "din_lc": "DIN",
    "min_lc": "MIN",
    "manufacturer_lc": "Manufacturer",
    "brand_lc": "Brand",
    "description_lc": "Description",
    "parent_category_lc": "Parent category",
    "subcategory_lc": "Sub category",
    "division_lc": "Division",
    "sector_lc": "Sector",
    "reason_lc": "Reason for request",
    "usage_num": "Usage",
    "meets_criteria_num": "Meets criteria",
    "current_action_key": "Current ACTION",
    "current_buysmart_key": "Current BuySmart action",
    "is_compass": "Compass USA",
    "is_canada": "Compass Canada",
    "is_healthtrust": "HealthTrust",
    "is_hmshost": "HMSHost",
    "is_foodbuyone": "FoodbuyOne",
    "is_mass_add": "Mass Add",
    "is_mass_srf": "Mass Add SRF",
    "is_prf": "PRF",
    "is_sorf": "SORF",
    "is_srf": "SRF",
    "is_one_time": "One-time request",
    "is_permanent": "Permanent request",
    "is_pantry": "Pantry/APL",
    "is_in_catalog": "In catalog",
    "is_in_cat_y": "In CAT = Y",
    "is_temp_available": "Temporarily available",
    "is_k12_apl": "K12 APL",
    "is_core_apl": "Core APL",
    "is_s1": "S1",
    "is_foh": "Front of House",
    "is_diverse": "Diverse",
    "has_conversion": "Has conversion DIN",
    "is_levy": "Levy",
    "is_schools": "Schools/Chartwells",
}

OPERATOR_LABELS = {
    "eq": "equals",
    "ne": "does not equal",
    "contains": "contains",
    "not_contains": "does not contain",
    "blank": "is blank",
    "not_blank": "is not blank",
    "is_true": "is true",
    "is_false": "is false",
    "gt": ">",
    "ge": ">=",
    "lt": "<",
    "le": "<=",
    "in": "is in",
    "not_in": "is not in",
    "in_ref": "is in reference list",
    "not_in_ref": "is not in reference list",
    "regex": "matches regex",
    "not_regex": "does not match regex",
}
SUPPORTED_OPERATORS = set(OPERATOR_LABELS)
NO_VALUE_OPERATORS = {"blank", "not_blank", "is_true", "is_false"}
NUMERIC_OPERATORS = {"gt", "ge", "lt", "le"}
LIST_OPERATORS = {"in", "not_in"}

ACTION_LABELS = {
    "set_action": "Set ACTION",
    "set_action_by_duration": "Set ACTION by duration",
    "set_if_stock": "Set If In Stock",
    "set_buysmart": "Set BuySmart",
    "set_review": "Flag for review",
    "append_validation": "Add validation",
    "add_note": "Add analyst note",
    "exclude": "Exclude row",
    "clear_field": "Clear field",
    "preserve_action_set_if_stock": "Preserve upstream action / set stock action",
}
USER_ACTION_TYPES = [
    "set_action",
    "set_if_stock",
    "set_buysmart",
    "set_review",
    "append_validation",
    "add_note",
    "exclude",
]

ACTION_OPTIONS = ["OK", "1X", "Use Right", "Find Alt First", "Cannot Add", "Invalid Information", "Review"]
IF_STOCK_OPTIONS = ["OK", "Review"]
BUYSMART_OPTIONS = ["Approved", "Denied", "Assigned", "Review"]

RUNTIME_KIND_ORDER = {
    "validation_rule": 0,
    "row_rule": 1,
    "buysmart_rule": 2,
    "downstream_rule": 3,
}

BUCKETS = [
    {
        "id": "auto-approved",
        "label": "Auto Approved",
        "description": "Rows the engine can approve cleanly from DAF logic.",
        "tone": "good",
    },
    {
        "id": "approved-1x",
        "label": "Approved 1X",
        "description": "One-time approved requests and PRF 1X closeout rows.",
        "tone": "good",
    },
    {
        "id": "vendor-exclusions",
        "label": "Vendor Exclusions",
        "description": "Rows removed from the managed workflow by vendor/pre-processing rules.",
        "tone": "dark",
    },
    {
        "id": "data-issues",
        "label": "Data Issues",
        "description": "Rows missing required identifiers or carrying invalid source data.",
        "tone": "bad",
    },
    {
        "id": "denied",
        "label": "Denied / Cannot Add",
        "description": "Rows the rules classify as denied, cannot add, or not in stock.",
        "tone": "bad",
    },
    {
        "id": "use-right",
        "label": "Use Right / Conversion",
        "description": "Rows routed to conversion/use-right handling.",
        "tone": "info",
    },
    {
        "id": "find-alt",
        "label": "Find Alt First",
        "description": "Rows that need an alternate item before approval or denial.",
        "tone": "warn",
    },
    {
        "id": "cdm-review",
        "label": "CDM Review",
        "description": "Rows that need category manager review.",
        "tone": "warn",
    },
    {
        "id": "compliance-review",
        "label": "Compliance Review",
        "description": "Rows the engine flagged for analyst or compliance review.",
        "tone": "warn",
    },
    {
        "id": "assigned-processing",
        "label": "Assigned for Processing",
        "description": "Rows assigned to a specialist or downstream operational workflow.",
        "tone": "info",
    },
]
BUCKET_BY_ID = {bucket["id"]: bucket for bucket in BUCKETS}
BUCKET_ID_BY_LABEL = {bucket["label"]: bucket["id"] for bucket in BUCKETS}

DEFAULT_REFERENCE_LISTS = {
    "local_vendors": [
        "Baldor",
        "Network",
        "UNFI",
        "Vesta",
        "Vistar Vending",
        "The Chefs Warehouse",
        "Gourmet",
    ]
}

EXPORT_HEADERS = [
    "Business",
    "Type",
    "Case#",
    "Vendor",
    "DIN",
    "MIN",
    "Description",
    "ACTION",
    "If In Stock: Action",
    "Buysmart Action",
    "Assigned Bucket",
    "Rule Applied",
    "Applied Rule Count",
    "Applied Rule Priorities",
    "Applied Rule Actions",
    "Applied Rule Details",
    "Needs Review",
    "Validation Status",
    "Excluded",
    "Excluded Reason",
    "Compliance Bucket",
    "Outcome Reporting",
    "Analyst Notes",
]

APPLIED_RULE_HEADERS = [
    "Source Row",
    "Case#",
    "Business",
    "Type",
    "Vendor",
    "DIN",
    "MIN",
    "Description",
    "Trace Order",
    "Rule Priority",
    "Rule ID",
    "Runtime Rule ID",
    "Runtime Kind",
    "Rule Description",
    "Associated Rule Action",
    "Matched At",
    "Final ACTION",
    "Final If In Stock: Action",
    "Final Buysmart Action",
    "Compliance Bucket",
    "Outcome Reporting",
    "Needs Review",
    "Excluded",
    "Excluded Reason",
]


# The source repository's bundled daf-seed.json, gzip-compressed and base64 encoded.
DAF_SEED_GZIP_BASE64 = """
H4sIAHV0Y2oC/+1d63LbyJX+P0/R5dRUZiaEKYA30RzNFkVZlmJLVImSPLuZ1BYINElEIMDgIplJ5cnyI4+0r7DndONKNtCgKJHylKdqZBJsAH35zv306f/7
93/++R0hbyaWTS/1OX3zjrw56Z8ShXxyp5ZBLvTAs77A13OHXHnu1KO+//aL7X95U8Pb/BmlAd7nw41/gStwLXsjawXXrunfQ+oH5Ga5gKbRxRPdspekbwSW
65BTi9pm8lN08U63w7T9+wf4qrMfhp5Jvfj6yA09A5rBt7+yXtnYg2v3Me3UP9lf+MkLbXpu4iivDw7U6AnR9Q+eGy7wpyuPKgvPhWf6ljNNG41D+A4XsU3f
tjN38+Hx0a3+aFLD8qHTA88KqGfp2OCOOqbrEcshx7oNn2rkkgaPrndfI7eXp+c1cgeP0+EfC/7xCDaHntTIzYySwYxOfPJZ9+jMDX1aIx9g/HMapG/U2ezh
e9Jr1uTcGQWucd8X/DgOl/5c9wLRbyauEv/hFEAycO1w7vjpINKWPs084JrO3QdKJp47Jzgw6D4x3MWS1InpEscNSDTDZK47+pSarNXEdh8zr3YfHT/wqD4/
0x3TxrXId83yDXiJt7ymE+pRx+DwZbAaITJjcF1Fr+ooaqNHRgvbCgLsUHxdbStaBgzQO76Ol5SaSQeVB75oNqwJDMWZWN6coTEzAwyJgLzLcD4GgL4jDfbT
v2olKNT2hsKL80ti+WRs6849GV6Tk8z3naMJOtPDHhTg6dwB6rdM4EMTd23exUAZLWDMOi5XHZYw1G3i0QeLPm4BoG5DgJITy6NGQDz3UbHpA7UJ66kUGk05
NBoF0DiOIEDGnu4YMzE2Tl3XhKUYOnQLiFgTctcnvxypB9/XyEitkRML5gu5DlDCqec6AXEn5AwZUY9AYyRsoI3AA9QATcNl4OyOSfp2QFQ/6BH1V6IvANYP
8CusFaFzmG1yPnen1Nk55uJ57JELWGyfxCPvkYE7X+gwwf2rTz1yBbzWCeqjcEwGekCnrrfsEZhX5caaU5yIKwqQdKBNAXhj+NjWPY0f/Zb0+TyQPc2xmGgu
3cCaLKPGBGgNn4avus8xoY0pR20qaldAPPESKD5S6wQ0Bz+cw0ouCSK+jH5acvppbkM/ZxejM9cPtiCe5DVHZO1hekZS5pnSi+K8P7g5Hxbx2LVOiiFy7YYB
Jbq/PqiNUaGpiiZiqaeuDZoACRcEGgczEEq4fjWygAZOAEydBC4JHVACQT8ChKcdjdWKt2XQacuh0yqAzgCUAVMH9RaZg2mKoRNzD964GEHxU/xyHJ2fkgyU
8g8n/csTklWvocV651K0DT+WIu04L/vX4dYHbWTqUHMT2NVyHSwAX8wNXQfAMnA9iryXMVcdWN3yLTnnnI9/reOPwQxY1OVQhtePlC5A1Z5RnFKT+Es/oHPC
p4QE9EtAHuE3Csihfo6Hbw7nltLQBHBmC6KbJiggwIodWJxoFsmCesSwXZ8CTUk5XkcO23Y5bK+uT7dDbO4BEp4nByo8jV0eqfDl5vr2PWqhfIGjC68TwrWs
flCLelyAazDXjHsU2yinPWs6C1DHZnpFJF1DH8wL8rN6AFJ9PgcEgsWxjCU4iVYAsAJNVx+DBAGPklHABVd/oWUdblf47SItdXPEdxTtsEaamtJs1Ih2eFho
RQGdUc/hvaioIR9+w3s0FrbOplkH7QwsIIUhvpQIsjTyjQK+WgroyimgU04B6q9bEwAYJUP2dztaEBpM0DS5jo1uORKOCGABv0fLWx+p9VWMVRYIz08LBdZf
j3d/zXosoY3s8oiRG+tFoG+78NIAXroFWhsNpSnydL1nJiJSnw+qtYnUBxqRz0kRScdeZrRveLruEHexcL0gdKxgieq4HgaugkYj/MQtTgt1d0omYRB69C2o
pcsxjIP8BiDDO0H3mi9sMKh/e1NGA+qBnAgOZURAqG/o9gq9fTX08MvuyGHVGt0lMbzna0QRTcc0AFhd6MaJCyyT+zH+7C7x+dTydCnV+Peg1VvGjBi6T334
6xAEX+QW2YaCmkqjJaAgPnFMWFkeyKrxEslpYVs6PBOgDqYpSjsu6n79RT0oxbwqx3y3APNnVLeD2Y0XgqgeFek/mUbFWN8E4Nm3IlIjjWcUaTwWmlwgrtG9
dQVoQPqvkfcw8VPXNWvkgpoBPBiez7xeGffWoxXMEv8Yg4HjwlIG0AnUo+CtKFQD64Hu0X2Io11BOmocE+gIsD5P5F900MeHwCr2eQ8/4lxw9Qmxw4dbQx0o
YBar5Tt/DFAsAG+2KQA9Bnh11ziZ4WWgEJAv+fXelDBaqtJqCwjjJ7EzhyhZBw4MaaZ7Jv/lwX+LCDAZHkBfwmmeEhfHDDJlEcJMXnMyyyhVGSAQI5myDJAA
Pn8qpTlNSnNZon39NMeDbLh6wI9QZCAB/Sk7JT8AXR0BTS3J9z8KLQ82j4/1wcnFkwnpafqViKAYCRnxVIhJRu4NGoxQvMAkOBi0YdwlN7zNcS+SBqVAkwcF
VbUC0EAVqaMmgoOJmMkWwLvaFnig7cX2bZ67xWZaYVzxFhjXNVpkewJZBVYs6KIYXtgwXQ8m97bBVkdpHwjglXSHKArRx5Yd6eBGbGr/9sZyFB8n8Lc35NEN
QYNiujemUFB0RZYCVB6aVJs7Byg+aWuEsu5IIMp+53oaRsMpCi90TaAi+TLwfYrSLQJvpHOTHwx2B/n1x68DzqmKUGOeH4di5NL8G6we8wYBspli8MKwl0cU
VW0T2HNlTLf3ypX9jNexHkWW66fDs4zrET7l9VOubD/BtnwuHi2wGdd16E0i8KBKT8TNNlGQr7ZSkNua0mluqiu0Xx8mn4ER+ykj3jcq1z0C31ApRWXn947K
8xG5HN6UgvPEhenHgAwiIAXpz0Ug3TLPY6/sc01B2cSrMNw9PuWRQ7WxCT5XEwefIsdrRA7RFYb3M1rssDypWpq36ReFPuAX9XtFjt0T6oM1vsAGPXLswYrH
qXKZNLls0lxhDIRxQd0w6CLQxzZNAh09IEJSz2W3wdfIKxFZ7uzeGSiFimf596Cls3dZgL3oOlflKwYIo9uX26eKdlpKV4TbP4fmFJVaZUb1h+VaBpoAzN2n
W2MxIxhVMMLitrej/vO4pzIPFLmnvmInQQ9TNNnQe+SjquUD0GJDq0boF8Q3a5+xqNDcARPFpDYNonTNNN7NvOaCmIU4zQkePDJmrmv7SfdY3CPwwvlCuPyb
Z3EetBQ1G3JOYI1v5yPE9j/9BIKRviNXuXw9mIuLz8TUA53MXRgxI13oPbAyi0caA2NWI364WNgUaQTdvhhBZLcwHpE6ef0aiW29Grnrf19GQpo8hKi25CT0
knRzfipQNrJLCrMaQQ1/GT3i6owABCAw6nEcpR6HVuppPAVbZ1y+SSTlyflVL6KkREPLElZ5AOXcIYP+zeaBE8fFj3A1BEpTgO6cpYywzifoOQLJoNvulIzD
gDGqoUMuhh/eYUcYCOFhehqq304nVw8OFVUV+ZUHUS9wkiL/hz6ZUAPmxqcYM8HM6WgpywhC/UYQ6wRx2v80ev8a4iDfSGOPpFEhDFiUdTiius8ilhHat6CP
igbDCMbnevVE6wJN2UId1iefKGiXPAcRO8UWO3oJgz/LQx4tXMd3PX9mLch5zre7c3HAR9ITdDeP91Jcgykx1n2ganhCbETUF1XdK3GqlR8vY6LQbANYFQEr
2vpy4hrE15c+maOtCUogLlkpNBtfEzTjdFGe9oqsJnaulCHygjrhYGnY9BsUXzUUK4QICzNTE0ukbqx5Bp6OxgoJeVlnBK5RxoORInDguvcWJSduOJ29fkM1
vykx55JZtVljWN/m/SFiexUVgpyZCqYZ5tHxyZmz3f29VVcMKhHUYnk6qRHr8DSclSf11tO769wIzsUbJyi7oddsyvndWI+A7flKx+oT0wJR70l3riGh8PQP
NgyTrfE8W6rgSXSlqYraUGtE7TYUtStUTvgLk3IKqWGM3IUlRTEjN8DcNjKxQ8tkiXDhAi5zJqKDnmVib8gP5+9rMAVxWu7fQ0AP9fDj2FLS6zQw3v4YmeLs
NefzhfsIE6BEWfSezyOtaJuz9/CO8PmoRbuiTHRzWwGxQRP/r1KWUCF8evi6WcIRANoy7qlDjgE6wAlvnTF8MGGOrvVHhro4kbFJ3H/gArXg3507YEuI/ko3
7lNToYTKcfP//+J/AIcfLk6vf2QimjoMDvmMqMLMgOPRMTkZ+JjrOkKnjQXg2pqa2m2l3QFaajSBqFpIVG1FOxBtq3sf5/5FXCHOvS1FaYWAave1o/TK9e7J
sW64zp6hF3n9y+1RLnc2gWIt5/KvkcshY4ZRuDTZKZ7oRH4VsF5TvrkIqZnNHRl9vGUbME8GiUAbzUF8TbAWTRpmgWH2mSN1poO4Gth0Ps+/cmN50WgDtBHj
hweKetioAG1kPZEkFGiCApjLI7TawauDeeK2Plr3ZKMmtuIZt7JOoBoP9tz1a5H/gddSgAlLFpJtZVlX3ndEOWshA1FIFjtekkheQEQna4oT4mV9/5zuk9g4
katKualmMQT/mYIIDTBDGl1xECGtA4HP9CyzdEe0Jg/0auqrg3lFj0wNGKDtGnqyKwI6MndNK9h9xFdkCVfKm+2vY5BFuFhxqKIniBH5PokurWeLPKEciSg2
GzMdI57oup6sAFsiLBaArctVDHnAVtNeHSg32Z5J1NZTLOMXhahsE9rTwOqzYjpPheww8vsQltzK5u5Z/DhNkYl5M/OoP3NBc2H7akjWSS4oTCYPiWoNSQQo
F/Z+KTdi5mXxBrLT4VlSJCkxPCIOyeuG7Bx9OYldAMXS2jtREkx8a4+s5gb2eH0SD0DI1YR6WheAIXQRBniT5WCllHPQbvNFAsr9jAnkt4JlV1Fbh0Lvh0cT
rl0h5aUhD09qRSkva/kYLwVMmbIaByePyH+/Bl1zk3Q/KRilsAoDF4v0GemqT1jYEUbySG2YqrUpw7o/24CvrSpqWxMKdXcBHNias+o/6NxY60UpFuXxQK0o
VL6627ly6caM4K5YIyrKSwUOiXE85ssJx+sqxI7wF+ec7hZ0ES9bzYrcGEvNVo20D5VOAyz0dgNgJdIVowlP8I0u+6xKKkCSPHynFYXv7vokiOW7lLXVsvnN
IC4rFILcRFNcy8onBRnPL1svdIt0ZinUaqBD2QiFZHsxz5OIdhqtOxrFiCzjV50aaTVrpKMqHfhHVTUEWwvA1pUh98yazjZMiG7Iw3VaUbgusv0EiuvLRY4r
2seJQcyUv9RafhX2scQH/xxR4sxk1IU2a8KdnsXsAKyqwlz8gejdzE7mOUYrsr4UqfIoklZc7SXykO8Qq8IQ8pluw5wPr8nUDgMso+pRWvfDqe7xjw90qjv1
e9efUa8+nWBLJ4zS03S7vgihEZu5C9fzLH8PgM7FlPI+/ziMnOqYG255YrDvidL4DbCQFTZDjusoqd/24Hty15cyW4sGmOLFQprPg3dgxx3g09qBqmgH+KHV
UDThfv8bEBSKT3XPmBHTA9O0HOTyIJTWfUUgz4WfEpSf4iTOANGWeF/0fuNQxfizJuTCGMAQerymyKPlU9xogtWWMezEf8TokwxyVwlC0aY1Vwp4PgFwHQBc
F3DWbitaW2RJf9LnY/IZlhBum1KHeqwYF7oBkorP0T4D3IYVee/L7Rt5pKhx8OqxeAF6MYz21BrvIbKTx2IFEALYIv3yLJwF+ly/twjwrilAIlvGRmeerUrB
zfQ5nGsytG2BxMOmoh6KqvlcJY+vY4JwFWeOPEbTUF89wD7YMIH7ZnOrNg9je1KMRXFR5iRE3ExD3TP96uGWrbfCqYfA14SFMc+irlWKaTfkcZWG9oqAtLIX
MsXS++nU34NLZmVnZk7Fi8IkqwCrwMtir0uiuxHQam27bltgI4M0grH2MvGPzNkEWfErUAaZ0pdXBKW1nZI+bCuJD7uK2gWTXOuC6tfVxNsN2EQmAevSsz7k
0ZZG47XywCMygq7gSRT/oF9BwpGUH+IBSjAakFsIC7S8fRwfZiGh0gSWiT5fjJl/mI+Zj19tISBZjmAWq1IbPfvs7XmppjYVTW0jMsEWEeaaJrlD0RvlSXFN
ebyl0fwa7BEfzBEL/rBCDWuHtqB3HExvyjaZ1ydeaAVoj/uz/TNjkYldAOTsMD/y4xJYbGXwKW/PvCV38Vj5QTI43CMEkhkaFCOIcvMmaUo8PPRjK9x2ALdo
2wBr1bqiKlCjDfLbmvKoTKMoKpNlHrtEbfa9dcbIMFib2G1Irbv3oucYacRdVz08Mg9mVs9kUr7OZDfzxPhVeGQ/OwfvyAf4PSCf9Hvc5D3SPZ18ApHOeXGN
3F5dD4c32eKxUSrAMldaNvTcBdUdlvTuBfCgz9DWhpf6LCACF45DLCI7BgsaPrvjP2JVUbD+Lfz+P9RzAcJYYfRU9+Y+Phr4/WcgUZwnuAerlTLiW/fRbU4c
eA5OS5QR9wlZuD+LK5nh6SHI7HhqSmKKEUblpfQijz01imJPsMJl9U+ek0SekoTkuGQKq4G1AphFuregZ3kGkoTlP0GjqV47Pqd+j5cEAerRbQ8+aAMj72gF
u8lCrLr9t6jWCQG6A6tzWg5ReYyq0SmGaNUC7zuGKUZHOU5XwqZH5ABvzx14+SqAWgl7K/Za7DqNMvYrFtvZXhlmEFRLk+HUVmnV56Y83tQoijelS79v/ph+
RqTxRDPUdjMpYizendf69gs33sttXA8rB3tFw460EWMGPAedDJlsuBg/QKkf62AQJqFXxhxruf0BtQohJ0y1UcZLhaXIb4/mDph2zP3fbYvV41wRg2qWnTzS
1Oh+nfBOi6LFnDab8fqKsF2Jhe6MZXaAZQq3HKWTnCryFdz7TXn8qHnw+0DYz18ZwqKsJsdVslGlVPnL+gv2Ir4PtQIs5ovklcJPHl1qqoWOAJQT0emL22Fv
26J4cY3mzQS8g3KMS3VetZRV/sGMY/YV92Sg+L98/3nvZ6NIYMx7XnYA+KUbD5tLdilIl0MPjxch4cJ29W3y2rVuV2moBzXS0ETuVqAcIIbjcDnCWYpv1lkN
h3k+EWYdvfKQVlPbN3ojVAHaLus3/foNnS9I/wHmi7kWM3g8yuibb1lpXGttG8KO4BfjqXTPxaaQOqGOBfri+pxvfoiU2gRIHSKk2s8MqZY81NRsPB1SpWnr
Sa56Jm2dKPKM4rj8cnoT5r4VPWFHEOJ86zi6N1qEZ0ISr/CCmxH8tGaySVkOecAPD/PDBfUeLD8bB9gcZ1qzR0YL2wpYLc2kxEJH6ZSdU+yz0/9YVhEe6scQ
UWpmtOQBpGbzhUBX4Rj23BKyQz2YZqdP8BCx2AGP+SxI3tg5fw/y8iVwxpY+D7Ot0HSgNITntQ+wqA9omTaLE+HLy8Aij900i2I3rIbYCI8MQzxvgZoKQcbs
q3p8n+ESNw+BULPiyoYPfMsr+0LNPfEoLuzqvEd+oAehL5F8vG4jvwHjUBPL8wO+VVUPAs8ahwE7LoVBhw/cYAPn9Rt7vFxkMu6iprkplGGVC71BcmbiO6YU
J+GxuIBcsvhpGgk/bRQPIq2fDGT45pOg5J+lic90PEk6GvEKz30U7D4XQFwebmm2yyCu/roDcGMW9u8W1qbLBVghiNnwN8CxPNyyjuAksKl+6bGH8/fiOz5T
eg/viC74yG83gS7cA6BtPito5QGYZlEA5mprzErNkMTIgPdkNm8J6qPvas+37tM/9Mgt8B3CJxHs19FNHf7nNeLxzwX+ieLqgkJdZe6bVTgBQeAe776S1mZB
BzQ+14i2Hadwk6Z3Mp55MqjxU2pZ+RgYT41gCd0UnsPJhHkXNoHpGeANnhBrDXp8s6aoncNywMb4qUMvpjI1Qh65aR6WwLVvY2m3Jbr/c1GRF0Asd9G8jTLH
+omEHV7HRZ30qDOJ9GVhidiHsXNox26ZuCBRaULSKiCVopktSNqM8YcCnMvyHvAs4MqGjkcG8CykgMT1ZE9db07qhctXyjphyeLeDR3ENWBSlHYxiHBIPOrD
rAS8cOViIYOkPNrSLIq2JFbaU7eLV9wonj2rM2tlvw77upcsc1QElT6TJXSlAz5h3MiYYORnAAFU/G6iI+h7YCGhqRSXXiWYdxzPDaEPtEJ1gnU7u6kqwt1i
QzDt9ehw4yoGkzzE0jp4AVgl+8Gf5M5xSOb+1+rP2QXecBqUkRB0Fx94MhqZYpVVaHunV9i4IwBaW2l1ngFo8mBKqyiYcrviZq/uv4mVFtBT/c08OUekvy/l
D5S8SMPLWVADVmIy6JE76phJseWqKerbhTh6eFaOk+8P/4WfnRPPM9sSyI2jSIZeXFxsjjncDq61Rck3V64fKBTPXOU7wR9d736SS3QSQE8eCWlppdCTpfI+
Fwh5JsmI2aOAQAxzjGLzkdlbqwGnXW2UyPSrqt+6KF4D+hV6Q4FrLPRgJq8VyWqjTTx3nqiszK2UhSJIWMqqdjMnK1PzNsec1uoC5rQizEVoN1zPo9KDPtry
QEmrKFDC8wFWBrFbjreS+cK+cqaDDu7bUSRY4OuH09HOwbjC/l7Iu42AOhkwhS6yG0ZL3NIDnMAOTXkxMwHA2q2CnddZgMFF3PGlSyEWhUXg71+x2SoS/sKv
5qaR2mbmJ75cd3h8deZqylsZsaU/8C5kWj7qngPj41e++9d3/w8m/3/B8J8AAA==
""".strip()


@dataclass(frozen=True)
class ParsedWorkbook:
    file_name: str
    sheet_name: str
    columns: list[str]
    rows: list[dict[str, Any]]
    warnings: list[str]
    source_row_numbers: list[int] | None = None


@dataclass(frozen=True)
class RunResult:
    run: dict[str, Any]
    results: list[dict[str, Any]]
    rows: list[dict[str, Any]]
    dry_run: bool


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat()


def new_id() -> str:
    return str(uuid.uuid4())


def stable_id(value: str) -> str:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return f"{digest[:8]}-{digest[8:12]}-{digest[12:16]}-{digest[16:20]}-{digest[20:32]}"


def json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if pd is not None:
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def json_dumps(value: Any, *, pretty: bool = False) -> str:
    return json.dumps(
        value,
        default=json_default,
        ensure_ascii=False,
        sort_keys=False,
        indent=2 if pretty else None,
        separators=None if pretty else (",", ":"),
    )


def json_loads_maybe(value: Any, fallback: Any = None) -> Any:
    if value is None:
        return fallback
    if isinstance(value, (dict, list, bool, int, float)):
        return value
    text = str(value).strip()
    if not text:
        return fallback
    try:
        return json.loads(text)
    except (TypeError, json.JSONDecodeError):
        return fallback if fallback is not None else value


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    if pd is not None:
        try:
            if pd.isna(value):
                return ""
        except (TypeError, ValueError):
            pass
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    if isinstance(value, Decimal):
        return format(value, "f").rstrip("0").rstrip(".")
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    return " ".join(str(value).strip().split())


def normalize_action(value: Any) -> str:
    text = clean_text(value)
    if not text:
        return ""
    lowered = text.lower()
    if lowered == "ok":
        return "OK"
    if lowered == "approved":
        return "Approved"
    if re.fullmatch(r"approved\s*-\s*1x", text, flags=re.IGNORECASE):
        return "Approved - 1X"
    if lowered == "blank":
        return ""
    return text


def normalize_key(value: Any) -> str:
    return re.sub(r"[^A-Z0-9]+", " ", clean_text(value).upper()).strip()


def parse_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float, Decimal)):
        number = float(value)
        return None if math.isnan(number) else number
    text = clean_text(value).replace(",", "").replace("%", "").strip()
    if not text or text.lower() == "blank":
        return None
    try:
        return float(Decimal(text))
    except (InvalidOperation, ValueError):
        return None


def percent_value(value: Any) -> float | None:
    parsed = parse_number(value)
    if parsed is None:
        return None
    return parsed / 100.0 if parsed > 1 else parsed


def normalize_date(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat() if value.tzinfo else value.replace(tzinfo=timezone.utc).isoformat()
    if isinstance(value, date):
        return datetime.combine(value, time.min, tzinfo=timezone.utc).isoformat()
    text = clean_text(value)
    if not text:
        return ""
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%m/%d/%y",
        "%Y/%m/%d",
    ):
        try:
            parsed = datetime.strptime(text.replace("Z", "+0000"), fmt)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.isoformat()
        except ValueError:
            continue
    return text


def append_text(existing: Any, value: Any) -> str:
    existing_text = clean_text(existing)
    value_text = clean_text(value)
    if not value_text:
        return existing_text
    if not existing_text:
        return value_text
    existing_parts = [item.strip() for item in existing_text.split(";") if item.strip()]
    if value_text in existing_parts:
        return existing_text
    return f"{existing_text}; {value_text}"


def bool_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float, Decimal)):
        return float(value) != 0
    text = clean_text(value).lower()
    if text in {"true", "1", "yes", "y", "on"}:
        return True
    if text in {"false", "0", "no", "n", "off", "", "none", "null"}:
        return False
    return True


def canonical_header(header: Any) -> str:
    raw = " ".join(clean_text(header).replace("_", " ").split())
    if not raw:
        return ""
    key = raw.lower().replace("-", " ").strip()
    if key in HEADER_ALIASES:
        return HEADER_ALIASES[key]
    return EXPECTED_HEADER_LOOKUP.get(key, raw)



def _plain_data(value: Any) -> Any:
    """Convert values to dictionaries/lists/primitives safe for Streamlit Session State."""
    if value is None or isinstance(value, (str, int, float, bool)):
        if isinstance(value, float) and math.isnan(value):
            return None
        return value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _plain_data(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_plain_data(item) for item in value]
    if pd is not None:
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
    if hasattr(value, "item"):
        try:
            return _plain_data(value.item())
        except Exception:
            pass
    return clean_text(value)


def parsed_workbook_to_payload(parsed: ParsedWorkbook) -> dict[str, Any]:
    return {
        "payload_type": "ParsedWorkbook",
        "payload_version": 1,
        "file_name": clean_text(parsed.file_name),
        "sheet_name": clean_text(parsed.sheet_name),
        "columns": [clean_text(column) for column in parsed.columns],
        "rows": _plain_data(parsed.rows),
        "warnings": [clean_text(item) for item in parsed.warnings if clean_text(item)],
        "source_row_numbers": [int(value) for value in (parsed.source_row_numbers or [])],
    }


def parsed_workbook_from_payload(value: Any) -> ParsedWorkbook | None:
    """Accept a plain payload or a legacy ParsedWorkbook from an older rerun."""
    if isinstance(value, Mapping):
        file_name = value.get("file_name")
        sheet_name = value.get("sheet_name")
        columns = value.get("columns")
        rows = value.get("rows")
        warnings = value.get("warnings")
        source_row_numbers = value.get("source_row_numbers")
    else:
        file_name = getattr(value, "file_name", None)
        sheet_name = getattr(value, "sheet_name", None)
        columns = getattr(value, "columns", None)
        rows = getattr(value, "rows", None)
        warnings = getattr(value, "warnings", None)
        source_row_numbers = getattr(value, "source_row_numbers", None)
    if not isinstance(columns, (list, tuple)) or not isinstance(rows, (list, tuple)):
        return None
    normalized_rows = [dict(row) for row in rows if isinstance(row, Mapping)]
    if len(normalized_rows) != len(rows):
        return None
    normalized_numbers: list[int] = []
    if isinstance(source_row_numbers, (list, tuple)):
        for number in source_row_numbers:
            try:
                normalized_numbers.append(int(number))
            except (TypeError, ValueError):
                normalized_numbers.append(len(normalized_numbers) + 2)
    return ParsedWorkbook(
        file_name=clean_text(file_name),
        sheet_name=clean_text(sheet_name) or "Sheet1",
        columns=[clean_text(column) for column in columns],
        rows=normalized_rows,
        warnings=[clean_text(item) for item in (warnings or []) if clean_text(item)],
        source_row_numbers=normalized_numbers or None,
    )


def run_result_to_payload(result: RunResult) -> dict[str, Any]:
    return {
        "payload_type": "RunResult",
        "payload_version": 1,
        "run": _plain_data(result.run),
        "results": _plain_data(result.results),
        "rows": _plain_data(result.rows),
        "dry_run": bool(result.dry_run),
    }


def run_result_from_payload(value: Any) -> RunResult | None:
    """Accept a plain payload or a legacy RunResult from an older rerun."""
    if isinstance(value, Mapping):
        run = value.get("run")
        results = value.get("results")
        rows = value.get("rows")
        dry_run = value.get("dry_run")
    else:
        run = getattr(value, "run", None)
        results = getattr(value, "results", None)
        rows = getattr(value, "rows", None)
        dry_run = getattr(value, "dry_run", None)
    if not isinstance(run, Mapping) or not isinstance(results, (list, tuple)) or not isinstance(rows, (list, tuple)):
        return None
    normalized_results = [dict(item) for item in results if isinstance(item, Mapping)]
    normalized_rows = [dict(item) for item in rows if isinstance(item, Mapping)]
    if len(normalized_results) != len(results) or len(normalized_rows) != len(rows):
        return None
    return RunResult(dict(run), normalized_results, normalized_rows, bool_value(dry_run))


def collapse_raw_row(raw_row: Mapping[str, Any]) -> dict[str, Any]:
    collapsed: dict[str, Any] = {}
    for key, value in raw_row.items():
        header = canonical_header(key)
        if not header:
            continue
        if header not in collapsed or not clean_text(collapsed[header]):
            collapsed[header] = value
    return collapsed


def queue_bucket_for_type(request_type: Any) -> str:
    key = normalize_key(request_type)
    if "PRF" in key:
        return "PRF Processing"
    if "SORF" in key:
        return "SORF Processing"
    if "SRF" in key:
        return "SRF Processing"
    return "Assigned for Processing"


def create_normalized_row(raw_row: Mapping[str, Any]) -> dict[str, Any]:
    source = collapse_raw_row(raw_row)

    def field(name: str) -> str:
        return clean_text(source.get(name))

    fields: dict[str, Any] = {
        "business": field("Business"),
        "requestType": field("Type"),
        "caseNumber": field("Case#"),
        "dateCreated": normalize_date(source.get("Date Created")),
        "sector": field("Sector"),
        "division": field("Division"),
        "unitName": field("Unit Name"),
        "unitNumber": field("Unit Number"),
        "vendor": field("Vendor"),
        "din": field("DIN"),
        "min": field("MIN"),
        "manufacturer": field("Manufacturer"),
        "brand": field("Brand"),
        "description": field("Description"),
        "parentCategory": field("Parent Category"),
        "subCategory": field("Sub Category"),
        "usageQty": parse_number(source.get("Usage")),
        "oneTimeOrPermanent": field("One-Time or Permanent"),
        "reasonForRequest": field("Reason for request"),
        "dpl": field("DPL"),
        "meetsCriteria": percent_value(source.get("Meets Criteria")),
        "inCat": field("In CAT"),
        "onMog": field("On MOG"),
        "pantry": field("Pantry"),
        "k12Apl": field("K12 APL"),
        "compassApl": field("Compass APL"),
        "conversionDin": field("Conversion DIN"),
        "conversionVaPct": percent_value(source.get("Conversion VA%")),
        "upstreamAction": normalize_action(source.get("ACTION")),
        "upstreamIfInStockAction": normalize_action(source.get("If In Stock: Action")),
        "upstreamBuysmartAction": normalize_action(source.get("Buysmart Action")),
    }

    def lower(name: str) -> str:
        return clean_text(fields.get(name)).lower()

    action_key = normalize_key(fields["upstreamAction"])
    buysmart_key = normalize_key(fields["upstreamBuysmartAction"])
    request_type_key = normalize_key(fields["requestType"])
    business_key = normalize_key(fields["business"])
    compass_apl = lower("compassApl")
    pantry = lower("pantry")
    division = lower("division")
    in_cat = lower("inCat")
    meets_criteria = fields["meetsCriteria"]

    derived: dict[str, Any] = {
        "business_key": business_key,
        "request_type_key": request_type_key,
        "is_compass": "COMPASS USA" in business_key,
        "is_canada": "COMPASS CANADA" in business_key,
        "is_healthtrust": "HEALTHTRUST" in business_key,
        "is_hmshost": "HMSHOST" in business_key,
        "is_foodbuyone": "FOODBUYONE" in business_key,
        "is_mass_add": request_type_key == "MASS ADDS",
        "is_mass_srf": request_type_key == "MASS ADDS SRF",
        "is_prf": request_type_key == "PRF",
        "is_sorf": request_type_key == "SORF",
        "is_srf": request_type_key == "SRF",
        "is_one_time": bool(re.search(r"one-time|one time|seasonal", clean_text(fields["oneTimeOrPermanent"]), re.I)),
        "is_permanent": bool(re.search(r"permanent", clean_text(fields["oneTimeOrPermanent"]), re.I)),
        "usage_num": fields["usageQty"],
        "meets_criteria_num": meets_criteria,
        "meets_criteria_ge_10": meets_criteria is not None and float(meets_criteria) >= 0.1,
        "in_cat_key": normalize_key(fields["inCat"]),
        "is_in_cat_y": clean_text(fields["inCat"]).lower() == "y",
        "is_temp_available": "temp available" in in_cat or in_cat == "ta",
        "is_in_catalog": clean_text(fields["inCat"]).lower() == "y" or "temp available" in in_cat,
        "is_pantry": "item" in pantry or "subcategory" in pantry or pantry == "y",
        "is_k12_apl": clean_text(fields["k12Apl"]).lower() == "y",
        "is_core_apl": "core apl" in compass_apl,
        "is_s1": bool(re.search(r"\bs1\b", clean_text(fields["compassApl"]), re.I)),
        "is_foh": "front of house" in compass_apl or bool(re.search(r"\bfoh\b", clean_text(fields["compassApl"]), re.I)),
        "is_diverse": "diverse" in compass_apl,
        "has_conversion": bool(clean_text(fields["conversionDin"])),
        "upstream_action_key": action_key,
        "current_action_key": action_key,
        "current_buysmart_key": buysmart_key,
        "brand_lc": lower("brand"),
        "manufacturer_lc": lower("manufacturer"),
        "description_lc": lower("description"),
        "subcategory_lc": lower("subCategory"),
        "parent_category_lc": lower("parentCategory"),
        "division_lc": division,
        "sector_lc": lower("sector"),
        "reason_lc": lower("reasonForRequest"),
        "vendor_lc": lower("vendor"),
        "din_lc": lower("din"),
        "min_lc": lower("min"),
        "is_levy": "levy" in lower("sector") or "levy" in division,
        "is_schools": "school" in division or "chartwells" in division,
    }
    return {"source": source, "fields": fields, "derived": derived}


def create_workflow_row(
    batch_id: str,
    raw_row: Mapping[str, Any],
    source_row_number: int,
    now: str | None = None,
) -> dict[str, Any]:
    timestamp = now or iso_now()
    normalized = create_normalized_row(raw_row)
    fields = normalized["fields"]
    upstream_action = clean_text(fields["upstreamAction"])
    upstream_if_stock = clean_text(fields["upstreamIfInStockAction"])
    request_type = clean_text(fields["requestType"])
    case_number = clean_text(fields["caseNumber"])
    row: dict[str, Any] = {
        "id": new_id(),
        "batch_id": batch_id,
        "source_row_number": int(source_row_number),
        "workflow_request_key": f"{case_number or 'row'}-{source_row_number}",
        "raw_row": deepcopy(normalized["source"]),
        "normalized_row": normalized,
        "business": clean_text(fields["business"]),
        "request_type": request_type,
        "case_number": case_number,
        "date_created": clean_text(fields["dateCreated"]),
        "sector": clean_text(fields["sector"]),
        "division": clean_text(fields["division"]),
        "unit_name": clean_text(fields["unitName"]),
        "unit_number": clean_text(fields["unitNumber"]),
        "vendor": clean_text(fields["vendor"]),
        "din": clean_text(fields["din"]),
        "min": clean_text(fields["min"]),
        "manufacturer": clean_text(fields["manufacturer"]),
        "brand": clean_text(fields["brand"]),
        "description": clean_text(fields["description"]),
        "parent_category": clean_text(fields["parentCategory"]),
        "sub_category": clean_text(fields["subCategory"]),
        "usage_qty": fields["usageQty"],
        "one_time_or_permanent": clean_text(fields["oneTimeOrPermanent"]),
        "reason_for_request": clean_text(fields["reasonForRequest"]),
        "dpl": clean_text(fields["dpl"]),
        "meets_criteria": fields["meetsCriteria"],
        "in_cat": clean_text(fields["inCat"]),
        "on_mog": clean_text(fields["onMog"]),
        "pantry": clean_text(fields["pantry"]),
        "k12_apl": clean_text(fields["k12Apl"]),
        "compass_apl": clean_text(fields["compassApl"]),
        "conversion_din": clean_text(fields["conversionDin"]),
        "conversion_va_pct": fields["conversionVaPct"],
        "upstream_action": upstream_action,
        "upstream_if_in_stock_action": upstream_if_stock,
        "action": upstream_action,
        "if_in_stock_action": upstream_if_stock,
        "buysmart_action": clean_text(fields["upstreamBuysmartAction"]),
        "rule_applied": "",
        "execution_trace": [],
        "needs_review": False,
        "analyst_notes": "",
        "validation_status": "",
        "excluded": False,
        "excluded_reason": "",
        "queue_bucket": queue_bucket_for_type(request_type),
        "request_bucket": queue_bucket_for_type(request_type),
        "outcome_reporting": "",
        "selected": False,
        "assignment": "",
        "status": "Ready",
        "last_sync_at": "",
        "last_saved_at": "",
        "created_at": timestamp,
        "updated_at": timestamp,
    }
    row["outcome_reporting"] = classify_outcome(row)
    return row


def refresh_derived(row: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
    source = deepcopy(row.get("raw_row") or {})
    source["ACTION"] = row.get("upstream_action", "")
    source["If In Stock: Action"] = row.get("upstream_if_in_stock_action", "")
    source["Buysmart Action"] = row.get("buysmart_action", "")
    normalized = create_normalized_row(source)
    normalized["derived"]["current_action_key"] = normalize_key(row.get("action"))
    normalized["derived"]["current_buysmart_key"] = normalize_key(row.get("buysmart_action"))
    row["normalized_row"] = normalized
    if not clean_text(row.get("queue_bucket")):
        row["queue_bucket"] = queue_bucket_for_type(row.get("request_type"))
    return row


def context_for_row(row: Mapping[str, Any]) -> dict[str, Any]:
    normalized = row.get("normalized_row") or {}
    context: dict[str, Any] = {}
    context.update(deepcopy(normalized.get("fields") or {}))
    context.update(deepcopy(normalized.get("derived") or {}))
    context["current_action_key"] = normalize_key(row.get("action"))
    context["current_buysmart_key"] = normalize_key(row.get("buysmart_action"))
    context["action"] = clean_text(row.get("action"))
    context["buysmartAction"] = clean_text(row.get("buysmart_action"))
    return context


def _number_for_compare(value: Any) -> float:
    parsed = parse_number(value)
    return parsed if parsed is not None else 0.0


def _in_list(left: Any, right: Any) -> bool:
    value = normalize_key(left)
    if isinstance(right, list):
        options = right
    else:
        options = [item.strip() for item in clean_text(right).split(",") if item.strip()]
    return any(normalize_key(item) == value for item in options)


def evaluate_predicate(
    predicate: Mapping[str, Any] | None,
    context: Mapping[str, Any],
    reference_lists: Mapping[str, Sequence[str]] | None = None,
) -> bool:
    if not predicate:
        return False
    if isinstance(predicate.get("all"), list):
        return all(evaluate_predicate(item, context, reference_lists) for item in predicate["all"] if isinstance(item, Mapping))
    if isinstance(predicate.get("any"), list):
        return any(evaluate_predicate(item, context, reference_lists) for item in predicate["any"] if isinstance(item, Mapping))
    if isinstance(predicate.get("not"), Mapping):
        return not evaluate_predicate(predicate["not"], context, reference_lists)

    field = clean_text(predicate.get("field"))
    op = clean_text(predicate.get("op"))
    left = context.get(field)
    right = predicate.get("value")

    if op == "eq":
        return normalize_key(left) == normalize_key(right)
    if op == "ne":
        return normalize_key(left) != normalize_key(right)
    if op == "contains":
        return clean_text(right).lower() in clean_text(left).lower()
    if op == "not_contains":
        return clean_text(right).lower() not in clean_text(left).lower()
    if op == "blank":
        return not clean_text(left)
    if op == "not_blank":
        return bool(clean_text(left))
    if op == "is_true":
        return bool_value(left)
    if op == "is_false":
        return not bool_value(left)
    if op == "gt":
        return _number_for_compare(left) > _number_for_compare(right)
    if op == "ge":
        return _number_for_compare(left) >= _number_for_compare(right)
    if op == "lt":
        return _number_for_compare(left) < _number_for_compare(right)
    if op == "le":
        return _number_for_compare(left) <= _number_for_compare(right)
    if op == "in":
        return _in_list(left, right)
    if op == "not_in":
        return not _in_list(left, right)
    if op in {"in_ref", "not_in_ref"}:
        lists = reference_lists or DEFAULT_REFERENCE_LISTS
        values = lists.get(clean_text(right), [])
        matched = any(clean_text(item).lower() in clean_text(left).lower() for item in values)
        return matched if op == "in_ref" else not matched
    if op in {"regex", "not_regex"}:
        try:
            matched = bool(re.search(clean_text(right), clean_text(left), flags=re.IGNORECASE))
        except re.error:
            matched = False
        return matched if op == "regex" else not matched
    return False


def summarize_actions(actions: Sequence[Mapping[str, Any]]) -> str:
    parts: list[str] = []
    for action in actions:
        action_type = clean_text(action.get("type"))
        value = clean_text(action.get("value"))
        if not value and action_type == "exclude":
            value = clean_text(action.get("reason"))
        parts.append(f"{action_type}: {value}" if value else action_type)
    return ", ".join(parts)


def apply_actions(
    row: MutableMapping[str, Any],
    actions: Sequence[Mapping[str, Any]],
    variant: Mapping[str, Any],
    reference_lists: Mapping[str, Sequence[str]] | None = None,
) -> None:
    for node in actions:
        context = context_for_row(row)
        when = node.get("when")
        if isinstance(when, Mapping) and not evaluate_predicate(when, context, reference_lists):
            continue
        if bool_value(node.get("only_if_action_blank")) and clean_text(row.get("action")):
            continue
        action_type = clean_text(node.get("type"))
        if action_type == "set_action":
            row["action"] = normalize_action(node.get("value"))
        elif action_type == "set_action_by_duration":
            duration = clean_text(row.get("one_time_or_permanent")).lower()
            row["action"] = "1X" if "one" in duration or "seasonal" in duration else "OK"
        elif action_type == "set_if_stock":
            row["if_in_stock_action"] = normalize_action(node.get("value"))
        elif action_type == "set_buysmart":
            row["buysmart_action"] = normalize_action(node.get("value"))
        elif action_type == "set_review":
            row["needs_review"] = bool_value(node.get("value", True))
        elif action_type == "append_validation":
            row["validation_status"] = append_text(row.get("validation_status"), node.get("value"))
        elif action_type == "add_note":
            row["analyst_notes"] = append_text(row.get("analyst_notes"), node.get("value"))
        elif action_type == "exclude":
            row["excluded"] = True
            row["excluded_reason"] = clean_text(node.get("reason")) or clean_text(variant.get("description"))
            row["needs_review"] = False
            row["buysmart_action"] = ""
        elif action_type == "clear_field":
            if clean_text(node.get("field")) == "Conversion DIN":
                row["conversion_din"] = ""
        elif action_type == "preserve_action_set_if_stock":
            upstream = clean_text(row.get("upstream_action"))
            if upstream and ("on mog" in upstream.lower() or "cannot add" in upstream.lower()):
                row["action"] = upstream
                row["if_in_stock_action"] = normalize_action(node.get("value"))


def executable_variants(rules: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    variants: list[dict[str, Any]] = []
    for rule in rules:
        for variant in rule.get("variants") or []:
            if (
                bool_value(variant.get("enabled"))
                and bool_value(variant.get("is_executable"))
                and clean_text(variant.get("status")) == "approved"
            ):
                variants.append(dict(variant))
    return sorted(
        variants,
        key=lambda variant: (
            int(variant.get("execution_priority") or 0),
            RUNTIME_KIND_ORDER.get(clean_text(variant.get("runtime_kind")), 9),
            clean_text(variant.get("runtime_rule_id")).lower(),
        ),
    )


def execute_row(
    input_row: Mapping[str, Any],
    variants: Sequence[Mapping[str, Any]],
    reference_lists: Mapping[str, Sequence[str]] | None = None,
) -> dict[str, Any]:
    now = iso_now()
    row = deepcopy(dict(input_row))
    refresh_derived(row)
    row["execution_trace"] = []
    row["rule_applied"] = ""
    ordered = sorted(
        [dict(variant) for variant in variants],
        key=lambda variant: (
            int(variant.get("execution_priority") or 0),
            RUNTIME_KIND_ORDER.get(clean_text(variant.get("runtime_kind")), 9),
            clean_text(variant.get("runtime_rule_id")).lower(),
        ),
    )
    if not ordered:
        row["needs_review"] = True
        row["validation_status"] = append_text(row.get("validation_status"), "Executable rule catalog missing")
        row["outcome_reporting"] = classify_outcome(row)
        row["queue_bucket"] = bucket_for_row(row)["label"]
        row["updated_at"] = now
        return row

    for variant in ordered:
        predicate = variant.get("predicate_json")
        if not isinstance(predicate, Mapping) or not evaluate_predicate(predicate, context_for_row(row), reference_lists):
            continue
        actions = variant.get("action_json") or []
        if not isinstance(actions, list):
            actions = []
        apply_actions(row, [item for item in actions if isinstance(item, Mapping)], variant, reference_lists)
        trace = {
            "runtimeRuleId": clean_text(variant.get("runtime_rule_id")),
            "ruleId": clean_text(variant.get("rule_id")),
            "description": clean_text(variant.get("description")),
            "actionSummary": summarize_actions([item for item in actions if isinstance(item, Mapping)]),
            "executionPriority": int(variant.get("execution_priority") or 0),
            "runtimeKind": clean_text(variant.get("runtime_kind")),
            "matchedAt": now,
            "automationLevel": clean_text(variant.get("automation_level")),
        }
        row["execution_trace"].append(trace)
        row["rule_applied"] = append_text(row.get("rule_applied"), variant.get("runtime_rule_id"))
        refresh_derived(row)
        if bool_value(variant.get("stop_processing")):
            break

    if not clean_text(row.get("buysmart_action")) and not bool_value(row.get("excluded")):
        row["buysmart_action"] = "Review" if bool_value(row.get("needs_review")) else "Assigned"
    row["status"] = "Excluded" if bool_value(row.get("excluded")) else "Review" if bool_value(row.get("needs_review")) else "Ready"
    row["outcome_reporting"] = classify_outcome(row)
    row["queue_bucket"] = bucket_for_row(row)["label"]
    row["updated_at"] = now
    return row


def decision_snapshot(row: Mapping[str, Any]) -> str:
    return "|".join(
        [
            clean_text(row.get("action")),
            clean_text(row.get("if_in_stock_action")),
            clean_text(row.get("buysmart_action")),
            str(bool_value(row.get("needs_review"))),
            str(bool_value(row.get("excluded"))),
            clean_text(row.get("outcome_reporting")),
        ]
    )


def execute_rows(
    input_rows: Sequence[Mapping[str, Any]],
    rules: Sequence[Mapping[str, Any]],
    row_ids: Sequence[str] | None = None,
    reference_lists: Mapping[str, Sequence[str]] | None = None,
) -> tuple[list[dict[str, Any]], int, int]:
    variants = executable_variants(rules)
    selected = set(row_ids or []) or None
    changed = 0
    review = 0
    output: list[dict[str, Any]] = []
    for source_row in input_rows:
        if selected is not None and clean_text(source_row.get("id")) not in selected:
            output.append(deepcopy(dict(source_row)))
            continue
        before = decision_snapshot(source_row)
        executed = execute_row(source_row, variants, reference_lists)
        if before != decision_snapshot(executed):
            changed += 1
        if bool_value(executed.get("needs_review")):
            review += 1
        output.append(executed)
    return output, changed, review


def create_results(
    run_id: str,
    before_rows: Sequence[Mapping[str, Any]],
    after_rows: Sequence[Mapping[str, Any]],
    row_ids: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    selected = set(row_ids or []) or None
    before_by_id = {clean_text(row.get("id")): row for row in before_rows}
    results: list[dict[str, Any]] = []
    for row in after_rows:
        row_id = clean_text(row.get("id"))
        if selected is not None and row_id not in selected:
            continue
        before = before_by_id.get(row_id, row)
        trace = row.get("execution_trace") or []
        if not trace and decision_snapshot(row) == decision_snapshot(before):
            continue
        rules_applied = [clean_text(item.get("runtimeRuleId")) for item in trace if isinstance(item, Mapping)]
        validations = [item.strip() for item in clean_text(row.get("validation_status")).split(";") if item.strip()]
        results.append(
            {
                "id": new_id(),
                "run_id": run_id,
                "workflow_row_id": row_id,
                "before_state": deepcopy(dict(before)),
                "after_state": deepcopy(dict(row)),
                "trace": deepcopy(trace),
                "rules_applied": rules_applied,
                "validations": validations,
                "created_at": iso_now(),
            }
        )
    return results


def classify_outcome(row: Mapping[str, Any]) -> str:
    action = clean_text(row.get("action"))
    buysmart = clean_text(row.get("buysmart_action"))
    if bool_value(row.get("excluded")):
        return "Excluded"
    if clean_text(row.get("validation_status")):
        return "Data Issue"
    if bool_value(row.get("needs_review")):
        return "Review"
    if "cannot add" in action.lower() or "denied" in buysmart.lower():
        return "Denied"
    if "find alt" in action.lower():
        return "Find Alt First"
    if "use right" in action.lower():
        return "Use Right"
    if "1x" in action.lower() or "1x" in buysmart.lower():
        return "Approved - 1X"
    if "ok" in action.lower() or "approved" in buysmart.lower():
        return "Approved"
    if "assigned" in buysmart.lower():
        return "Assigned"
    return "Pending"


def bucket_for_row(row: Mapping[str, Any]) -> dict[str, str]:
    action = clean_text(row.get("action"))
    buysmart = clean_text(row.get("buysmart_action"))
    if bool_value(row.get("excluded")):
        return BUCKET_BY_ID["vendor-exclusions"]
    if clean_text(row.get("validation_status")):
        return BUCKET_BY_ID["data-issues"]
    if "cannot add" in action.lower() or "denied" in buysmart.lower():
        return BUCKET_BY_ID["denied"]
    if "use right" in action.lower():
        return BUCKET_BY_ID["use-right"]
    if "find alt" in action.lower():
        return BUCKET_BY_ID["find-alt"]
    if bool_value(row.get("needs_review")):
        return BUCKET_BY_ID["compliance-review"]
    if "1x" in action.lower():
        return BUCKET_BY_ID["approved-1x"]
    if "ok" in action.lower() or "approved" in buysmart.lower():
        return BUCKET_BY_ID["auto-approved"]
    return BUCKET_BY_ID["assigned-processing"]


def summarize_batch(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    row_count = len(rows)
    outcome_counts: dict[str, int] = {}
    business_counts: dict[str, int] = {}
    type_counts: dict[str, int] = {}
    for row in rows:
        for value, target in (
            (clean_text(row.get("outcome_reporting")), outcome_counts),
            (clean_text(row.get("business")), business_counts),
            (clean_text(row.get("request_type")), type_counts),
        ):
            if value:
                target[value] = target.get(value, 0) + 1

    bucket_summaries: list[dict[str, Any]] = []
    for bucket in BUCKETS:
        bucket_rows = [row for row in rows if bucket_for_row(row)["id"] == bucket["id"]]
        if not bucket_rows:
            continue
        bucket_summaries.append(
            {
                **bucket,
                "count": len(bucket_rows),
                "review_count": sum(bool_value(row.get("needs_review")) for row in bucket_rows),
                "outcome_keys": sorted({clean_text(row.get("outcome_reporting")) for row in bucket_rows if clean_text(row.get("outcome_reporting"))}),
                "rule_ids": sorted(
                    {
                        item.strip()
                        for row in bucket_rows
                        for item in clean_text(row.get("rule_applied")).split(";")
                        if item.strip()
                    }
                ),
                "examples": [
                    {
                        "row_id": clean_text(row.get("id")),
                        "case_number": clean_text(row.get("case_number")),
                        "vendor": clean_text(row.get("vendor")),
                        "description": clean_text(row.get("description")),
                        "action": clean_text(row.get("action")),
                        "buysmart_action": clean_text(row.get("buysmart_action")),
                        "outcome_reporting": clean_text(row.get("outcome_reporting")),
                        "rule_applied": clean_text(row.get("rule_applied")),
                    }
                    for row in bucket_rows[:3]
                ],
            }
        )

    return {
        "row_count": row_count,
        "review_count": sum(bool_value(row.get("needs_review")) for row in rows),
        "excluded_count": sum(bool_value(row.get("excluded")) for row in rows),
        "approved_count": sum("approved" in clean_text(row.get("buysmart_action")).lower() for row in rows),
        "denied_count": sum(
            "denied" in clean_text(row.get("buysmart_action")).lower()
            or "cannot add" in clean_text(row.get("action")).lower()
            for row in rows
        ),
        "assigned_count": sum("assigned" in clean_text(row.get("buysmart_action")).lower() for row in rows),
        "outcome_counts": dict(sorted(outcome_counts.items())),
        "business_counts": dict(sorted(business_counts.items())),
        "type_counts": dict(sorted(type_counts.items())),
        "bucket_summaries": bucket_summaries,
        "automation_coverage_pct": round(
            (sum(bool(clean_text(row.get("rule_applied"))) for row in rows) * 100.0 / row_count) if row_count else 0.0,
            1,
        ),
    }


def catalog_snapshot(rules: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    ordered = executable_variants(rules)
    return {
        "ruleCount": len(rules),
        "variantCount": sum(len(rule.get("variants") or []) for rule in rules),
        "executionOrder": [
            {
                "runtimeRuleId": variant.get("runtime_rule_id"),
                "ruleId": variant.get("rule_id"),
                "priority": variant.get("execution_priority"),
                "runtimeKind": variant.get("runtime_kind"),
                "stopProcessing": variant.get("stop_processing"),
            }
            for variant in ordered
        ],
        "capturedAt": iso_now(),
    }


def field_pred(field: str, op: str, value: Any = None) -> dict[str, Any]:
    predicate: dict[str, Any] = {"field": field, "op": op}
    if value is not None:
        predicate["value"] = value
    return predicate


def all_pred(*predicates: Mapping[str, Any]) -> dict[str, Any]:
    return {"all": [dict(item) for item in predicates]}


def any_pred(*predicates: Mapping[str, Any]) -> dict[str, Any]:
    return {"any": [dict(item) for item in predicates]}


def not_pred(predicate: Mapping[str, Any]) -> dict[str, Any]:
    return {"not": dict(predicate)}


def action_node(action_type: str, **values: Any) -> dict[str, Any]:
    return {"type": action_type, **values}


def preferred_predicate() -> dict[str, Any]:
    return any_pred(
        field_pred("is_s1", "is_true"),
        field_pred("is_foh", "is_true"),
        field_pred("is_diverse", "is_true"),
        field_pred("is_core_apl", "is_true"),
        field_pred("is_pantry", "is_true"),
        field_pred("meets_criteria_ge_10", "is_true"),
    )


def not_preferred_predicate() -> dict[str, Any]:
    return not_pred(preferred_predicate())


def executable_spec_for(daf_row: Mapping[str, Any]) -> dict[str, Any] | None:
    rule_id = clean_text(daf_row.get("ruleId"))
    criteria = clean_text(daf_row.get("decisionCriteria")).lower()
    business = clean_text(daf_row.get("business")).lower()
    request_types = clean_text(daf_row.get("requestTypes")).lower()
    source_action = clean_text(daf_row.get("action"))
    source_buysmart = clean_text(daf_row.get("buysmartAction"))

    def spec(
        predicate: Mapping[str, Any],
        actions: Sequence[Mapping[str, Any]],
        stop_processing: bool = False,
        runtime_kind: str = "row_rule",
    ) -> dict[str, Any]:
        return {
            "predicate": deepcopy(dict(predicate)),
            "actions": [deepcopy(dict(item)) for item in actions],
            "stop_processing": stop_processing,
            "runtime_kind": runtime_kind,
        }

    if rule_id == "R001":
        return spec(
            field_pred("vendor_lc", "in_ref", "local_vendors"),
            [
                action_node("exclude", reason="Local/vendor exclusion from DAF R001"),
                action_node("add_note", value="Removed from managed workflow by vendor exclusion."),
            ],
            True,
            "validation_rule",
        )
    if rule_id == "R002":
        return spec(
            any_pred(field_pred("min_lc", "blank"), field_pred("din_lc", "blank")),
            [
                action_node("set_action", value="Invalid Information"),
                action_node("append_validation", value="Missing MIN or DIN"),
                action_node("set_review", value=True),
                action_node("set_buysmart", value="Review"),
            ],
            True,
            "validation_rule",
        )
    if rule_id == "R004":
        return spec(
            field_pred("is_hmshost", "is_true"),
            [
                action_node("set_action", value="Review"),
                action_node("set_buysmart", value="Assigned"),
                action_node("set_review", value=True),
                action_node("add_note", value="Route as HMSHost."),
            ],
        )
    if rule_id == "R005":
        return spec(
            all_pred(field_pred("is_canada", "is_true"), field_pred("is_mass_add", "is_true")),
            [
                action_node("set_buysmart", value="Assigned"),
                action_node("set_review", value=True),
                action_node("add_note", value="Canada mass add requires APL/Pantry confirmation."),
            ],
        )
    if rule_id == "R006":
        return spec(
            all_pred(
                field_pred("is_canada", "is_true"),
                field_pred("is_prf", "is_true"),
                any_pred(field_pred("is_s1", "is_true"), field_pred("is_pantry", "is_true")),
            ),
            [
                action_node(
                    "preserve_action_set_if_stock",
                    value="OK" if "cannot" in source_action.lower() else "",
                ),
                action_node("set_action", value=source_action or "OK", only_if_action_blank=True),
                action_node("set_buysmart", value="Assigned"),
            ],
        )
    if rule_id == "R007":
        return spec(
            all_pred(
                field_pred("is_canada", "is_true"),
                any_pred(
                    field_pred("is_prf", "is_true"),
                    field_pred("is_sorf", "is_true"),
                    field_pred("is_srf", "is_true"),
                ),
                field_pred("is_one_time", "is_true"),
                field_pred("usage_num", "le", 10),
                not_preferred_predicate(),
            ),
            [
                action_node("set_action", value="1X"),
                action_node("set_if_stock", value="OK"),
                action_node("set_buysmart", value="Assigned"),
            ],
        )
    if rule_id == "R008":
        return spec(
            all_pred(
                field_pred("is_canada", "is_true"),
                field_pred("is_one_time", "is_true"),
                field_pred("usage_num", "gt", 10),
                not_preferred_predicate(),
            ),
            [
                action_node("set_buysmart", value="Review"),
                action_node("set_review", value=True),
                action_node("add_note", value="Canada one-time usage above 10 requires escalation."),
            ],
        )
    if rule_id == "R011":
        return spec(
            all_pred(
                field_pred("is_healthtrust", "is_true"),
                field_pred("is_prf", "is_true"),
                field_pred("has_conversion", "is_true"),
            ),
            [action_node("set_action", value="Use Right"), action_node("set_buysmart", value="Assigned")],
        )
    if rule_id == "R012" and ("is not" in criteria or "does not" in criteria):
        return spec(
            all_pred(field_pred("is_healthtrust", "is_true"), not_preferred_predicate()),
            [
                action_node("set_action", value="Review"),
                action_node("set_buysmart", value="Assigned"),
                action_node("set_review", value=True),
            ],
        )
    if rule_id == "R012":
        return spec(
            all_pred(
                field_pred("is_healthtrust", "is_true"),
                any_pred(field_pred("is_prf", "is_true"), field_pred("is_sorf", "is_true")),
                preferred_predicate(),
            ),
            [
                action_node("set_action_by_duration"),
                action_node("set_buysmart", value=source_buysmart or "Assigned"),
            ],
        )
    if rule_id == "R014" and "healthtrust" in business:
        return spec(
            all_pred(
                field_pred("is_healthtrust", "is_true"),
                field_pred("is_sorf", "is_true"),
                field_pred("has_conversion", "is_true"),
                field_pred("usage_num", "lt", 10),
            ),
            [
                action_node("set_action", value="Use Right"),
                action_node("set_buysmart", value="Review"),
                action_node("set_review", value=True),
            ],
        )
    if rule_id == "R014":
        return spec(
            all_pred(
                field_pred("is_compass", "is_true"),
                field_pred("is_srf", "is_true"),
                field_pred("has_conversion", "is_true"),
            ),
            [action_node("set_action", value="Use Right"), action_node("set_buysmart", value="Assigned")],
        )
    if rule_id == "R016":
        return spec(
            all_pred(
                field_pred("is_compass", "is_true"),
                any_pred(
                    field_pred("reason_lc", "contains", "sponsorship"),
                    field_pred("reason_lc", "contains", "menucycle"),
                ),
            ),
            [action_node("set_action_by_duration"), action_node("set_buysmart", value="Assigned")],
        )
    if rule_id == "R023":
        return spec(
            all_pred(
                field_pred("is_compass", "is_true"),
                any_pred(field_pred("is_prf", "is_true"), field_pred("is_sorf", "is_true")),
                any_pred(
                    field_pred("is_s1", "is_true"),
                    field_pred("is_foh", "is_true"),
                    field_pred("is_diverse", "is_true"),
                    field_pred("is_core_apl", "is_true"),
                ),
            ),
            [
                action_node("set_action_by_duration"),
                action_node("preserve_action_set_if_stock", value="OK"),
            ],
        )
    if rule_id == "R024":
        return spec(
            all_pred(
                field_pred("is_compass", "is_true"),
                field_pred("is_schools", "is_true"),
                field_pred("is_k12_apl", "is_true"),
            ),
            [action_node("set_action_by_duration")],
        )
    if rule_id == "R025":
        return spec(field_pred("is_pantry", "is_true"), [action_node("set_action_by_duration")])
    if rule_id == "R026":
        return spec(field_pred("meets_criteria_ge_10", "is_true"), [action_node("set_action_by_duration")])
    if rule_id == "R027":
        return spec(
            any_pred(
                field_pred("reason_lc", "contains", "sponsorship"),
                field_pred("reason_lc", "contains", "commodity"),
                field_pred("reason_lc", "contains", "allocation"),
            ),
            [action_node("set_action_by_duration")],
        )
    if rule_id == "R028":
        return spec(
            field_pred(
                "description_lc",
                "regex",
                r"halal|gluten free|sugar free|vegan|kosher|\bgf\b|puree|nutritional",
            ),
            [action_node("set_action_by_duration")],
        )
    if rule_id == "R036":
        return spec(
            all_pred(field_pred("is_one_time", "is_true"), field_pred("usage_num", "lt", 15)),
            [
                action_node("set_action", value="1X", only_if_action_blank=True),
                action_node("set_buysmart", value="Assigned"),
            ],
        )
    if rule_id == "R041":
        return spec(
            all_pred(
                field_pred("is_compass", "is_true"),
                field_pred("is_prf", "is_true"),
                field_pred("is_permanent", "is_true"),
                any_pred(
                    field_pred("current_action_key", "eq", "OK"),
                    field_pred("current_action_key", "contains", "ON MOG"),
                ),
                field_pred("is_in_cat_y", "is_true"),
                field_pred("din_lc", "not_contains", "new"),
            ),
            [action_node("set_buysmart", value="Approved")],
            False,
            "buysmart_rule",
        )
    if rule_id == "R042":
        return spec(
            all_pred(
                any_pred(field_pred("is_in_cat_y", "is_false"), field_pred("is_temp_available", "is_true")),
                field_pred("current_action_key", "contains", "CANNOT ADD"),
            ),
            [action_node("set_buysmart", value="Denied")],
            False,
            "buysmart_rule",
        )
    if rule_id == "R043":
        return spec(
            any_pred(field_pred("is_mass_add", "is_true"), field_pred("is_mass_srf", "is_true")),
            [action_node("set_buysmart", value="Assigned")],
            False,
            "buysmart_rule",
        )
    if rule_id == "R044":
        return spec(
            field_pred("current_buysmart_key", "blank"),
            [action_node("set_buysmart", value="Assigned")],
            False,
            "buysmart_rule",
        )
    if rule_id == "R047":
        return spec(
            all_pred(field_pred("is_prf", "is_true"), field_pred("current_action_key", "eq", "1X")),
            [action_node("set_buysmart", value="Approved")],
            False,
            "downstream_rule",
        )
    if rule_id == "R048":
        return spec(
            all_pred(field_pred("is_prf", "is_true"), field_pred("current_action_key", "contains", "ON MOG")),
            [action_node("set_buysmart", value="Approved")],
            False,
            "downstream_rule",
        )
    if "approved rows" in request_types:
        note = first_text(daf_row.get("downstreamHandling"), daf_row.get("setAction"))
        return spec(
            field_pred("current_buysmart_key", "eq", "APPROVED"),
            [action_node("add_note", value=note)],
            False,
            "downstream_rule",
        )
    return None


def first_text(*values: Any) -> str:
    for value in values:
        text = clean_text(value)
        if text:
            return text
    return ""


def build_aggregate_logic(daf_row: Mapping[str, Any]) -> str:
    parts = [
        f"ACTION: {clean_text(daf_row.get('action'))}" if clean_text(daf_row.get("action")) else "",
        f"If In Stock: {clean_text(daf_row.get('ifInStockAction'))}" if clean_text(daf_row.get("ifInStockAction")) else "",
        f"BuySmart Action: {clean_text(daf_row.get('buysmartAction'))}" if clean_text(daf_row.get("buysmartAction")) else "",
        f"Set ACTION: {clean_text(daf_row.get('setAction'))}" if clean_text(daf_row.get("setAction")) else "",
        f"Downstream: {clean_text(daf_row.get('downstreamHandling'))}" if clean_text(daf_row.get("downstreamHandling")) else "",
    ]
    return " | ".join(part for part in parts if part)


def runtime_kind_for(daf_row: Mapping[str, Any]) -> str:
    group = clean_text(daf_row.get("ruleGroup")).lower()
    if "closeout" in group:
        return "buysmart_rule"
    if "upload" in group or "splitting" in group:
        return "downstream_rule"
    if "pre-processing" in group:
        return "validation_rule"
    return "row_rule"


def automation_level_for(daf_row: Mapping[str, Any], spec: Mapping[str, Any] | None) -> str:
    if spec is not None:
        return "alpha"
    text = " ".join(
        clean_text(daf_row.get(key))
        for key in ("decisionCriteria", "setAction", "downstreamHandling", "notes")
    ).lower()
    if any(word in text for word in ("external", "matrix", "judgment", "follow up")):
        return "manual"
    if any(word in text for word in ("manual", "review", "specialist")):
        return "guided"
    return "future"


def status_for_automation(level: str) -> str:
    if level == "alpha":
        return "approved"
    if level == "guided":
        return "ready"
    return "draft"


def aggregate_automation(variants: Sequence[Mapping[str, Any]]) -> str:
    levels = {clean_text(item.get("automation_level")) for item in variants}
    if "alpha" in levels:
        return "alpha"
    if "guided" in levels:
        return "guided"
    if "manual" in levels:
        return "manual"
    return "future"


def rule_number(rule_id: str) -> int:
    digits = "".join(character for character in rule_id if character.isdigit())
    return int(digits) if digits else 9999


def load_bundled_daf_workbook() -> dict[str, Any]:
    payload = gzip.decompress(base64.b64decode(DAF_SEED_GZIP_BASE64))
    return json.loads(payload.decode("utf-8-sig"))


def build_seed_catalog() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    workbook = load_bundled_daf_workbook()
    logic_rows = workbook.get("logicRows") or []
    grouped: dict[str, list[dict[str, Any]]] = {}
    for raw in logic_rows:
        row = dict(raw)
        grouped.setdefault(clean_text(row.get("ruleId")), []).append(row)

    duplicate_rule_ids = sorted(rule_id for rule_id, rows in grouped.items() if len(rows) > 1)
    timestamp = iso_now()
    rules: list[dict[str, Any]] = []
    executable_count = 0
    guided_count = 0
    manual_count = 0

    for rule_id in sorted(grouped, key=lambda item: (rule_number(item), item)):
        rows = grouped[rule_id]
        first = rows[0]
        definition_id = stable_id(f"definition:{rule_id}")
        version_id = stable_id(f"version:{rule_id}:1")
        variants: list[dict[str, Any]] = []
        for index, daf_row in enumerate(rows, start=1):
            spec = executable_spec_for(daf_row)
            field_filter_logic = first_text(
                daf_row.get("fieldFilterLogic"),
                daf_row.get("decisionCriteria"),
                f"{clean_text(daf_row.get('business'))} {clean_text(daf_row.get('requestTypes'))}".strip(),
            )
            aggregate_logic = first_text(daf_row.get("aggregateLogic"), build_aggregate_logic(daf_row))
            warnings = [] if spec else [
                "This DAF row references judgment, external lookup data, or downstream handling and is stored for guided/manual execution."
            ]
            level = automation_level_for(daf_row, spec)
            status = status_for_automation(level)
            if level == "alpha":
                executable_count += 1
            elif level == "guided":
                guided_count += 1
            elif level in {"manual", "future"}:
                manual_count += 1
            predicate = deepcopy(spec.get("predicate")) if spec else None
            actions = deepcopy(spec.get("actions")) if spec else None
            source = {
                "ruleId": rule_id,
                "ruleGroup": clean_text(daf_row.get("ruleGroup")),
                "business": clean_text(daf_row.get("business")),
                "requestTypes": clean_text(daf_row.get("requestTypes")),
                "decisionCriteria": clean_text(daf_row.get("decisionCriteria")),
                "action": clean_text(daf_row.get("action")),
                "ifInStockAction": clean_text(daf_row.get("ifInStockAction")),
                "buysmartAction": clean_text(daf_row.get("buysmartAction")),
                "dailyActionFileColumns": clean_text(daf_row.get("dailyActionFileColumns")),
                "setAction": clean_text(daf_row.get("setAction")),
                "downstreamHandling": clean_text(daf_row.get("downstreamHandling")),
                "discoveryReference": clean_text(daf_row.get("discoveryReference")),
                "notes": clean_text(daf_row.get("notes")),
                "sourceRowNumber": int(daf_row.get("sourceRowNumber") or 0),
                "fieldFilterLogic": field_filter_logic,
                "aggregateLogic": aggregate_logic,
                "logic": " => ".join(item for item in (field_filter_logic, aggregate_logic) if item),
                "compiledLogic": {
                    "compilerVersion": COMPILER_VERSION,
                    "fieldFilterLogic": field_filter_logic,
                    "aggregateLogic": aggregate_logic,
                    "predicateJson": deepcopy(predicate),
                    "actionJson": deepcopy(actions),
                    "executable": spec is not None,
                    "warnings": warnings,
                },
            }
            variants.append(
                {
                    "id": stable_id(f"variant:{rule_id}:{index}:{int(daf_row.get('sourceRowNumber') or 0)}"),
                    "rule_definition_id": definition_id,
                    "rule_version_id": version_id,
                    "rule_id": rule_id,
                    "runtime_rule_id": f"{rule_id}.{index:02d}",
                    "runtime_kind": clean_text(spec.get("runtime_kind")) if spec else runtime_kind_for(daf_row),
                    "execution_priority": rule_number(rule_id) * 100 + (index - 1),
                    "enabled": status in {"approved", "ready"},
                    "is_executable": spec is not None,
                    "stop_processing": bool(spec.get("stop_processing")) if spec else False,
                    "predicate_json": predicate,
                    "action_json": actions,
                    "description": field_filter_logic or clean_text(daf_row.get("decisionCriteria")),
                    "automation_level": level,
                    "status": status,
                    "source": source,
                    "created_at": timestamp,
                }
            )

        group_name = clean_text(first.get("ruleGroup")) or "Rule"
        notes = " | ".join(clean_text(row.get("notes")) for row in rows if clean_text(row.get("notes")))
        rules.append(
            {
                "id": definition_id,
                "rule_id": rule_id,
                "name": f"{group_name} {rule_id}",
                "rule_group": clean_text(first.get("ruleGroup")),
                "business_scope": clean_text(first.get("business")),
                "request_types": [
                    item.strip()
                    for item in clean_text(first.get("requestTypes")).split(",")
                    if item.strip()
                ],
                "discovery_reference": clean_text(first.get("discoveryReference")),
                "notes": notes,
                "owner_team": "Compliance Operations",
                "version_id": version_id,
                "version_number": 1,
                "status": "approved" if any(item["status"] == "approved" for item in variants) else "ready",
                "automation_level": aggregate_automation(variants),
                "variants": variants,
                "is_bundled": True,
                "created_at": timestamp,
                "updated_at": timestamp,
            }
        )

    report = {
        "created": len(rules),
        "updated": 0,
        "unchanged": 0,
        "warnings": workbook.get("warnings") or [],
        "duplicateRuleIds": duplicate_rule_ids,
        "sheetNames": workbook.get("sheetNames") or [],
        "executableVariants": executable_count,
        "guidedVariants": guided_count,
        "manualVariants": manual_count,
    }
    return rules, report


def clean_rule_id(value: Any) -> str:
    return re.sub(r"[^A-Z0-9_-]", "", clean_text(value).upper())


def next_user_rule_id(rules: Sequence[Mapping[str, Any]]) -> str:
    values = []
    for rule in rules:
        match = re.fullmatch(r"U(\d+)", clean_text(rule.get("rule_id")), flags=re.IGNORECASE)
        if match:
            values.append(int(match.group(1)))
    return f"U{max(values, default=0) + 1:03d}"


def next_user_priority(rules: Sequence[Mapping[str, Any]]) -> int:
    priorities = [
        int(variant.get("execution_priority") or 0) + 10
        for rule in rules
        for variant in (rule.get("variants") or [])
    ]
    return max([USER_RULE_PRIORITY_FLOOR, *priorities])


def normalize_filter_value(operator: str, value: Any) -> Any:
    if operator in NO_VALUE_OPERATORS:
        return None
    if operator in NUMERIC_OPERATORS:
        parsed = parse_number(value)
        if parsed is None:
            raise ValueError("Numeric filter operators require a numeric value.")
        return parsed
    if operator in LIST_OPERATORS:
        items = [item.strip() for item in clean_text(value).split(",") if item.strip()]
        if not items:
            raise ValueError("List filter operators require one or more comma-separated values.")
        return items
    text = clean_text(value)
    if not text:
        raise ValueError("Filter value is required.")
    if operator in {"regex", "not_regex"}:
        try:
            re.compile(text)
        except re.error as exc:
            raise ValueError(f"Invalid regular expression: {exc}") from exc
    return text


def predicate_from_filter_rows(filter_rows: Sequence[Mapping[str, Any]], join: str) -> dict[str, Any]:
    predicates: list[dict[str, Any]] = []
    for filter_row in filter_rows:
        field = clean_text(filter_row.get("field"))
        operator = clean_text(filter_row.get("op"))
        if field not in FIELD_LABELS:
            raise ValueError(f"Unsupported filter field: {field or '(blank)'}. ")
        if operator not in SUPPORTED_OPERATORS:
            raise ValueError(f"Unsupported filter operator: {operator or '(blank)'}. ")
        predicate = {"field": field, "op": operator}
        value = normalize_filter_value(operator, filter_row.get("value"))
        if value is not None:
            predicate["value"] = value
        predicates.append(predicate)
    if not predicates:
        raise ValueError("Add at least one filter.")
    if len(predicates) == 1:
        return predicates[0]
    return {"any" if join == "any" else "all": predicates}


def action_json_from_rows(action_rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    for action_row in action_rows:
        action_type = clean_text(action_row.get("type"))
        value = clean_text(action_row.get("value"))
        reason = clean_text(action_row.get("reason"))
        if not action_type:
            continue
        if action_type not in USER_ACTION_TYPES:
            raise ValueError(f"Unsupported action type: {action_type}.")
        if action_type == "exclude":
            actions.append({"type": "exclude", "reason": reason or "User-managed exclusion rule"})
        elif action_type == "set_review":
            actions.append({"type": "set_review", "value": True})
        elif action_type in {"set_action", "set_if_stock", "set_buysmart", "append_validation", "add_note"}:
            if not value:
                raise ValueError(f"{ACTION_LABELS[action_type]} requires a value.")
            actions.append({"type": action_type, "value": value})
    if not actions:
        raise ValueError("Add at least one rule action.")
    return actions


def filter_logic_text(predicate: Mapping[str, Any]) -> str:
    if isinstance(predicate.get("all"), list):
        return " AND ".join(filter_logic_text(item) for item in predicate["all"] if isinstance(item, Mapping))
    if isinstance(predicate.get("any"), list):
        return " OR ".join(filter_logic_text(item) for item in predicate["any"] if isinstance(item, Mapping))
    if isinstance(predicate.get("not"), Mapping):
        return f"NOT ({filter_logic_text(predicate['not'])})"
    field = clean_text(predicate.get("field"))
    operator = clean_text(predicate.get("op"))
    label = FIELD_LABELS.get(field, field)
    op_label = OPERATOR_LABELS.get(operator, operator)
    if operator in NO_VALUE_OPERATORS:
        return f"{label} {op_label}"
    value = predicate.get("value")
    rendered = ", ".join(clean_text(item) for item in value) if isinstance(value, list) else clean_text(value)
    return f"{label} {op_label} {rendered}"


def aggregate_logic_text(actions: Sequence[Mapping[str, Any]]) -> str:
    parts: list[str] = []
    for action in actions:
        action_type = clean_text(action.get("type"))
        value = clean_text(action.get("value"))
        if action_type == "exclude":
            parts.append(f"Exclude: {clean_text(action.get('reason')) or 'matched row'}")
        elif action_type == "set_review":
            parts.append("Flag for review")
        elif action_type == "append_validation":
            parts.append(f"Validation: {value}")
        elif action_type == "add_note":
            parts.append(f"Note: {value}")
        elif action_type == "set_action":
            parts.append(f"Set ACTION: {value}")
        elif action_type == "set_if_stock":
            parts.append(f"Set If In Stock: {value}")
        elif action_type == "set_buysmart":
            parts.append(f"Set BuySmart: {value}")
        else:
            parts.append(action_type)
    return " | ".join(parts)


def source_action_fields(actions: Sequence[Mapping[str, Any]]) -> tuple[str, str, str, str]:
    def first_value(action_type: str) -> str:
        return next(
            (clean_text(item.get("value")) for item in actions if clean_text(item.get("type")) == action_type and clean_text(item.get("value"))),
            "",
        )

    notes = "; ".join(
        clean_text(item.get("value"))
        for item in actions
        if clean_text(item.get("type")) == "add_note" and clean_text(item.get("value"))
    )
    return first_value("set_action"), first_value("set_if_stock"), first_value("set_buysmart"), notes


def user_rule_source(
    rule_id: str,
    rule_group: str,
    business_scope: str,
    request_types: Sequence[str],
    priority: int,
    notes: str,
    predicate: Mapping[str, Any],
    actions: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    filter_text = filter_logic_text(predicate)
    aggregate_text = aggregate_logic_text(actions)
    action, if_stock, buysmart, downstream_note = source_action_fields(actions)
    return {
        "ruleId": rule_id,
        "ruleGroup": rule_group,
        "business": business_scope,
        "requestTypes": ", ".join(request_types),
        "executionPriority": priority,
        "decisionCriteria": filter_text,
        "action": action,
        "ifInStockAction": if_stock,
        "buysmartAction": buysmart,
        "dailyActionFileColumns": "",
        "setAction": aggregate_text,
        "downstreamHandling": downstream_note,
        "discoveryReference": "Created in Compliance Rules",
        "notes": notes,
        "sourceRowNumber": 0,
        "fieldFilterLogic": filter_text,
        "aggregateLogic": aggregate_text,
        "logic": f"{filter_text} => {aggregate_text}",
        "compiledLogic": {
            "compilerVersion": COMPILER_VERSION,
            "fieldFilterLogic": filter_text,
            "aggregateLogic": aggregate_text,
            "predicateJson": deepcopy(dict(predicate)),
            "actionJson": deepcopy([dict(item) for item in actions]),
            "executable": True,
            "warnings": [],
        },
    }




def validate_predicate_definition(predicate: Mapping[str, Any]) -> None:
    if not isinstance(predicate, Mapping):
        raise ValueError("Predicate must be a JSON object.")
    compound_keys = [key for key in ("all", "any", "not") if key in predicate]
    if compound_keys:
        if len(compound_keys) != 1:
            raise ValueError("A compound predicate must use exactly one of all, any, or not.")
        key = compound_keys[0]
        if key == "not":
            child = predicate.get("not")
            if not isinstance(child, Mapping):
                raise ValueError("The not predicate requires one child object.")
            validate_predicate_definition(child)
            return
        children = predicate.get(key)
        if not isinstance(children, list) or not children:
            raise ValueError(f"The {key} predicate requires at least one child predicate.")
        for child in children:
            if not isinstance(child, Mapping):
                raise ValueError(f"Every {key} child must be a predicate object.")
            validate_predicate_definition(child)
        return
    field = clean_text(predicate.get("field"))
    operator = clean_text(predicate.get("op"))
    if field not in FIELD_LABELS:
        raise ValueError(f"Unsupported predicate field: {field or '(blank)' }.")
    if operator not in SUPPORTED_OPERATORS:
        raise ValueError(f"Unsupported predicate operator: {operator or '(blank)' }.")
    normalize_filter_value(operator, predicate.get("value"))


def validate_action_definition(actions: Sequence[Mapping[str, Any]]) -> None:
    if not isinstance(actions, list) or not actions:
        raise ValueError("At least one action object is required.")
    for index, node in enumerate(actions, start=1):
        if not isinstance(node, Mapping):
            raise ValueError(f"Action {index} must be a JSON object.")
        action_type = clean_text(node.get("type"))
        if action_type not in ACTION_LABELS:
            raise ValueError(f"Unsupported action type at position {index}: {action_type or '(blank)' }.")
        when = node.get("when")
        if when is not None:
            if not isinstance(when, Mapping):
                raise ValueError(f"Action {index} when clause must be a predicate object.")
            validate_predicate_definition(when)
        if action_type in {
            "set_action", "set_if_stock", "set_buysmart", "append_validation",
            "add_note", "preserve_action_set_if_stock"
        } and not clean_text(node.get("value")):
            raise ValueError(f"{ACTION_LABELS[action_type]} requires a value.")
        if action_type == "clear_field" and clean_text(node.get("field")) != "Conversion DIN":
            raise ValueError("Clear field currently supports only Conversion DIN.")

def create_user_rule(
    request: Mapping[str, Any],
    existing_rules: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    requested_id = clean_rule_id(request.get("rule_id"))
    rule_id = requested_id or next_user_rule_id(existing_rules)
    if any(clean_text(rule.get("rule_id")).lower() == rule_id.lower() for rule in existing_rules):
        raise ValueError(f"Rule {rule_id} already exists.")
    name = clean_text(request.get("name"))
    if not name:
        raise ValueError("Rule name is required.")
    priority = int(request.get("execution_priority") or next_user_priority(existing_rules))
    if priority < 1:
        raise ValueError("Rule priority must be a positive whole number.")
    predicate = deepcopy(request.get("predicate_json"))
    actions = deepcopy(request.get("action_json"))
    if not isinstance(predicate, Mapping):
        raise ValueError("A valid predicate is required.")
    if not isinstance(actions, list) or not actions:
        raise ValueError("At least one action is required.")
    validate_predicate_definition(predicate)
    validate_action_definition(actions)
    request_types = [
        item.strip()
        for item in clean_text(request.get("request_types") or "PRF, SORF, SRF").split(",")
        if item.strip()
    ]
    rule_group = clean_text(request.get("rule_group")) or "User Managed"
    business_scope = clean_text(request.get("business_scope")) or "All"
    notes = clean_text(request.get("notes"))
    enabled = bool_value(request.get("enabled", True))
    timestamp = iso_now()
    definition_id = new_id()
    version_id = new_id()
    source = user_rule_source(
        rule_id,
        rule_group,
        business_scope,
        request_types,
        priority,
        notes,
        predicate,
        actions,
    )
    variant = {
        "id": new_id(),
        "rule_definition_id": definition_id,
        "rule_version_id": version_id,
        "rule_id": rule_id,
        "runtime_rule_id": f"{rule_id}.01",
        "runtime_kind": clean_text(request.get("runtime_kind")) or "row_rule",
        "execution_priority": priority,
        "enabled": enabled,
        "is_executable": True,
        "stop_processing": bool_value(request.get("stop_processing")),
        "predicate_json": deepcopy(dict(predicate)),
        "action_json": deepcopy(actions),
        "description": filter_logic_text(predicate),
        "automation_level": "alpha",
        "status": "approved" if enabled else "disabled",
        "source": source,
        "created_at": timestamp,
    }
    return {
        "id": definition_id,
        "rule_id": rule_id,
        "name": name,
        "rule_group": rule_group,
        "business_scope": business_scope,
        "request_types": request_types,
        "discovery_reference": "Created in Compliance Rules",
        "notes": notes,
        "owner_team": "Compliance Operations",
        "version_id": version_id,
        "version_number": 1,
        "status": "approved" if enabled else "disabled",
        "automation_level": "alpha",
        "variants": [variant],
        "is_bundled": False,
        "created_at": timestamp,
        "updated_at": timestamp,
    }


def update_rule(rule: Mapping[str, Any], request: Mapping[str, Any]) -> dict[str, Any]:
    updated = deepcopy(dict(rule))
    name = clean_text(request.get("name"))
    if not name:
        raise ValueError("Rule name is required.")
    variants = updated.get("variants") or []
    if not variants:
        raise ValueError("This rule has no editable variant.")
    predicate = deepcopy(request.get("predicate_json"))
    actions = deepcopy(request.get("action_json"))
    if not isinstance(predicate, Mapping):
        raise ValueError("A valid predicate is required.")
    if not isinstance(actions, list) or not actions:
        raise ValueError("At least one action is required.")
    validate_predicate_definition(predicate)
    validate_action_definition(actions)
    priority = int(request.get("execution_priority") or variants[0].get("execution_priority") or 1)
    if priority < 1:
        raise ValueError("Rule priority must be a positive whole number.")
    enabled = bool_value(request.get("enabled", True))
    request_types = [
        item.strip()
        for item in clean_text(request.get("request_types") or ", ".join(updated.get("request_types") or [])).split(",")
        if item.strip()
    ]
    rule_group = clean_text(request.get("rule_group")) or clean_text(updated.get("rule_group")) or "User Managed"
    business_scope = clean_text(request.get("business_scope")) or clean_text(updated.get("business_scope")) or "All"
    notes = clean_text(request.get("notes"))
    new_version_id = new_id()
    updated["version_id"] = new_version_id
    updated["version_number"] = int(updated.get("version_number") or 1) + 1
    primary = variants[0]
    primary.update(
        {
            "rule_version_id": new_version_id,
            "execution_priority": priority,
            "enabled": enabled,
            "is_executable": True,
            "stop_processing": bool_value(request.get("stop_processing")),
            "predicate_json": deepcopy(dict(predicate)),
            "action_json": deepcopy(actions),
            "description": filter_logic_text(predicate),
            "automation_level": "alpha",
            "status": "approved" if enabled else "disabled",
            "runtime_kind": clean_text(request.get("runtime_kind")) or clean_text(primary.get("runtime_kind")) or "row_rule",
            "source": user_rule_source(
                clean_text(updated.get("rule_id")),
                rule_group,
                business_scope,
                request_types,
                priority,
                notes,
                predicate,
                actions,
            ),
        }
    )
    if not enabled:
        for variant in variants:
            variant["enabled"] = False
            variant["status"] = "disabled"
    updated.update(
        {
            "name": name,
            "rule_group": rule_group,
            "business_scope": business_scope,
            "request_types": request_types,
            "notes": notes,
            "status": "approved" if enabled else "disabled",
            "automation_level": "alpha",
            "updated_at": iso_now(),
        }
    )
    return updated


def set_rule_enabled(rule: Mapping[str, Any], enabled: bool) -> dict[str, Any]:
    updated = deepcopy(dict(rule))
    variants = updated.get("variants") or []
    has_executable = any(bool_value(variant.get("is_executable")) for variant in variants)
    updated["status"] = "approved" if enabled and has_executable else "ready" if enabled else "disabled"
    updated["updated_at"] = iso_now()
    for variant in variants:
        executable = bool_value(variant.get("is_executable"))
        variant["enabled"] = enabled and executable
        variant["status"] = "approved" if enabled and executable else "ready" if enabled else "disabled"
    return updated


def is_bundled_rule(rule: Mapping[str, Any]) -> bool:
    return bool_value(rule.get("is_bundled")) or bool(re.fullmatch(r"R\d+", clean_text(rule.get("rule_id")), re.I))


def predicate_is_simple(predicate: Any) -> bool:
    if not isinstance(predicate, Mapping):
        return False
    if "field" in predicate and "op" in predicate:
        return True
    for key in ("all", "any"):
        if key in predicate and isinstance(predicate[key], list):
            return all(isinstance(item, Mapping) and "field" in item and "op" in item for item in predicate[key])
    return False


def filters_from_simple_predicate(predicate: Mapping[str, Any] | None) -> tuple[list[dict[str, Any]], str]:
    if not predicate:
        return [{"field": "vendor_lc", "op": "contains", "value": ""}], "all"
    if isinstance(predicate.get("all"), list):
        items = predicate["all"]
        join = "all"
    elif isinstance(predicate.get("any"), list):
        items = predicate["any"]
        join = "any"
    else:
        items = [predicate]
        join = "all"
    rows: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, Mapping) or "field" not in item or "op" not in item:
            continue
        value = item.get("value")
        if isinstance(value, list):
            value = ", ".join(clean_text(part) for part in value)
        rows.append({"field": clean_text(item.get("field")), "op": clean_text(item.get("op")), "value": value or ""})
    return rows or [{"field": "vendor_lc", "op": "contains", "value": ""}], join


def action_rows_from_json(actions: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if isinstance(actions, list):
        for action in actions:
            if not isinstance(action, Mapping):
                continue
            action_type = clean_text(action.get("type"))
            if action_type in USER_ACTION_TYPES:
                rows.append(
                    {
                        "type": action_type,
                        "value": clean_text(action.get("value")),
                        "reason": clean_text(action.get("reason")),
                    }
                )
    return rows or [{"type": "set_buysmart", "value": "Review", "reason": ""}]


# -----------------------------------------------------------------------------
# Workbook ingestion and export (single-file, no openpyxl dependency required)
# -----------------------------------------------------------------------------


def _decode_csv_bytes(data: bytes) -> str:
    errors: list[str] = []
    for encoding in ("utf-8-sig", "utf-8", "cp1252"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError as exc:
            errors.append(f"{encoding}: {exc}")
    raise ValueError("The CSV encoding could not be read. " + " | ".join(errors))


def parse_csv_workbook(file_name: str, data: bytes) -> ParsedWorkbook:
    text = _decode_csv_bytes(data)
    sample = text[:8192]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",\t;|")
    except csv.Error:
        dialect = csv.excel
    reader = csv.reader(io.StringIO(text), dialect)
    try:
        raw_headers = next(reader)
    except StopIteration as exc:
        raise ValueError("The CSV file is empty.") from exc
    headers = [canonical_header(value) or f"Column {index + 1}" for index, value in enumerate(raw_headers)]
    rows: list[dict[str, Any]] = []
    source_row_numbers: list[int] = []
    for raw_values in reader:
        if not any(clean_text(value) for value in raw_values):
            continue
        record: dict[str, Any] = {}
        for index, header in enumerate(headers):
            value = raw_values[index] if index < len(raw_values) else ""
            if header not in record or not clean_text(record[header]):
                record[header] = value
        rows.append(record)
        source_row_numbers.append(reader.line_num)
    warnings: list[str] = []
    if not any(header.lower() == "buysmart action" for header in headers):
        warnings.append("Source workbook does not include Buysmart Action; engine output will create it.")
    return ParsedWorkbook(file_name, "CSV", headers, rows, warnings, source_row_numbers)


def _xlsx_column_index(cell_reference: str) -> int:
    letters = "".join(character for character in cell_reference if character.isalpha()).upper()
    index = 0
    for character in letters:
        index = index * 26 + (ord(character) - 64)
    return max(index - 1, 0)


def _xlsx_shared_strings(archive: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in archive.namelist():
        return []
    root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
    namespace = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    values: list[str] = []
    for item in root.findall(f"{namespace}si"):
        values.append("".join(node.text or "" for node in item.iter(f"{namespace}t")))
    return values


def _looks_like_excel_date_format(format_code: str) -> bool:
    # Remove quoted literals and escaped characters before checking date tokens.
    cleaned = re.sub(r'"[^"]*"', "", format_code.lower())
    cleaned = re.sub(r"\\.", "", cleaned)
    cleaned = re.sub(r"\[[^\]]+\]", "", cleaned)
    return bool(re.search(r"(^|[^a-z])[ymdhis]+([^a-z]|$)", cleaned))


def _xlsx_date_style_indexes(archive: zipfile.ZipFile) -> set[int]:
    if "xl/styles.xml" not in archive.namelist():
        return set()
    root = ET.fromstring(archive.read("xl/styles.xml"))
    namespace = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    custom_formats: dict[int, str] = {}
    num_fmts = root.find(f"{namespace}numFmts")
    if num_fmts is not None:
        for node in num_fmts.findall(f"{namespace}numFmt"):
            try:
                custom_formats[int(node.attrib.get("numFmtId", "0"))] = node.attrib.get("formatCode", "")
            except ValueError:
                continue
    built_in_date_ids = set(range(14, 23)) | set(range(27, 37)) | set(range(45, 48)) | {50, 51, 52, 53, 54, 55, 56, 57, 58}
    date_styles: set[int] = set()
    cell_xfs = root.find(f"{namespace}cellXfs")
    if cell_xfs is None:
        return date_styles
    for index, xf in enumerate(cell_xfs.findall(f"{namespace}xf")):
        try:
            num_fmt_id = int(xf.attrib.get("numFmtId", "0"))
        except ValueError:
            num_fmt_id = 0
        if num_fmt_id in built_in_date_ids or _looks_like_excel_date_format(custom_formats.get(num_fmt_id, "")):
            date_styles.add(index)
    return date_styles


def _excel_serial_to_datetime(value: float, date_1904: bool) -> datetime:
    base = datetime(1904, 1, 1) if date_1904 else datetime(1899, 12, 30)
    return base + timedelta(days=value)


def _xlsx_cell_value(
    cell: ET.Element,
    namespace: str,
    shared_strings: Sequence[str],
    date_styles: set[int],
    date_1904: bool,
) -> Any:
    cell_type = cell.attrib.get("t", "")
    style_index = int(cell.attrib.get("s", "0") or 0)
    if cell_type == "inlineStr":
        inline = cell.find(f"{namespace}is")
        return "" if inline is None else "".join(node.text or "" for node in inline.iter(f"{namespace}t"))
    value_node = cell.find(f"{namespace}v")
    if value_node is None or value_node.text is None:
        return ""
    raw = value_node.text
    if cell_type == "s":
        try:
            return shared_strings[int(raw)]
        except (ValueError, IndexError):
            return raw
    if cell_type == "b":
        return raw == "1"
    if cell_type in {"str", "e"}:
        return raw
    try:
        number = float(raw)
    except ValueError:
        return raw
    if style_index in date_styles:
        parsed = _excel_serial_to_datetime(number, date_1904)
        return parsed.isoformat()
    return int(number) if number.is_integer() else number


def _xlsx_sheet_path(archive: zipfile.ZipFile) -> tuple[str, str, bool]:
    workbook_root = ET.fromstring(archive.read("xl/workbook.xml"))
    main_ns = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    rel_ns = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"
    workbook_pr = workbook_root.find(f"{main_ns}workbookPr")
    date_1904 = bool(workbook_pr is not None and workbook_pr.attrib.get("date1904") in {"1", "true", "TRUE"})
    sheets = workbook_root.find(f"{main_ns}sheets")
    if sheets is None:
        raise ValueError("Source workbook has no worksheets.")
    sheet = next(iter(sheets.findall(f"{main_ns}sheet")), None)
    if sheet is None:
        raise ValueError("Source workbook has no worksheets.")
    sheet_name = sheet.attrib.get("name", "Sheet1")
    relationship_id = sheet.attrib.get(f"{rel_ns}id")
    if not relationship_id:
        raise ValueError("The first worksheet relationship is missing.")
    rels_root = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
    package_ns = "{http://schemas.openxmlformats.org/package/2006/relationships}"
    target = ""
    for relation in rels_root.findall(f"{package_ns}Relationship"):
        if relation.attrib.get("Id") == relationship_id:
            target = relation.attrib.get("Target", "")
            break
    if not target:
        raise ValueError("The first worksheet file could not be resolved.")
    target = target.lstrip("/")
    if not target.startswith("xl/"):
        target = "xl/" + target
    # Normalize ../ segments without importing platform-specific os.path behavior.
    parts: list[str] = []
    for part in target.split("/"):
        if part in {"", "."}:
            continue
        if part == "..":
            if parts:
                parts.pop()
        else:
            parts.append(part)
    return "/".join(parts), sheet_name, date_1904


def parse_xlsx_workbook(file_name: str, data: bytes) -> ParsedWorkbook:
    try:
        archive = zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile as exc:
        raise ValueError("The uploaded file is not a valid XLSX/XLSM workbook.") from exc
    with archive:
        required = {"xl/workbook.xml", "xl/_rels/workbook.xml.rels"}
        if not required.issubset(set(archive.namelist())):
            raise ValueError("The workbook is missing required Open XML files.")
        sheet_path, sheet_name, date_1904 = _xlsx_sheet_path(archive)
        if sheet_path not in archive.namelist():
            raise ValueError(f"Worksheet file {sheet_path} is missing from the workbook.")
        shared_strings = _xlsx_shared_strings(archive)
        date_styles = _xlsx_date_style_indexes(archive)
        root = ET.fromstring(archive.read(sheet_path))
        namespace = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
        sheet_data = root.find(f"{namespace}sheetData")
        if sheet_data is None:
            raise ValueError("The first worksheet has no cell data.")
        parsed_rows: list[tuple[int, dict[int, Any]]] = []
        for row_node in sheet_data.findall(f"{namespace}row"):
            try:
                row_number = int(row_node.attrib.get("r", "0"))
            except ValueError:
                row_number = len(parsed_rows) + 1
            values: dict[int, Any] = {}
            for cell in row_node.findall(f"{namespace}c"):
                reference = cell.attrib.get("r", "A1")
                values[_xlsx_column_index(reference)] = _xlsx_cell_value(
                    cell,
                    namespace,
                    shared_strings,
                    date_styles,
                    date_1904,
                )
            parsed_rows.append((row_number, values))
        if not parsed_rows:
            raise ValueError("The first worksheet is empty.")
        header_tuple = next((item for item in parsed_rows if item[0] == 1), parsed_rows[0])
        header_values = header_tuple[1]
        last_column = max(header_values, default=-1)
        if last_column < 0:
            raise ValueError("The workbook header row is empty.")
        headers = [
            canonical_header(header_values.get(index, "")) or f"Column {index + 1}"
            for index in range(last_column + 1)
        ]
        rows: list[dict[str, Any]] = []
        source_row_numbers: list[int] = []
        for row_number, values in parsed_rows:
            if row_number <= header_tuple[0]:
                continue
            record: dict[str, Any] = {}
            has_value = False
            for index, header in enumerate(headers):
                value = values.get(index, "")
                if clean_text(value):
                    has_value = True
                if header not in record or not clean_text(record[header]):
                    record[header] = value
            if has_value:
                rows.append(record)
                source_row_numbers.append(row_number)
        warnings: list[str] = []
        if not any(header.lower() == "buysmart action" for header in headers):
            warnings.append("Source workbook does not include Buysmart Action; engine output will create it.")
        return ParsedWorkbook(file_name, sheet_name, headers, rows, warnings, source_row_numbers)


def parse_source_workbook(file_name: str, data: bytes) -> ParsedWorkbook:
    extension = file_name.rsplit(".", 1)[-1].lower() if "." in file_name else ""
    if extension in {"csv", "txt", "tsv"}:
        return parse_csv_workbook(file_name, data)
    if extension in {"xlsx", "xlsm"}:
        return parse_xlsx_workbook(file_name, data)
    if extension == "xls":
        raise ValueError("Legacy .xls files are not supported. Save the workbook as .xlsx or CSV and upload it again.")
    raise ValueError("Upload a CSV, XLSX, or XLSM source file.")



def _package_version(distribution_name: str) -> str:
    try:
        return importlib_metadata.version(distribution_name)
    except Exception:
        return "not available"


_SOURCE_FINGERPRINT_CACHE: dict[str, Any] | None = None


def source_code_fingerprint() -> dict[str, Any]:
    """Return a stable fingerprint of the exact Python file executing this rerun."""
    global _SOURCE_FINGERPRINT_CACHE
    if _SOURCE_FINGERPRINT_CACHE is not None:
        return dict(_SOURCE_FINGERPRINT_CACHE)
    file_path = clean_text(globals().get("__file__"))
    result: dict[str, Any] = {
        "path": file_path,
        "file_name": file_path.rsplit("/", 1)[-1] if file_path else "",
        "sha256": "unavailable",
        "sha256_short": "unavailable",
        "size_bytes": 0,
        "deployment_sentinel": DEPLOYMENT_SENTINEL,
        "read_error": "",
    }
    try:
        if not file_path:
            raise RuntimeError("Python __file__ is unavailable in this runtime.")
        with open(file_path, "rb") as source_file:
            content = source_file.read()
        digest = hashlib.sha256(content).hexdigest()
        result.update({"sha256": digest, "sha256_short": digest[:16], "size_bytes": len(content)})
    except Exception as exc:
        result["read_error"] = f"{type(exc).__name__}: {exc}"
    _SOURCE_FINGERPRINT_CACHE = result
    return dict(result)


def runtime_environment_snapshot() -> dict[str, Any]:
    return {
        "app_version": APP_VERSION,
        "workbook_parser_version": WORKBOOK_PARSER_VERSION,
        "session_state_schema_version": SESSION_STATE_SCHEMA_VERSION,
        "deployment_sentinel": DEPLOYMENT_SENTINEL,
        "source": source_code_fingerprint(),
        "python": sys.version.split()[0],
        "python_implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "streamlit": _package_version("streamlit"),
        "pandas": _package_version("pandas"),
        "snowpark": _package_version("snowflake-snowpark-python"),
        "snowflake_connector": _package_version("snowflake-connector-python"),
    }


def _file_signature(data: bytes) -> dict[str, Any]:
    prefix = data[:16]
    if data.startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"):
        kind = "OLE Compound File"
    elif data.startswith((b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08")):
        kind = "ZIP / Open XML"
    elif data.startswith(b"\xef\xbb\xbf"):
        kind = "UTF-8 text with BOM"
    else:
        kind = "unknown"
    return {
        "kind": kind,
        "hex": prefix.hex(" "),
        "ascii": "".join(chr(value) if 32 <= value < 127 else "." for value in prefix),
    }


def _xml_local_name(tag: Any) -> str:
    return str(tag).rsplit("}", 1)[-1].rsplit(":", 1)[-1]


def _xlsx_package_diagnostics(data: bytes) -> dict[str, Any]:
    if data.startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"):
        return {
            "container": "OLE Compound File",
            "valid_open_xml": False,
            "likely_encrypted_or_legacy": True,
            "members": 0,
            "worksheets": [],
        }
    if not zipfile.is_zipfile(io.BytesIO(data)):
        return {
            "container": "not a ZIP package",
            "valid_open_xml": False,
            "likely_encrypted_or_legacy": False,
            "members": 0,
            "worksheets": [],
        }
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        names = archive.namelist()
        corrupt_member = archive.testzip()
        required = {"xl/workbook.xml", "xl/_rels/workbook.xml.rels"}
        worksheets: list[dict[str, Any]] = []
        if "xl/workbook.xml" in names:
            try:
                root = ET.fromstring(archive.read("xl/workbook.xml"))
                for node in root.iter():
                    if _xml_local_name(node.tag) != "sheet":
                        continue
                    relationship_id = ""
                    for key, value in node.attrib.items():
                        if _xml_local_name(key) == "id":
                            relationship_id = value
                            break
                    worksheets.append(
                        {
                            "name": node.attrib.get("name", ""),
                            "state": node.attrib.get("state", "visible"),
                            "relationship_id": relationship_id,
                        }
                    )
            except Exception as exc:
                worksheets.append({"metadata_error": f"{type(exc).__name__}: {exc}"})
        leaf_names = {name.lower().rsplit("/", 1)[-1] for name in names}
        return {
            "container": "ZIP / Open XML",
            "valid_open_xml": required.issubset(set(names)) and corrupt_member is None,
            "required_members_present": sorted(required & set(names)),
            "required_members_missing": sorted(required - set(names)),
            "corrupt_member": corrupt_member or "",
            "members": len(names),
            "worksheets": worksheets,
            "encrypted_members_detected": sorted({"encryptedpackage", "encryptioninfo"} & leaf_names),
        }


def _classify_workbook_exception(exc: Exception, file_name: str, data: bytes) -> dict[str, Any]:
    message = clean_text(exc)
    lowered = message.lower()
    signature = _file_signature(data)
    if data.startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"):
        return {
            "code": "OLE_OR_ENCRYPTED_WORKBOOK",
            "summary": "The file is an OLE compound document, not an unencrypted XLSX Open XML package.",
            "recommended_action": "Remove workbook password protection and save a new .xlsx copy. Legacy .xls files are not supported.",
        }
    if isinstance(exc, zipfile.BadZipFile) or "not a valid xlsx" in lowered:
        return {
            "code": "INVALID_XLSX_CONTAINER",
            "summary": "The file extension is XLSX/XLSM, but the content is not a readable ZIP/Open XML package.",
            "recommended_action": "Open the source in Excel and use Save As → Excel Workbook (.xlsx), then upload the newly saved file.",
        }
    if "missing required open xml" in lowered:
        return {
            "code": "OPEN_XML_PARTS_MISSING",
            "summary": "Required workbook XML parts are missing from the package.",
            "recommended_action": "Open and re-save the workbook in Excel to rebuild the package, or export the source as CSV.",
        }
    if isinstance(exc, ET.ParseError):
        return {
            "code": "MALFORMED_OPENXML",
            "summary": "An XML document inside the workbook is malformed or truncated.",
            "recommended_action": "Open and re-save the workbook in Excel, then upload the repaired copy.",
        }
    if "legacy .xls" in lowered:
        return {
            "code": "LEGACY_XLS_UNSUPPORTED",
            "summary": "Legacy .xls files are not supported.",
            "recommended_action": "Save the workbook as .xlsx or CSV and upload it again.",
        }
    return {
        "code": "UNCLASSIFIED_WORKBOOK_FAILURE",
        "summary": f"{type(exc).__name__}: {message}",
        "recommended_action": "Download the diagnostic JSON below and inspect the exception traceback and package preflight details.",
        "details": {"file_name": file_name, "signature": signature},
    }


def parse_source_workbook_with_diagnostics(file_name: str, data: bytes) -> dict[str, Any]:
    started = perf_counter()
    source_hash = hashlib.sha256(data).hexdigest()
    extension = file_name.rsplit(".", 1)[-1].lower() if "." in file_name else ""
    diagnostics: dict[str, Any] = {
        "diagnostic_id": f"WB-{source_hash[:12]}-{uuid.uuid4().hex[:8]}",
        "timestamp_utc": iso_now(),
        "component": "Workbook parser",
        "status": "running",
        "parser_version": WORKBOOK_PARSER_VERSION,
        "file": {
            "name": clean_text(file_name),
            "extension": extension,
            "size_bytes": len(data),
            "sha256": source_hash,
            "signature": _file_signature(data),
        },
        "environment": runtime_environment_snapshot(),
        "stages": [],
    }
    try:
        if not data:
            raise ValueError("The uploaded file contains zero bytes.")
        diagnostics["stages"].append({"stage": "upload_validation", "status": "passed", "detail": "Captured file size, signature, and SHA-256."})
        if extension in {"xlsx", "xlsm"}:
            package = _xlsx_package_diagnostics(data)
            diagnostics["package_preflight"] = package
            if package.get("likely_encrypted_or_legacy"):
                raise ValueError("The uploaded workbook is an OLE/encrypted or legacy workbook.")
            if not bool_value(package.get("valid_open_xml")):
                raise ValueError("The uploaded file is not a valid XLSX/XLSM Open XML package.")
            diagnostics["stages"].append({"stage": "open_xml_preflight", "status": "passed", "detail": f"Validated {int(package.get('members') or 0):,} package members."})
        parse_started = perf_counter()
        parsed = parse_source_workbook(file_name, data)
        diagnostics["stages"].append({
            "stage": "workbook_parse",
            "status": "passed",
            "detail": f"Parsed {len(parsed.rows):,} row(s) and {len(parsed.columns):,} column(s).",
            "elapsed_ms": round((perf_counter() - parse_started) * 1000, 2),
        })
        payload = parsed_workbook_to_payload(parsed)
        restored = parsed_workbook_from_payload(payload)
        if restored is None or restored.columns != parsed.columns or len(restored.rows) != len(parsed.rows):
            raise RuntimeError("Parsed workbook failed its Session-State-safe payload round trip.")
        diagnostics["stages"].append({"stage": "session_safe_payload", "status": "passed", "detail": "Converted parser output to plain dictionaries/lists and reconstructed it successfully."})
        diagnostics["workbook"] = {
            "sheet_name": parsed.sheet_name,
            "row_count": len(parsed.rows),
            "column_count": len(parsed.columns),
            "recognized_headers": [column for column in parsed.columns if column in EXPECTED_HEADERS],
            "unrecognized_headers": [column for column in parsed.columns if column not in EXPECTED_HEADERS],
            "warnings": list(parsed.warnings),
            "source_row_range": [min(parsed.source_row_numbers or [0]), max(parsed.source_row_numbers or [0])],
        }
        diagnostics["status"] = "success"
        diagnostics["root_cause"] = {
            "code": "NONE",
            "summary": "Workbook parsing succeeded.",
            "recommended_action": "No corrective action is required.",
        }
        diagnostics["total_elapsed_ms"] = round((perf_counter() - started) * 1000, 2)
        return {"ok": True, "workbook": payload, "diagnostics": _plain_data(diagnostics)}
    except Exception as exc:
        classification = _classify_workbook_exception(exc, file_name, data)
        diagnostics["status"] = "failed"
        diagnostics["root_cause"] = classification
        diagnostics["exception"] = {
            "type": type(exc).__name__,
            "message": clean_text(exc),
            "traceback": "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))[-24000:],
        }
        diagnostics["stages"].append({"stage": "failure", "status": "failed", "detail": classification.get("summary", "Workbook parsing failed.")})
        diagnostics["total_elapsed_ms"] = round((perf_counter() - started) * 1000, 2)
        return {"ok": False, "workbook": None, "diagnostics": _plain_data(diagnostics)}


def parse_source_workbook_for_ui(
    file_name: str,
    source_hash: str,
    data: bytes,
    retry_nonce: int = 0,
) -> dict[str, Any]:
    """
    Parse directly from the current uploader bytes.

    This boundary is intentionally not decorated with Streamlit caching and does
    not store parser objects in Session State. Every rerun validates the current
    bytes, recomputes SHA-256, and returns a plain-data payload.
    """
    actual_hash = hashlib.sha256(data).hexdigest()
    if clean_text(source_hash) != actual_hash:
        return {
            "ok": False,
            "workbook": None,
            "diagnostics": {
                "diagnostic_id": f"WB-{actual_hash[:12]}-HASH",
                "timestamp_utc": iso_now(),
                "component": "Workbook upload boundary",
                "status": "failed",
                "app_version": APP_VERSION,
                "deployment_sentinel": DEPLOYMENT_SENTINEL,
                "root_cause": {
                    "code": "UPLOAD_HASH_MISMATCH",
                    "summary": "The SHA-256 supplied to the parser does not match the current uploader bytes.",
                    "recommended_action": "Remove the upload, add the file again, and retry. The parser did not process mismatched bytes.",
                },
                "upload": {
                    "supplied_hash": clean_text(source_hash),
                    "actual_hash": actual_hash,
                    "retry_nonce": int(retry_nonce or 0),
                    "size_bytes": len(data),
                },
                "environment": runtime_environment_snapshot(),
                "stages": [],
            },
        }

    outcome = parse_source_workbook_with_diagnostics(file_name, data)
    if not isinstance(outcome, Mapping):
        raise RuntimeError(
            f"Parser contract failure: expected a mapping, received {type(outcome).__name__}."
        )

    plain_outcome = _plain_data(outcome)
    if not isinstance(plain_outcome, Mapping):
        raise RuntimeError("Parser contract failure: plain-data conversion did not return a mapping.")

    diagnostics = plain_outcome.get("diagnostics")
    if isinstance(diagnostics, MutableMapping):
        diagnostics["retry_nonce"] = int(retry_nonce or 0)
        diagnostics["app_version"] = APP_VERSION
        diagnostics["deployment_sentinel"] = DEPLOYMENT_SENTINEL
        diagnostics["source_fingerprint"] = source_code_fingerprint()

    return dict(plain_outcome)


def _xlsx_column_letter(index: int) -> str:
    value = index + 1
    letters = ""
    while value:
        value, remainder = divmod(value - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


def _raw_cell_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return timestamp_text(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (dict, list, tuple, set)):
        return json_dumps(value)
    return str(value).strip()


def _sanitize_xml_text(value: Any) -> str:
    text = _raw_cell_text(value)
    text = "".join(
        character
        for character in text
        if character in {"\t", "\n", "\r"} or ord(character) >= 32
    )
    return xml_escape(text, {'"': "&quot;"})


def _safe_sheet_name(name: str, used: set[str]) -> str:
    cleaned = re.sub(r"[\\/*?:\[\]]", "_", clean_text(name) or "Sheet")[:31]
    candidate = cleaned
    suffix = 2
    while candidate.lower() in used:
        suffix_text = f" {suffix}"
        candidate = f"{cleaned[:31-len(suffix_text)]}{suffix_text}"
        suffix += 1
    used.add(candidate.lower())
    return candidate


def _worksheet_xml(headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> str:
    row_count = len(rows) + 1
    col_count = max(len(headers), 1)
    dimension = f"A1:{_xlsx_column_letter(col_count - 1)}{max(row_count, 1)}"
    widths: list[float] = []
    for column_index, header in enumerate(headers):
        lengths = [len(_raw_cell_text(header))]
        for row in rows[:5000]:
            if column_index < len(row):
                lengths.append(max(len(line) for line in _raw_cell_text(row[column_index]).splitlines() or [""]))
        widths.append(float(min(max(max(lengths, default=10) + 2, 12), 44)))
    cols_xml = "".join(
        f'<col min="{index+1}" max="{index+1}" width="{width:.1f}" customWidth="1"/>'
        for index, width in enumerate(widths)
    )

    def cell_xml(row_number: int, column_index: int, value: Any, style: int = 0) -> str:
        reference = f"{_xlsx_column_letter(column_index)}{row_number}"
        text = _sanitize_xml_text(value)
        return (
            f'<c r="{reference}" t="inlineStr" s="{style}">'
            f'<is><t xml:space="preserve">{text}</t></is></c>'
        )

    row_xml: list[str] = []
    row_xml.append(
        '<row r="1" customHeight="1" ht="22">'
        + "".join(cell_xml(1, index, header, 1) for index, header in enumerate(headers))
        + "</row>"
    )
    for row_index, row in enumerate(rows, start=2):
        cells = "".join(
            cell_xml(row_index, column_index, value, 2 if "\n" in _raw_cell_text(value) else 0)
            for column_index, value in enumerate(row)
            if _raw_cell_text(value) or column_index < len(headers)
        )
        row_xml.append(f'<row r="{row_index}">{cells}</row>')
    auto_filter = f'<autoFilter ref="A1:{_xlsx_column_letter(col_count - 1)}{max(row_count, 1)}"/>'
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        f'<dimension ref="{dimension}"/>'
        '<sheetViews><sheetView workbookViewId="0">'
        '<pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" state="frozen"/>'
        '<selection pane="bottomLeft" activeCell="A2" sqref="A2"/>'
        '</sheetView></sheetViews>'
        '<sheetFormatPr defaultRowHeight="15"/>'
        f'<cols>{cols_xml}</cols>'
        f'<sheetData>{"".join(row_xml)}</sheetData>'
        f'{auto_filter}'
        '<pageMargins left="0.7" right="0.7" top="0.75" bottom="0.75" header="0.3" footer="0.3"/>'
        '</worksheet>'
    )


def build_xlsx(sheets: Sequence[tuple[str, Sequence[str], Sequence[Sequence[Any]]]]) -> bytes:
    if not sheets:
        sheets = [("Sheet1", ["Value"], [])]
    used_names: set[str] = set()
    normalized_sheets = [(_safe_sheet_name(name, used_names), list(headers), list(rows)) for name, headers, rows in sheets]
    content_types = [
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>',
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">',
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>',
        '<Default Extension="xml" ContentType="application/xml"/>',
        '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>',
        '<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>',
    ]
    content_types.extend(
        f'<Override PartName="/xl/worksheets/sheet{index}.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        for index in range(1, len(normalized_sheets) + 1)
    )
    content_types.append("</Types>")

    root_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
        '</Relationships>'
    )
    workbook_sheets = "".join(
        f'<sheet name="{_sanitize_xml_text(name)}" sheetId="{index}" r:id="rId{index}"/>'
        for index, (name, _, _) in enumerate(normalized_sheets, start=1)
    )
    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        f'<sheets>{workbook_sheets}</sheets>'
        '<calcPr calcId="191029" fullCalcOnLoad="1"/>'
        '</workbook>'
    )
    workbook_rels_items = [
        f'<Relationship Id="rId{index}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet{index}.xml"/>'
        for index in range(1, len(normalized_sheets) + 1)
    ]
    styles_rel_id = len(normalized_sheets) + 1
    workbook_rels_items.append(
        f'<Relationship Id="rId{styles_rel_id}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>'
    )
    workbook_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        + "".join(workbook_rels_items)
        + "</Relationships>"
    )
    styles_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<fonts count="2">'
        '<font><sz val="11"/><name val="Aptos"/><family val="2"/></font>'
        '<font><b/><sz val="11"/><name val="Aptos Display"/><family val="2"/></font>'
        '</fonts>'
        '<fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills>'
        '<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>'
        '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
        '<cellXfs count="3">'
        '<xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0" applyAlignment="1"><alignment vertical="top"/></xf>'
        '<xf numFmtId="0" fontId="1" fillId="0" borderId="0" xfId="0" applyFont="1" applyAlignment="1"><alignment vertical="center"/></xf>'
        '<xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0" applyAlignment="1"><alignment vertical="top" wrapText="1"/></xf>'
        '</cellXfs>'
        '<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>'
        '</styleSheet>'
    )

    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", "".join(content_types))
        archive.writestr("_rels/.rels", root_rels)
        archive.writestr("xl/workbook.xml", workbook_xml)
        archive.writestr("xl/_rels/workbook.xml.rels", workbook_rels)
        archive.writestr("xl/styles.xml", styles_xml)
        for index, (_, headers, rows) in enumerate(normalized_sheets, start=1):
            archive.writestr(f"xl/worksheets/sheet{index}.xml", _worksheet_xml(headers, rows))
    return output.getvalue()


def rule_priority_lookup(rules: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    lookup: dict[str, int] = {}
    for rule in rules:
        variants = rule.get("variants") or []
        priorities = [int(variant.get("execution_priority") or 0) for variant in variants if int(variant.get("execution_priority") or 0) > 0]
        if priorities:
            lookup[clean_text(rule.get("rule_id"))] = min(priorities)
        for variant in variants:
            runtime_id = clean_text(variant.get("runtime_rule_id"))
            if runtime_id:
                lookup[runtime_id] = int(variant.get("execution_priority") or 0)
    return lookup


def trace_priority(trace: Mapping[str, Any], lookup: Mapping[str, int]) -> str:
    explicit = clean_text(trace.get("executionPriority"))
    if explicit:
        return explicit
    runtime_id = clean_text(trace.get("runtimeRuleId"))
    if runtime_id and runtime_id in lookup:
        return str(lookup[runtime_id])
    rule_id = clean_text(trace.get("ruleId"))
    return str(lookup[rule_id]) if rule_id in lookup else ""


def rule_trace_detail(trace: Mapping[str, Any], lookup: Mapping[str, int]) -> str:
    resolved_priority = trace_priority(trace, lookup)
    priority = f"Priority {resolved_priority}" if resolved_priority else "No priority"
    runtime_id = first_text(trace.get("runtimeRuleId"), trace.get("ruleId"), "Unknown rule")
    runtime_kind = clean_text(trace.get("runtimeKind"))
    description = clean_text(trace.get("description"))
    action_summary = clean_text(trace.get("actionSummary"))
    result = f"{priority} {runtime_id}"
    if runtime_kind:
        result += f" {runtime_kind}"
    if description:
        result += f" - {description}"
    if action_summary:
        result += f" => {action_summary}"
    return result


def export_value(row: Mapping[str, Any], header: str, lookup: Mapping[str, int]) -> str:
    traces = [item for item in (row.get("execution_trace") or []) if isinstance(item, Mapping)]
    mapping = {
        "Business": clean_text(row.get("business")),
        "Type": clean_text(row.get("request_type")),
        "Case#": clean_text(row.get("case_number")),
        "Vendor": clean_text(row.get("vendor")),
        "DIN": clean_text(row.get("din")),
        "MIN": clean_text(row.get("min")),
        "Description": clean_text(row.get("description")),
        "ACTION": clean_text(row.get("action")),
        "If In Stock: Action": clean_text(row.get("if_in_stock_action")),
        "Buysmart Action": clean_text(row.get("buysmart_action")),
        "Assigned Bucket": bucket_for_row(row)["label"],
        "Rule Applied": clean_text(row.get("rule_applied")),
        "Applied Rule Count": str(len(traces)),
        "Applied Rule Priorities": "; ".join(filter(None, (trace_priority(trace, lookup) for trace in traces))),
        "Applied Rule Actions": "\n".join(
            f"{first_text(trace.get('runtimeRuleId'), trace.get('ruleId'))}: {first_text(trace.get('actionSummary'), 'Matched')}"
            for trace in traces
        ),
        "Applied Rule Details": "\n".join(rule_trace_detail(trace, lookup) for trace in traces),
        "Needs Review": "TRUE" if bool_value(row.get("needs_review")) else "FALSE",
        "Validation Status": clean_text(row.get("validation_status")),
        "Excluded": "TRUE" if bool_value(row.get("excluded")) else "FALSE",
        "Excluded Reason": clean_text(row.get("excluded_reason")),
        "Compliance Bucket": bucket_for_row(row)["label"],
        "Outcome Reporting": clean_text(row.get("outcome_reporting")),
        "Analyst Notes": _raw_cell_text(row.get("analyst_notes")),
    }
    return mapping.get(header, "")


def applied_rule_value(
    row: Mapping[str, Any],
    trace: Mapping[str, Any],
    trace_order: int,
    header: str,
    lookup: Mapping[str, int],
) -> str:
    mapping = {
        "Source Row": clean_text(row.get("source_row_number")),
        "Case#": clean_text(row.get("case_number")),
        "Business": clean_text(row.get("business")),
        "Type": clean_text(row.get("request_type")),
        "Vendor": clean_text(row.get("vendor")),
        "DIN": clean_text(row.get("din")),
        "MIN": clean_text(row.get("min")),
        "Description": clean_text(row.get("description")),
        "Trace Order": str(trace_order),
        "Rule Priority": trace_priority(trace, lookup),
        "Rule ID": clean_text(trace.get("ruleId")),
        "Runtime Rule ID": clean_text(trace.get("runtimeRuleId")),
        "Runtime Kind": clean_text(trace.get("runtimeKind")),
        "Rule Description": clean_text(trace.get("description")),
        "Associated Rule Action": clean_text(trace.get("actionSummary")),
        "Matched At": clean_text(trace.get("matchedAt")),
        "Final ACTION": clean_text(row.get("action")),
        "Final If In Stock: Action": clean_text(row.get("if_in_stock_action")),
        "Final Buysmart Action": clean_text(row.get("buysmart_action")),
        "Compliance Bucket": bucket_for_row(row)["label"],
        "Outcome Reporting": clean_text(row.get("outcome_reporting")),
        "Needs Review": "TRUE" if bool_value(row.get("needs_review")) else "FALSE",
        "Excluded": "TRUE" if bool_value(row.get("excluded")) else "FALSE",
        "Excluded Reason": clean_text(row.get("excluded_reason")),
    }
    return mapping.get(header, "")


def export_csv(rows: Sequence[Mapping[str, Any]], rules: Sequence[Mapping[str, Any]]) -> bytes:
    lookup = rule_priority_lookup(rules)
    output = io.StringIO(newline="")
    writer = csv.writer(output)
    writer.writerow(EXPORT_HEADERS)
    for row in rows:
        writer.writerow([export_value(row, header, lookup) for header in EXPORT_HEADERS])
    return output.getvalue().encode("utf-8-sig")


def export_xlsx(rows: Sequence[Mapping[str, Any]], rules: Sequence[Mapping[str, Any]]) -> bytes:
    lookup = rule_priority_lookup(rules)
    outcome_rows = [[export_value(row, header, lookup) for header in EXPORT_HEADERS] for row in rows]
    applied_rows: list[list[str]] = []
    for row in rows:
        traces = [item for item in (row.get("execution_trace") or []) if isinstance(item, Mapping)]
        for trace_order, trace in enumerate(traces, start=1):
            applied_rows.append(
                [applied_rule_value(row, trace, trace_order, header, lookup) for header in APPLIED_RULE_HEADERS]
            )
    return build_xlsx(
        [
            ("Outcomes", EXPORT_HEADERS, outcome_rows),
            ("Applied Rules", APPLIED_RULE_HEADERS, applied_rows),
        ]
    )



def run_application_self_check() -> dict[str, Any]:
    started = perf_counter()
    tests: list[dict[str, Any]] = []

    def run_test(name: str, callback: Any) -> None:
        test_started = perf_counter()
        try:
            detail = callback()
            tests.append({"name": name, "status": "passed", "detail": _plain_data(detail), "elapsed_ms": round((perf_counter() - test_started) * 1000, 2)})
        except Exception as exc:
            tests.append({
                "name": name,
                "status": "failed",
                "detail": f"{type(exc).__name__}: {exc}",
                "traceback": "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))[-12000:],
                "elapsed_ms": round((perf_counter() - test_started) * 1000, 2),
            })

    def csv_test() -> dict[str, Any]:
        outcome = parse_source_workbook_with_diagnostics("self_check.csv", b"Business,Type,Vendor\nCompass USA,PRF,Self Check Vendor\n")
        parsed = parsed_workbook_from_payload(outcome.get("workbook")) if bool_value(outcome.get("ok")) else None
        if parsed is None or len(parsed.rows) != 1 or parsed.columns[:3] != ["Business", "Type", "Vendor"]:
            raise AssertionError(outcome.get("diagnostics"))
        return {"rows": len(parsed.rows), "columns": parsed.columns}

    def xlsx_test() -> dict[str, Any]:
        source = build_xlsx([("Data", ["Business", "Type", "Vendor"], [["Compass USA", "PRF", "Self Check Vendor"]])])
        outcome = parse_source_workbook_with_diagnostics("self_check.xlsx", source)
        parsed = parsed_workbook_from_payload(outcome.get("workbook")) if bool_value(outcome.get("ok")) else None
        if parsed is None or len(parsed.rows) != 1 or parsed.sheet_name != "Data":
            raise AssertionError(outcome.get("diagnostics"))
        return {"sheet": parsed.sheet_name, "rows": len(parsed.rows)}

    def run_result_test() -> dict[str, Any]:
        original = RunResult({"id": "self-check"}, [{"workflow_row_id": "row-1"}], [{"id": "row-1", "action": "Approved"}], True)
        payload = run_result_to_payload(original)
        restored = run_result_from_payload(payload)
        if restored is None or not restored.dry_run or restored.rows[0].get("action") != "Approved":
            raise AssertionError("RunResult payload round trip failed.")
        return {"payload_type": payload.get("payload_type"), "dry_run": restored.dry_run}

    def catalog_test() -> dict[str, Any]:
        rules, report = build_seed_catalog()
        variants = sum(len(rule.get("variants") or []) for rule in rules)
        if len(rules) != 53 or variants != 59:
            raise AssertionError(f"Expected 53 rules and 59 variants; found {len(rules)} and {variants}.")
        return {"rules": len(rules), "variants": variants, "executable": report.get("executableVariants")}

    def source_invariants_test() -> dict[str, Any]:
        identity = source_code_fingerprint()
        read_error = clean_text(identity.get("read_error"))
        if read_error:
            return {"source_read": False, "note": read_error, "deployment_sentinel": DEPLOYMENT_SENTINEL}
        file_path = clean_text(identity.get("path"))
        with open(file_path, "r", encoding="utf-8") as source_file:
            source_text = source_file.read()
        required_markers = [
            DEPLOYMENT_SENTINEL,
            "parse_source_workbook_for_ui",
            "parsed_workbook_to_payload",
            "run_result_to_payload",
            "render_actionable_exception",
        ]
        missing = [marker for marker in required_markers if marker not in source_text]
        quote = chr(34)
        forbidden = [
            "st.session_state[" + quote + "_parsed_upload" + quote + "] = " + "parse_source_workbook",
            "isinstance(" + "parsed, " + "ParsedWorkbook)",
            "st.session_state[" + quote + "_last_execution_result" + quote + "] = " + "result",
            "st.session_state[" + quote + "_workbench_run_result" + quote + "] = " + "result",
        ]
        present_forbidden = [marker for marker in forbidden if marker in source_text]
        ddl_matches = re.findall(r"(?i)\bCREATE\s+(?:TABLE|DATABASE|SCHEMA)\b", source_text)
        parser_cache_decorators = re.findall(
            r"(?s)@st\.cache_data[^\n]*\n\s*def\s+parse_source_workbook",
            source_text,
        )
        sidebar_definitions = len(re.findall(r"(?m)^def\s+render_sidebar\(", source_text))
        legacy_generic_message = "The uploaded workbook could not be " + "parsed."
        legacy_generic_present = legacy_generic_message in source_text
        if missing or present_forbidden or ddl_matches or parser_cache_decorators or sidebar_definitions != 1 or legacy_generic_present:
            raise AssertionError({
                "missing_required_markers": missing,
                "forbidden_legacy_patterns": present_forbidden,
                "runtime_ddl_tokens": ddl_matches,
                "workbook_parser_cache_decorators": parser_cache_decorators,
                "render_sidebar_definition_count": sidebar_definitions,
                "legacy_generic_parser_message_present": legacy_generic_present,
            })
        return {
            "source_read": True,
            "sha256": identity.get("sha256"),
            "size_bytes": identity.get("size_bytes"),
            "deployment_sentinel": DEPLOYMENT_SENTINEL,
            "runtime_ddl_tokens": 0,
            "legacy_session_state_patterns": 0,
            "workbook_parser_cache_decorators": 0,
            "render_sidebar_definition_count": sidebar_definitions,
            "legacy_generic_parser_message_present": False,
        }

    run_test("CSV parser and plain-payload round trip", csv_test)
    run_test("XLSX parser and plain-payload round trip", xlsx_test)
    run_test("Run result plain Session State payload", run_result_test)
    run_test("Bundled rule catalog integrity", catalog_test)
    run_test("Deployed source safety invariants", source_invariants_test)
    failed = [test for test in tests if test.get("status") != "passed"]
    return {
        "diagnostic_id": f"SELF-{uuid.uuid4().hex[:10].upper()}",
        "timestamp_utc": iso_now(),
        "component": "Application self-check",
        "app_version": APP_VERSION,
        "status": "failed" if failed else "passed",
        "tests_passed": len(tests) - len(failed),
        "tests_failed": len(failed),
        "total_elapsed_ms": round((perf_counter() - started) * 1000, 2),
        "tests": tests,
        "environment": runtime_environment_snapshot(),
        "classification": {
            "code": "SELF_CHECK_FAILED" if failed else "SELF_CHECK_PASSED",
            "summary": "One or more internal contracts failed." if failed else "Parser, uncached upload boundary, payload, bundled catalog, and deployed source safety contracts passed.",
            "recommended_action": "Inspect the failed test traceback before using the affected workflow." if failed else "No action is required.",
        },
    }


def ensure_application_self_check(*, force: bool = False) -> dict[str, Any]:
    require_streamlit()
    existing = st.session_state.get("_application_self_check")
    if not force and isinstance(existing, Mapping) and clean_text(existing.get("app_version")) == APP_VERSION:
        return dict(existing)
    report = _plain_data(run_application_self_check())
    st.session_state["_application_self_check"] = report
    if clean_text(report.get("status")) == "failed":
        record_diagnostic_event(report)
    return report


# -----------------------------------------------------------------------------
# Snowflake persistence
# -----------------------------------------------------------------------------


def chunked(values: Sequence[Any], size: int = MAX_JSON_BATCH_ROWS) -> Iterator[list[Any]]:
    if size < 1:
        raise ValueError("Chunk size must be positive.")
    for start in range(0, len(values), size):
        yield list(values[start : start + size])


def snowflake_row_dict(row: Any) -> dict[str, Any]:
    if isinstance(row, Mapping):
        raw = dict(row)
    elif hasattr(row, "as_dict"):
        try:
            raw = row.as_dict(recursive=True)
        except TypeError:
            raw = row.as_dict()
    else:
        try:
            raw = dict(row)
        except Exception:
            return {}
    return {clean_text(key).lower(): value for key, value in raw.items()}


def timestamp_text(value: Any) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return clean_text(value)


def normalize_persisted_json(value: Any, fallback: Any) -> Any:
    if value is None:
        return deepcopy(fallback)
    if isinstance(value, (dict, list)):
        return deepcopy(value)
    return json_loads_maybe(value, deepcopy(fallback))


class SnowflakeRulesStore:
    """Thin Snowpark persistence layer bound to an explicit Snowflake schema."""

    @staticmethod
    def _identifier(value: Any, label: str) -> str:
        identifier = clean_text(value).upper()
        if not re.fullmatch(r"[A-Z][A-Z0-9_$]*", identifier):
            raise ValueError(f"{label} must be a valid Snowflake identifier.")
        return identifier

    @staticmethod
    def _quoted(identifier: str) -> str:
        return f'"{identifier}"'

    def __init__(
        self,
        session: Any,
        table_prefix: str = TABLE_PREFIX,
        *,
        database: str = TARGET_DATABASE,
        schema: str = TARGET_SCHEMA,
    ):
        self.session = session
        self.prefix = self._identifier(table_prefix, "TABLE_PREFIX")
        self.database = self._identifier(database, "TARGET_DATABASE")
        self.schema = self._identifier(schema, "TARGET_SCHEMA")
        self.schema_name = f"{self._quoted(self.database)}.{self._quoted(self.schema)}"
        self.tables = {
            key: (
                f"{self.schema_name}."
                f"{self._quoted(f'{self.prefix}_{suffix}')}"
            )
            for key, suffix in TABLE_SUFFIXES.items()
        }

    def table(self, key: str) -> str:
        if key not in self.tables:
            raise KeyError(f"Unknown table key: {key}")
        return self.tables[key]

    def collect(self, query: str, params: Sequence[Any] | None = None) -> list[Any]:
        if params is None:
            return list(self.session.sql(query).collect())
        return list(self.session.sql(query, params=list(params)).collect())

    def execute(self, query: str, params: Sequence[Any] | None = None) -> None:
        self.collect(query, params)

    def scalar(self, query: str, params: Sequence[Any] | None = None, default: Any = None) -> Any:
        rows = self.collect(query, params)
        if not rows:
            return default
        data = snowflake_row_dict(rows[0])
        return next(iter(data.values()), default)

    @contextmanager
    def transaction(self) -> Iterator[None]:
        self.execute("BEGIN")
        try:
            yield
        except Exception:
            try:
                self.execute("ROLLBACK")
            finally:
                raise
        else:
            self.execute("COMMIT")

    def verify_backend(self) -> dict[str, str]:
        """Verify that every separately provisioned backend table is readable.

        This method intentionally performs no DDL. Streamlit apps created in a
        Workspace can have a personal database as their current context; all
        persistence SQL therefore uses explicit three-part object names.
        """
        failures: list[str] = []
        for key, table_name in self.tables.items():
            try:
                self.collect(f"SELECT 1 AS READY FROM {table_name} LIMIT 0")
            except Exception as exc:
                failures.append(f"{key}: {table_name} ({clean_text(exc)})")
        if failures:
            detail = "\n".join(f"- {failure}" for failure in failures)
            raise RuntimeError(
                "The Compliance Rules backend is missing or inaccessible. "
                f"Run compliance_rules_backend.sql in {self.database}.{self.schema} "
                f"and grant the Streamlit owner role {TARGET_ROLE} DML access.\n{detail}"
            )
        return dict(self.tables)

    def bootstrap(self) -> None:
        """Backward-compatible alias that now verifies rather than creates tables."""
        self.verify_backend()

    def context(self) -> dict[str, Any]:
        """Return runtime context while identifying the actual persistence target."""
        runtime: dict[str, Any] = {}
        try:
            query = """
                SELECT
                    CURRENT_USER() AS USER_NAME,
                    CURRENT_ROLE() AS ROLE_NAME,
                    CURRENT_DATABASE() AS DATABASE_NAME,
                    CURRENT_SCHEMA() AS SCHEMA_NAME,
                    CURRENT_WAREHOUSE() AS WAREHOUSE_NAME,
                    CURRENT_VERSION() AS SNOWFLAKE_VERSION
            """
            rows = self.collect(query)
            runtime = snowflake_row_dict(rows[0]) if rows else {}
        except Exception as exc:
            runtime = {"context_error": clean_text(exc)}

        # The Streamlit object's session database can be a Workspace personal
        # database. Surface both contexts, but make DATABASE_NAME/SCHEMA_NAME the
        # fully qualified persistence namespace used by every application query.
        return {
            "user_name": runtime.get("user_name") or self.viewer_user(),
            "role_name": runtime.get("role_name") or TARGET_ROLE,
            "database_name": self.database,
            "schema_name": self.schema,
            "warehouse_name": runtime.get("warehouse_name") or TARGET_WAREHOUSE,
            "snowflake_version": runtime.get("snowflake_version"),
            "session_database_name": runtime.get("database_name"),
            "session_schema_name": runtime.get("schema_name"),
            "context_error": runtime.get("context_error"),
        }

    @staticmethod
    def viewer_user() -> str:
        """Return the signed-in Streamlit viewer when the runtime exposes it."""
        if st is None:
            return ""
        try:
            user = getattr(st, "user", None)
            for attribute in ("user_name", "email", "name"):
                value = clean_text(getattr(user, attribute, ""))
                if value:
                    return value
        except Exception:
            pass
        return ""

    def current_user(self) -> str:
        viewer = self.viewer_user()
        if viewer:
            return viewer
        try:
            return clean_text(self.context().get("user_name")) or "UNKNOWN_USER"
        except Exception:
            return "UNKNOWN_USER"

    def health(self) -> dict[str, Any]:
        context = self.context()
        counts: dict[str, int] = {}
        for key, table_name in self.tables.items():
            try:
                counts[key] = int(self.scalar(f"SELECT COUNT(*) AS VALUE FROM {table_name}", default=0) or 0)
            except Exception:
                counts[key] = -1
        return {
            "context": context,
            "target": {
                "role": TARGET_ROLE,
                "warehouse": TARGET_WAREHOUSE,
                "database": self.database,
                "schema": self.schema,
                "tables": dict(self.tables),
            },
            "counts": counts,
            "app_version": APP_VERSION,
        }

    def upsert_rules(self, rule_values: Sequence[Mapping[str, Any]]) -> None:
        if not rule_values:
            return
        table_name = self.table("rules")
        for batch in chunked(list(rule_values)):
            payload = json_dumps(batch)
            query = f"""
                MERGE INTO {table_name} AS target
                USING (
                    SELECT
                        value:"id"::VARCHAR AS ID,
                        value:"rule_id"::VARCHAR AS RULE_ID,
                        value:"name"::VARCHAR AS NAME,
                        value:"rule_group"::VARCHAR AS RULE_GROUP,
                        value:"status"::VARCHAR AS STATUS,
                        value:"automation_level"::VARCHAR AS AUTOMATION_LEVEL,
                        COALESCE(
                            value:"variants"[0]:"execution_priority"::NUMBER,
                            0
                        ) AS EXECUTION_PRIORITY,
                        COALESCE(value:"is_bundled"::BOOLEAN, FALSE) AS IS_BUNDLED,
                        COALESCE(
                            TRY_TO_TIMESTAMP_TZ(value:"updated_at"::VARCHAR),
                            CURRENT_TIMESTAMP()
                        ) AS UPDATED_AT,
                        value AS RULE_JSON
                    FROM TABLE(FLATTEN(INPUT => PARSE_JSON(?)))
                ) AS source
                ON target.RULE_ID = source.RULE_ID
                WHEN MATCHED THEN UPDATE SET
                    ID = source.ID,
                    NAME = source.NAME,
                    RULE_GROUP = source.RULE_GROUP,
                    STATUS = source.STATUS,
                    AUTOMATION_LEVEL = source.AUTOMATION_LEVEL,
                    EXECUTION_PRIORITY = source.EXECUTION_PRIORITY,
                    IS_BUNDLED = source.IS_BUNDLED,
                    UPDATED_AT = source.UPDATED_AT,
                    RULE_JSON = source.RULE_JSON
                WHEN NOT MATCHED THEN INSERT (
                    ID, RULE_ID, NAME, RULE_GROUP, STATUS, AUTOMATION_LEVEL,
                    EXECUTION_PRIORITY, IS_BUNDLED, UPDATED_AT, RULE_JSON
                ) VALUES (
                    source.ID, source.RULE_ID, source.NAME, source.RULE_GROUP,
                    source.STATUS, source.AUTOMATION_LEVEL, source.EXECUTION_PRIORITY,
                    source.IS_BUNDLED, source.UPDATED_AT, source.RULE_JSON
                )
            """
            self.execute(query, [payload])

    def load_rules(self) -> list[dict[str, Any]]:
        query = f"""
            SELECT TO_JSON(RULE_JSON) AS RULE_JSON
            FROM {self.table('rules')}
            ORDER BY EXECUTION_PRIORITY, RULE_ID
        """
        output: list[dict[str, Any]] = []
        for source_row in self.collect(query):
            data = snowflake_row_dict(source_row)
            rule = normalize_persisted_json(data.get("rule_json"), {})
            if isinstance(rule, dict) and rule:
                output.append(rule)
        return output

    def get_rule(self, rule_id: str) -> dict[str, Any] | None:
        query = f"""
            SELECT TO_JSON(RULE_JSON) AS RULE_JSON
            FROM {self.table('rules')}
            WHERE UPPER(RULE_ID) = UPPER(?)
            LIMIT 1
        """
        rows = self.collect(query, [rule_id])
        if not rows:
            return None
        parsed = normalize_persisted_json(snowflake_row_dict(rows[0]).get("rule_json"), {})
        return parsed if isinstance(parsed, dict) else None

    def delete_user_rule(self, rule_id: str) -> bool:
        existing = self.get_rule(rule_id)
        if not existing:
            return False
        if is_bundled_rule(existing):
            raise ValueError("Bundled DAF rules cannot be deleted. Disable or restore them instead.")
        with self.transaction():
            self.execute(
                f"DELETE FROM {self.table('rules')} WHERE UPPER(RULE_ID) = UPPER(?) AND COALESCE(IS_BUNDLED, FALSE) = FALSE",
                [rule_id],
            )
            self.log_event(
                entity_type="rule",
                entity_id=clean_text(existing.get("id")),
                action="delete_rule",
                before=existing,
                details={"rule_id": clean_text(existing.get("rule_id"))},
            )
        return True

    def seed_bundled_rules(self, force: bool = False) -> dict[str, Any]:
        seeds, report = build_seed_catalog()
        existing = {clean_text(rule.get("rule_id")).upper(): rule for rule in self.load_rules()}
        to_write = seeds if force else [rule for rule in seeds if clean_text(rule.get("rule_id")).upper() not in existing]
        report = deepcopy(report)
        report["created"] = len([rule for rule in to_write if clean_text(rule.get("rule_id")).upper() not in existing])
        report["updated"] = len(to_write) - int(report["created"])
        report["unchanged"] = len(seeds) - len(to_write)
        if force or to_write:
            with self.transaction():
                if to_write:
                    self.upsert_rules(to_write)
                self.log_event(
                    entity_type="rule_catalog",
                    entity_id="bundled-daf",
                    action="restore_bundled_rules" if force else "seed_bundled_rules",
                    details=report,
                )
        return report

    def restore_bundled_rule(self, rule_id: str) -> dict[str, Any]:
        seeds, _ = build_seed_catalog()
        match = next((rule for rule in seeds if clean_text(rule.get("rule_id")).upper() == clean_text(rule_id).upper()), None)
        if match is None:
            raise ValueError(f"Bundled rule {rule_id} was not found in the embedded DAF catalog.")
        with self.transaction():
            self.upsert_rules([match])
            self.log_event(
                entity_type="rule",
                entity_id=clean_text(match.get("id")),
                action="restore_bundled_rule",
                after=match,
                details={"rule_id": clean_text(match.get("rule_id"))},
            )
        return match

    def seed_reference_lists(self) -> int:
        existing = self.load_reference_lists(include_defaults=False)
        inserted = 0
        for name, values in DEFAULT_REFERENCE_LISTS.items():
            if name not in existing:
                self.replace_reference_list(name, values, notes="Bundled default reference values")
                inserted += len(values)
        return inserted

    def load_reference_lists(self, include_defaults: bool = True) -> dict[str, list[str]]:
        query = f"""
            SELECT LIST_NAME, VALUE
            FROM {self.table('references')}
            WHERE COALESCE(ACTIVE, TRUE) = TRUE
            ORDER BY LIST_NAME, VALUE
        """
        output: dict[str, list[str]] = {}
        for source_row in self.collect(query):
            data = snowflake_row_dict(source_row)
            name = clean_text(data.get("list_name"))
            value = clean_text(data.get("value"))
            if name and value:
                output.setdefault(name, []).append(value)
        if include_defaults:
            for name, values in DEFAULT_REFERENCE_LISTS.items():
                output.setdefault(name, list(values))
        return output

    def replace_reference_list(self, list_name: str, values: Sequence[Any], notes: str = "") -> None:
        name = clean_text(list_name)
        if not name:
            raise ValueError("Reference list name is required.")
        normalized: list[str] = []
        seen: set[str] = set()
        for raw in values:
            value = clean_text(raw)
            key = value.lower()
            if value and key not in seen:
                normalized.append(value)
                seen.add(key)
        table_name = self.table("references")
        with self.transaction():
            self.execute(f"DELETE FROM {table_name} WHERE LIST_NAME = ?", [name])
            if normalized:
                payload = json_dumps(
                    [
                        {"list_name": name, "value": value, "notes": clean_text(notes), "updated_at": iso_now()}
                        for value in normalized
                    ]
                )
                query = f"""
                    INSERT INTO {table_name} (LIST_NAME, VALUE, ACTIVE, NOTES, UPDATED_AT)
                    SELECT
                        value:"list_name"::VARCHAR,
                        value:"value"::VARCHAR,
                        TRUE,
                        value:"notes"::VARCHAR,
                        COALESCE(TRY_TO_TIMESTAMP_TZ(value:"updated_at"::VARCHAR), CURRENT_TIMESTAMP())
                    FROM TABLE(FLATTEN(INPUT => PARSE_JSON(?)))
                """
                self.execute(query, [payload])
            self.log_event(
                entity_type="reference_list",
                entity_id=name,
                action="replace_reference_list",
                after={"name": name, "values": normalized},
                details={"value_count": len(normalized), "notes": clean_text(notes)},
            )

    def find_batch_by_hash(self, file_sha256: str) -> dict[str, Any] | None:
        file_hash = clean_text(file_sha256)
        if not file_hash:
            return None
        query = f"""
            SELECT
                ID, NAME, SOURCE_KIND, REPORTING_DATE, STATUS, ROW_COUNT,
                SOURCE_FILE_NAME, SOURCE_SHEET_NAME, FILE_SHA256,
                TO_JSON(WARNINGS) AS WARNINGS_JSON,
                TO_JSON(METADATA) AS METADATA_JSON,
                ARCHIVED, CREATED_AT, UPDATED_AT
            FROM {self.table('batches')}
            WHERE FILE_SHA256 = ?
            ORDER BY CREATED_AT DESC
            LIMIT 1
        """
        rows = self.collect(query, [file_hash])
        return self._batch_from_row(rows[0]) if rows else None

    def _batch_from_row(self, source_row: Any) -> dict[str, Any]:
        data = snowflake_row_dict(source_row)
        return {
            "id": clean_text(data.get("id")),
            "name": clean_text(data.get("name")),
            "source_kind": clean_text(data.get("source_kind")),
            "reporting_date": timestamp_text(data.get("reporting_date")),
            "status": clean_text(data.get("status")),
            "row_count": int(data.get("row_count") or 0),
            "source_file_name": clean_text(data.get("source_file_name")),
            "source_sheet_name": clean_text(data.get("source_sheet_name")),
            "file_sha256": clean_text(data.get("file_sha256")),
            "warnings": normalize_persisted_json(data.get("warnings_json"), []),
            "metadata": normalize_persisted_json(data.get("metadata_json"), {}),
            "archived": bool_value(data.get("archived")),
            "created_at": timestamp_text(data.get("created_at")),
            "updated_at": timestamp_text(data.get("updated_at")),
        }

    def list_batches(self, include_archived: bool = False, limit: int = 250) -> list[dict[str, Any]]:
        where = "" if include_archived else "WHERE COALESCE(ARCHIVED, FALSE) = FALSE"
        safe_limit = max(1, min(int(limit), 5000))
        query = f"""
            SELECT
                ID, NAME, SOURCE_KIND, REPORTING_DATE, STATUS, ROW_COUNT,
                SOURCE_FILE_NAME, SOURCE_SHEET_NAME, FILE_SHA256,
                TO_JSON(WARNINGS) AS WARNINGS_JSON,
                TO_JSON(METADATA) AS METADATA_JSON,
                ARCHIVED, CREATED_AT, UPDATED_AT
            FROM {self.table('batches')}
            {where}
            ORDER BY CREATED_AT DESC
            LIMIT {safe_limit}
        """
        return [self._batch_from_row(row) for row in self.collect(query)]

    def get_batch(self, batch_id: str) -> dict[str, Any] | None:
        query = f"""
            SELECT
                ID, NAME, SOURCE_KIND, REPORTING_DATE, STATUS, ROW_COUNT,
                SOURCE_FILE_NAME, SOURCE_SHEET_NAME, FILE_SHA256,
                TO_JSON(WARNINGS) AS WARNINGS_JSON,
                TO_JSON(METADATA) AS METADATA_JSON,
                ARCHIVED, CREATED_AT, UPDATED_AT
            FROM {self.table('batches')}
            WHERE ID = ?
            LIMIT 1
        """
        rows = self.collect(query, [batch_id])
        return self._batch_from_row(rows[0]) if rows else None

    def create_batch(
        self,
        parsed: ParsedWorkbook,
        source_bytes: bytes,
        batch_name: str = "",
        reporting_date: date | str | None = None,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        if not parsed.rows:
            raise ValueError("The source workbook contains no data rows.")
        batch_id = new_id()
        timestamp = iso_now()
        extension = parsed.file_name.rsplit(".", 1)[-1].upper() if "." in parsed.file_name else "FILE"
        if isinstance(reporting_date, datetime):
            reporting_value = reporting_date.date().isoformat()
        elif isinstance(reporting_date, date):
            reporting_value = reporting_date.isoformat()
        elif reporting_date:
            reporting_value = normalize_date(reporting_date)[:10]
        else:
            reporting_value = ""
        batch = {
            "id": batch_id,
            "name": clean_text(batch_name) or f"{parsed.file_name} · {timestamp[:10]}",
            "source_kind": extension,
            "reporting_date": reporting_value,
            "status": "Uploaded",
            "row_count": len(parsed.rows),
            "source_file_name": parsed.file_name,
            "source_sheet_name": parsed.sheet_name,
            "file_sha256": hashlib.sha256(source_bytes).hexdigest(),
            "warnings": list(parsed.warnings),
            "metadata": {
                "columns": list(parsed.columns),
                "ingested_by": self.current_user(),
                "app_version": APP_VERSION,
            },
            "archived": False,
            "created_at": timestamp,
            "updated_at": timestamp,
        }
        source_numbers = parsed.source_row_numbers or list(range(2, len(parsed.rows) + 2))
        workflow_rows = [
            create_workflow_row(
                batch_id,
                raw_row,
                source_row_number=source_numbers[index] if index < len(source_numbers) else index + 2,
                now=timestamp,
            )
            for index, raw_row in enumerate(parsed.rows)
        ]
        query = f"""
            INSERT INTO {self.table('batches')} (
                ID, NAME, SOURCE_KIND, REPORTING_DATE, STATUS, ROW_COUNT,
                SOURCE_FILE_NAME, SOURCE_SHEET_NAME, FILE_SHA256, WARNINGS,
                METADATA, ARCHIVED, CREATED_AT, UPDATED_AT
            )
            SELECT
                ?, ?, ?, TRY_TO_DATE(NULLIF(?, '')), ?, ?, ?, ?, ?,
                PARSE_JSON(?), PARSE_JSON(?), FALSE,
                TRY_TO_TIMESTAMP_TZ(?), TRY_TO_TIMESTAMP_TZ(?)
        """
        params = [
            batch["id"],
            batch["name"],
            batch["source_kind"],
            batch["reporting_date"],
            batch["status"],
            batch["row_count"],
            batch["source_file_name"],
            batch["source_sheet_name"],
            batch["file_sha256"],
            json_dumps(batch["warnings"]),
            json_dumps(batch["metadata"]),
            batch["created_at"],
            batch["updated_at"],
        ]
        with self.transaction():
            self.execute(query, params)
            self.upsert_rows(workflow_rows)
            self.log_event(
                entity_type="batch",
                entity_id=batch_id,
                batch_id=batch_id,
                action="ingest_workbook",
                after=batch,
                details={
                    "source_file_name": parsed.file_name,
                    "source_sheet_name": parsed.sheet_name,
                    "row_count": len(workflow_rows),
                    "file_sha256": batch["file_sha256"],
                },
            )
        return batch, workflow_rows

    def update_batch_status(self, batch_id: str, status: str, row_count: int | None = None) -> None:
        if row_count is None:
            self.execute(
                f"UPDATE {self.table('batches')} SET STATUS = ?, UPDATED_AT = CURRENT_TIMESTAMP() WHERE ID = ?",
                [clean_text(status), batch_id],
            )
        else:
            self.execute(
                f"UPDATE {self.table('batches')} SET STATUS = ?, ROW_COUNT = ?, UPDATED_AT = CURRENT_TIMESTAMP() WHERE ID = ?",
                [clean_text(status), int(row_count), batch_id],
            )

    def archive_batch(self, batch_id: str, archived: bool = True) -> None:
        with self.transaction():
            self.execute(
                f"UPDATE {self.table('batches')} SET ARCHIVED = ?, UPDATED_AT = CURRENT_TIMESTAMP() WHERE ID = ?",
                [bool(archived), batch_id],
            )
            self.log_event(
                entity_type="batch",
                entity_id=batch_id,
                batch_id=batch_id,
                action="archive_batch" if archived else "restore_batch",
                details={"archived": bool(archived)},
            )

    def delete_batch(self, batch_id: str) -> None:
        # Deliberately explicit and transactional because this is destructive.
        with self.transaction():
            self.execute(
                f"DELETE FROM {self.table('results')} WHERE BATCH_ID = ?",
                [batch_id],
            )
            self.execute(
                f"DELETE FROM {self.table('runs')} WHERE BATCH_ID = ?",
                [batch_id],
            )
            self.execute(
                f"DELETE FROM {self.table('rows')} WHERE BATCH_ID = ?",
                [batch_id],
            )
            self.execute(
                f"DELETE FROM {self.table('batches')} WHERE ID = ?",
                [batch_id],
            )
            self.log_event(
                entity_type="batch",
                entity_id=batch_id,
                batch_id=batch_id,
                action="delete_batch",
                details={"destructive": True},
            )

    def upsert_rows(self, row_values: Sequence[Mapping[str, Any]]) -> None:
        if not row_values:
            return
        table_name = self.table("rows")
        for batch in chunked(list(row_values)):
            payload = json_dumps(batch)
            query = f"""
                MERGE INTO {table_name} AS target
                USING (
                    SELECT
                        value:"id"::VARCHAR AS ID,
                        value:"batch_id"::VARCHAR AS BATCH_ID,
                        value:"source_row_number"::NUMBER AS SOURCE_ROW_NUMBER,
                        value:"business"::VARCHAR AS BUSINESS,
                        value:"request_type"::VARCHAR AS REQUEST_TYPE,
                        value:"case_number"::VARCHAR AS CASE_NUMBER,
                        value:"vendor"::VARCHAR AS VENDOR,
                        value:"din"::VARCHAR AS DIN,
                        value:"min"::VARCHAR AS MIN,
                        value:"description"::VARCHAR AS DESCRIPTION,
                        value:"action"::VARCHAR AS ACTION,
                        value:"if_in_stock_action"::VARCHAR AS IF_IN_STOCK_ACTION,
                        value:"buysmart_action"::VARCHAR AS BUYSMART_ACTION,
                        value:"rule_applied"::VARCHAR AS RULE_APPLIED,
                        COALESCE(value:"needs_review"::BOOLEAN, FALSE) AS NEEDS_REVIEW,
                        value:"validation_status"::VARCHAR AS VALIDATION_STATUS,
                        COALESCE(value:"excluded"::BOOLEAN, FALSE) AS EXCLUDED,
                        value:"queue_bucket"::VARCHAR AS QUEUE_BUCKET,
                        value:"outcome_reporting"::VARCHAR AS OUTCOME_REPORTING,
                        value:"status"::VARCHAR AS STATUS,
                        COALESCE(
                            TRY_TO_TIMESTAMP_TZ(value:"updated_at"::VARCHAR),
                            CURRENT_TIMESTAMP()
                        ) AS UPDATED_AT,
                        value AS ROW_JSON
                    FROM TABLE(FLATTEN(INPUT => PARSE_JSON(?)))
                ) AS source
                ON target.ID = source.ID
                WHEN MATCHED THEN UPDATE SET
                    BATCH_ID = source.BATCH_ID,
                    SOURCE_ROW_NUMBER = source.SOURCE_ROW_NUMBER,
                    BUSINESS = source.BUSINESS,
                    REQUEST_TYPE = source.REQUEST_TYPE,
                    CASE_NUMBER = source.CASE_NUMBER,
                    VENDOR = source.VENDOR,
                    DIN = source.DIN,
                    MIN = source.MIN,
                    DESCRIPTION = source.DESCRIPTION,
                    ACTION = source.ACTION,
                    IF_IN_STOCK_ACTION = source.IF_IN_STOCK_ACTION,
                    BUYSMART_ACTION = source.BUYSMART_ACTION,
                    RULE_APPLIED = source.RULE_APPLIED,
                    NEEDS_REVIEW = source.NEEDS_REVIEW,
                    VALIDATION_STATUS = source.VALIDATION_STATUS,
                    EXCLUDED = source.EXCLUDED,
                    QUEUE_BUCKET = source.QUEUE_BUCKET,
                    OUTCOME_REPORTING = source.OUTCOME_REPORTING,
                    STATUS = source.STATUS,
                    UPDATED_AT = source.UPDATED_AT,
                    ROW_JSON = source.ROW_JSON
                WHEN NOT MATCHED THEN INSERT (
                    ID, BATCH_ID, SOURCE_ROW_NUMBER, BUSINESS, REQUEST_TYPE,
                    CASE_NUMBER, VENDOR, DIN, MIN, DESCRIPTION, ACTION,
                    IF_IN_STOCK_ACTION, BUYSMART_ACTION, RULE_APPLIED,
                    NEEDS_REVIEW, VALIDATION_STATUS, EXCLUDED, QUEUE_BUCKET,
                    OUTCOME_REPORTING, STATUS, UPDATED_AT, ROW_JSON
                ) VALUES (
                    source.ID, source.BATCH_ID, source.SOURCE_ROW_NUMBER,
                    source.BUSINESS, source.REQUEST_TYPE, source.CASE_NUMBER,
                    source.VENDOR, source.DIN, source.MIN, source.DESCRIPTION,
                    source.ACTION, source.IF_IN_STOCK_ACTION,
                    source.BUYSMART_ACTION, source.RULE_APPLIED,
                    source.NEEDS_REVIEW, source.VALIDATION_STATUS,
                    source.EXCLUDED, source.QUEUE_BUCKET,
                    source.OUTCOME_REPORTING, source.STATUS,
                    source.UPDATED_AT, source.ROW_JSON
                )
            """
            self.execute(query, [payload])

    def _row_from_result(self, source_row: Any) -> dict[str, Any]:
        data = snowflake_row_dict(source_row)
        parsed = normalize_persisted_json(data.get("row_json"), {})
        return parsed if isinstance(parsed, dict) else {}

    def row_count(self, batch_id: str, filters: Mapping[str, Any] | None = None) -> int:
        where, params = self._row_filter_sql(batch_id, filters)
        value = self.scalar(
            f"SELECT COUNT(*) AS VALUE FROM {self.table('rows')} {where}",
            params,
            0,
        )
        return int(value or 0)

    def _row_filter_sql(
        self,
        batch_id: str,
        filters: Mapping[str, Any] | None = None,
    ) -> tuple[str, list[Any]]:
        filters = filters or {}
        clauses = ["BATCH_ID = ?"]
        params: list[Any] = [batch_id]
        search = clean_text(filters.get("search"))
        if search:
            clauses.append(
                "(BUSINESS ILIKE ? OR REQUEST_TYPE ILIKE ? OR CASE_NUMBER ILIKE ? OR "
                "VENDOR ILIKE ? OR DIN ILIKE ? OR MIN ILIKE ? OR DESCRIPTION ILIKE ? OR RULE_APPLIED ILIKE ?)"
            )
            pattern = f"%{search}%"
            params.extend([pattern] * 8)
        column_filters = {
            "business": "BUSINESS",
            "request_type": "REQUEST_TYPE",
            "status": "STATUS",
            "queue_bucket": "QUEUE_BUCKET",
            "outcome_reporting": "OUTCOME_REPORTING",
            "action": "ACTION",
        }
        for key, column in column_filters.items():
            value = clean_text(filters.get(key))
            if value and value.lower() not in {"all", "(all)"}:
                clauses.append(f"{column} = ?")
                params.append(value)
        for key, column in (("needs_review", "NEEDS_REVIEW"), ("excluded", "EXCLUDED")):
            value = filters.get(key)
            if value is not None and clean_text(value).lower() not in {"", "all", "(all)"}:
                clauses.append(f"COALESCE({column}, FALSE) = ?")
                params.append(bool_value(value))
        return "WHERE " + " AND ".join(clauses), params

    def load_rows(
        self,
        batch_id: str,
        filters: Mapping[str, Any] | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        where, params = self._row_filter_sql(batch_id, filters)
        limit_sql = ""
        if limit is not None:
            safe_limit = max(1, min(int(limit), 100_000))
            safe_offset = max(0, int(offset))
            limit_sql = f" LIMIT {safe_limit} OFFSET {safe_offset}"
        query = f"""
            SELECT TO_JSON(ROW_JSON) AS ROW_JSON
            FROM {self.table('rows')}
            {where}
            ORDER BY SOURCE_ROW_NUMBER, ID
            {limit_sql}
        """
        return [self._row_from_result(row) for row in self.collect(query, params)]

    def get_row(self, row_id: str) -> dict[str, Any] | None:
        query = f"""
            SELECT TO_JSON(ROW_JSON) AS ROW_JSON
            FROM {self.table('rows')}
            WHERE ID = ?
            LIMIT 1
        """
        rows = self.collect(query, [row_id])
        return self._row_from_result(rows[0]) if rows else None

    def distinct_row_values(self, batch_id: str, field: str, limit: int = 500) -> list[str]:
        columns = {
            "business": "BUSINESS",
            "request_type": "REQUEST_TYPE",
            "status": "STATUS",
            "queue_bucket": "QUEUE_BUCKET",
            "outcome_reporting": "OUTCOME_REPORTING",
            "action": "ACTION",
        }
        column = columns.get(field)
        if not column:
            raise ValueError(f"Unsupported distinct row field: {field}")
        safe_limit = max(1, min(int(limit), 5000))
        query = f"""
            SELECT DISTINCT {column} AS VALUE
            FROM {self.table('rows')}
            WHERE BATCH_ID = ? AND NULLIF(TRIM({column}), '') IS NOT NULL
            ORDER BY VALUE
            LIMIT {safe_limit}
        """
        return [clean_text(snowflake_row_dict(row).get("value")) for row in self.collect(query, [batch_id])]

    def save_row_override(
        self,
        row: Mapping[str, Any],
        before: Mapping[str, Any] | None = None,
        changed_fields: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        updated = deepcopy(dict(row))
        updated["updated_at"] = iso_now()
        updated["last_saved_at"] = updated["updated_at"]
        updated["status"] = "Excluded" if bool_value(updated.get("excluded")) else "Review" if bool_value(updated.get("needs_review")) else "Ready"
        updated["outcome_reporting"] = classify_outcome(updated)
        updated["queue_bucket"] = bucket_for_row(updated)["label"]
        refresh_derived(updated)
        with self.transaction():
            self.upsert_rows([updated])
            self.log_event(
                entity_type="workflow_row",
                entity_id=clean_text(updated.get("id")),
                batch_id=clean_text(updated.get("batch_id")),
                action="analyst_override",
                before=before,
                after=updated,
                details={"changed_fields": list(changed_fields or [])},
            )
        return updated

    def save_run(self, run: Mapping[str, Any]) -> None:
        table_name = self.table("runs")
        query = f"""
            MERGE INTO {table_name} AS target
            USING (
                SELECT
                    ? AS ID,
                    ? AS BATCH_ID,
                    ? AS MODE,
                    ? AS STATUS,
                    ? AS DRY_RUN,
                    ? AS INPUT_ROW_COUNT,
                    ? AS SELECTED_ROW_COUNT,
                    ? AS CHANGED_ROW_COUNT,
                    ? AS REVIEW_ROW_COUNT,
                    TRY_TO_TIMESTAMP_TZ(?) AS STARTED_AT,
                    TRY_TO_TIMESTAMP_TZ(?) AS COMPLETED_AT,
                    TRY_TO_TIMESTAMP_TZ(?) AS CREATED_AT,
                    PARSE_JSON(?) AS RUN_JSON
            ) AS source
            ON target.ID = source.ID
            WHEN MATCHED THEN UPDATE SET
                BATCH_ID = source.BATCH_ID,
                MODE = source.MODE,
                STATUS = source.STATUS,
                DRY_RUN = source.DRY_RUN,
                INPUT_ROW_COUNT = source.INPUT_ROW_COUNT,
                SELECTED_ROW_COUNT = source.SELECTED_ROW_COUNT,
                CHANGED_ROW_COUNT = source.CHANGED_ROW_COUNT,
                REVIEW_ROW_COUNT = source.REVIEW_ROW_COUNT,
                STARTED_AT = source.STARTED_AT,
                COMPLETED_AT = source.COMPLETED_AT,
                CREATED_AT = source.CREATED_AT,
                RUN_JSON = source.RUN_JSON
            WHEN NOT MATCHED THEN INSERT (
                ID, BATCH_ID, MODE, STATUS, DRY_RUN, INPUT_ROW_COUNT,
                SELECTED_ROW_COUNT, CHANGED_ROW_COUNT, REVIEW_ROW_COUNT,
                STARTED_AT, COMPLETED_AT, CREATED_AT, RUN_JSON
            ) VALUES (
                source.ID, source.BATCH_ID, source.MODE, source.STATUS,
                source.DRY_RUN, source.INPUT_ROW_COUNT, source.SELECTED_ROW_COUNT,
                source.CHANGED_ROW_COUNT, source.REVIEW_ROW_COUNT,
                source.STARTED_AT, source.COMPLETED_AT, source.CREATED_AT,
                source.RUN_JSON
            )
        """
        params = [
            clean_text(run.get("id")),
            clean_text(run.get("batch_id")),
            clean_text(run.get("mode")),
            clean_text(run.get("status")),
            bool_value(run.get("dry_run")),
            int(run.get("input_row_count") or 0),
            int(run.get("selected_row_count") or 0),
            int(run.get("changed_row_count") or 0),
            int(run.get("review_row_count") or 0),
            clean_text(run.get("started_at")),
            clean_text(run.get("completed_at")),
            clean_text(run.get("created_at")),
            json_dumps(run),
        ]
        self.execute(query, params)

    def insert_results(self, result_values: Sequence[Mapping[str, Any]], batch_id: str) -> None:
        if not result_values:
            return
        table_name = self.table("results")
        enriched = []
        for result in result_values:
            value = deepcopy(dict(result))
            value["batch_id"] = batch_id
            enriched.append(value)
        for batch in chunked(enriched):
            query = f"""
                MERGE INTO {table_name} AS target
                USING (
                    SELECT
                        value:"id"::VARCHAR AS ID,
                        value:"run_id"::VARCHAR AS RUN_ID,
                        value:"batch_id"::VARCHAR AS BATCH_ID,
                        value:"workflow_row_id"::VARCHAR AS WORKFLOW_ROW_ID,
                        value:"rules_applied" AS RULES_APPLIED,
                        COALESCE(
                            TRY_TO_TIMESTAMP_TZ(value:"created_at"::VARCHAR),
                            CURRENT_TIMESTAMP()
                        ) AS CREATED_AT,
                        value AS RESULT_JSON
                    FROM TABLE(FLATTEN(INPUT => PARSE_JSON(?)))
                ) AS source
                ON target.ID = source.ID
                WHEN MATCHED THEN UPDATE SET
                    RUN_ID = source.RUN_ID,
                    BATCH_ID = source.BATCH_ID,
                    WORKFLOW_ROW_ID = source.WORKFLOW_ROW_ID,
                    RULES_APPLIED = source.RULES_APPLIED,
                    CREATED_AT = source.CREATED_AT,
                    RESULT_JSON = source.RESULT_JSON
                WHEN NOT MATCHED THEN INSERT (
                    ID, RUN_ID, BATCH_ID, WORKFLOW_ROW_ID, RULES_APPLIED,
                    CREATED_AT, RESULT_JSON
                ) VALUES (
                    source.ID, source.RUN_ID, source.BATCH_ID,
                    source.WORKFLOW_ROW_ID, source.RULES_APPLIED,
                    source.CREATED_AT, source.RESULT_JSON
                )
            """
            self.execute(query, [json_dumps(batch)])

    def list_runs(self, batch_id: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        params: list[Any] = []
        where = ""
        if batch_id:
            where = "WHERE BATCH_ID = ?"
            params.append(batch_id)
        safe_limit = max(1, min(int(limit), 5000))
        query = f"""
            SELECT TO_JSON(RUN_JSON) AS RUN_JSON
            FROM {self.table('runs')}
            {where}
            ORDER BY CREATED_AT DESC
            LIMIT {safe_limit}
        """
        output: list[dict[str, Any]] = []
        for row in self.collect(query, params):
            value = normalize_persisted_json(snowflake_row_dict(row).get("run_json"), {})
            if isinstance(value, dict):
                output.append(value)
        return output

    def load_run_results(self, run_id: str, limit: int = 10_000) -> list[dict[str, Any]]:
        safe_limit = max(1, min(int(limit), 100_000))
        query = f"""
            SELECT TO_JSON(RESULT_JSON) AS RESULT_JSON
            FROM {self.table('results')}
            WHERE RUN_ID = ?
            ORDER BY CREATED_AT, ID
            LIMIT {safe_limit}
        """
        output: list[dict[str, Any]] = []
        for row in self.collect(query, [run_id]):
            value = normalize_persisted_json(snowflake_row_dict(row).get("result_json"), {})
            if isinstance(value, dict):
                output.append(value)
        return output

    def log_event(
        self,
        entity_type: str,
        action: str,
        entity_id: str = "",
        batch_id: str = "",
        before: Mapping[str, Any] | Sequence[Any] | None = None,
        after: Mapping[str, Any] | Sequence[Any] | None = None,
        details: Mapping[str, Any] | Sequence[Any] | None = None,
        user_name: str = "",
    ) -> None:
        query = f"""
            INSERT INTO {self.table('audit')} (
                ID, ENTITY_TYPE, ENTITY_ID, BATCH_ID, ACTION, USER_NAME,
                CREATED_AT, BEFORE_JSON, AFTER_JSON, DETAILS
            )
            SELECT
                ?, ?, NULLIF(?, ''), NULLIF(?, ''), ?, ?, CURRENT_TIMESTAMP(),
                IFF(NULLIF(?, '') IS NULL, NULL, PARSE_JSON(?)),
                IFF(NULLIF(?, '') IS NULL, NULL, PARSE_JSON(?)),
                IFF(NULLIF(?, '') IS NULL, NULL, PARSE_JSON(?))
        """
        before_json = "" if before is None else json_dumps(before)
        after_json = "" if after is None else json_dumps(after)
        details_json = "" if details is None else json_dumps(details)
        params = [
            new_id(),
            clean_text(entity_type),
            clean_text(entity_id),
            clean_text(batch_id),
            clean_text(action),
            clean_text(user_name) or self.current_user(),
            before_json,
            before_json or "{}",
            after_json,
            after_json or "{}",
            details_json,
            details_json or "{}",
        ]
        self.execute(query, params)

    def list_audit(
        self,
        batch_id: str | None = None,
        entity_id: str | None = None,
        limit: int = 250,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if batch_id:
            clauses.append("BATCH_ID = ?")
            params.append(batch_id)
        if entity_id:
            clauses.append("ENTITY_ID = ?")
            params.append(entity_id)
        where = "WHERE " + " AND ".join(clauses) if clauses else ""
        safe_limit = max(1, min(int(limit), 5000))
        query = f"""
            SELECT
                ID, ENTITY_TYPE, ENTITY_ID, BATCH_ID, ACTION, USER_NAME,
                CREATED_AT, TO_JSON(BEFORE_JSON) AS BEFORE_JSON_TEXT,
                TO_JSON(AFTER_JSON) AS AFTER_JSON_TEXT,
                TO_JSON(DETAILS) AS DETAILS_TEXT
            FROM {self.table('audit')}
            {where}
            ORDER BY CREATED_AT DESC
            LIMIT {safe_limit}
        """
        output: list[dict[str, Any]] = []
        for source_row in self.collect(query, params):
            data = snowflake_row_dict(source_row)
            output.append(
                {
                    "id": clean_text(data.get("id")),
                    "entity_type": clean_text(data.get("entity_type")),
                    "entity_id": clean_text(data.get("entity_id")),
                    "batch_id": clean_text(data.get("batch_id")),
                    "action": clean_text(data.get("action")),
                    "user_name": clean_text(data.get("user_name")),
                    "created_at": timestamp_text(data.get("created_at")),
                    "before": normalize_persisted_json(data.get("before_json_text"), None),
                    "after": normalize_persisted_json(data.get("after_json_text"), None),
                    "details": normalize_persisted_json(data.get("details_text"), {}),
                }
            )
        return output


# -----------------------------------------------------------------------------
# Run orchestration and analyst updates
# -----------------------------------------------------------------------------


def selected_rows(rows: Sequence[Mapping[str, Any]], row_ids: Sequence[str] | None) -> list[dict[str, Any]]:
    selected = set(row_ids or [])
    if not selected:
        return [deepcopy(dict(row)) for row in rows]
    return [deepcopy(dict(row)) for row in rows if clean_text(row.get("id")) in selected]


def run_batch(
    store: SnowflakeRulesStore,
    batch_id: str,
    *,
    dry_run: bool = False,
    row_ids: Sequence[str] | None = None,
) -> RunResult:
    batch = store.get_batch(batch_id)
    if batch is None:
        raise ValueError("The selected batch no longer exists.")
    before_rows = store.load_rows(batch_id)
    if not before_rows:
        raise ValueError("The selected batch contains no workflow rows.")
    requested_ids = [clean_text(value) for value in (row_ids or []) if clean_text(value)]
    if requested_ids:
        existing_ids = {clean_text(row.get("id")) for row in before_rows}
        missing = [value for value in requested_ids if value not in existing_ids]
        if missing:
            raise ValueError(f"{len(missing)} selected row(s) were not found in this batch.")
    rules = store.load_rules()
    if not rules:
        raise ValueError("The rule catalog is empty. Seed or create rules before running the engine.")
    references = store.load_reference_lists()
    started_at = iso_now()
    after_rows, changed_count, review_count = execute_rows(
        before_rows,
        rules,
        row_ids=requested_ids or None,
        reference_lists=references,
    )
    completed_at = iso_now()
    run_id = new_id()
    selected_count = len(requested_ids) if requested_ids else len(before_rows)
    mode = "selected" if requested_ids else "full"
    if dry_run:
        mode = f"dry_{mode}"
    run = {
        "id": run_id,
        "batch_id": batch_id,
        "batch_name": clean_text(batch.get("name")),
        "mode": mode,
        "status": "completed",
        "dry_run": bool(dry_run),
        "input_row_count": len(before_rows),
        "selected_row_count": selected_count,
        "changed_row_count": changed_count,
        "review_row_count": review_count,
        "started_at": started_at,
        "completed_at": completed_at,
        "created_at": completed_at,
        "executed_by": store.current_user(),
        "rule_catalog_snapshot": catalog_snapshot(rules),
        "reference_list_names": sorted(references),
        "selected_row_ids": requested_ids,
        "app_version": APP_VERSION,
    }
    results = create_results(run_id, before_rows, after_rows, row_ids=requested_ids or None)
    if not dry_run:
        rows_to_persist = selected_rows(after_rows, requested_ids or None)
        with store.transaction():
            store.upsert_rows(rows_to_persist)
            store.save_run(run)
            store.insert_results(results, batch_id)
            if not requested_ids:
                store.update_batch_status(batch_id, "Processed", len(after_rows))
            else:
                store.update_batch_status(batch_id, "In Review", len(after_rows))
            store.log_event(
                entity_type="run",
                entity_id=run_id,
                batch_id=batch_id,
                action="execute_selected_rows" if requested_ids else "execute_batch",
                after=run,
                details={
                    "changed_row_count": changed_count,
                    "review_row_count": review_count,
                    "result_count": len(results),
                },
            )
    return RunResult(run=run, results=results, rows=after_rows, dry_run=dry_run)


def apply_analyst_changes(
    source_row: Mapping[str, Any],
    changes: Mapping[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    editable_fields = {
        "action",
        "if_in_stock_action",
        "buysmart_action",
        "needs_review",
        "analyst_notes",
        "validation_status",
        "excluded",
        "excluded_reason",
        "assignment",
        "queue_bucket",
    }
    updated = deepcopy(dict(source_row))
    changed: list[str] = []
    for field in editable_fields:
        if field not in changes:
            continue
        value = changes[field]
        if field in {"needs_review", "excluded"}:
            value = bool_value(value)
        elif field in {"action", "if_in_stock_action", "buysmart_action"}:
            value = normalize_action(value)
        else:
            value = clean_text(value)
        if updated.get(field) != value:
            updated[field] = value
            changed.append(field)
    if bool_value(updated.get("excluded")) and not clean_text(updated.get("excluded_reason")):
        updated["excluded_reason"] = "Excluded by analyst"
        if "excluded_reason" not in changed:
            changed.append("excluded_reason")
    if not bool_value(updated.get("excluded")) and "excluded" in changed:
        updated["excluded_reason"] = ""
        if "excluded_reason" not in changed:
            changed.append("excluded_reason")
    updated["outcome_reporting"] = classify_outcome(updated)
    updated["status"] = "Excluded" if bool_value(updated.get("excluded")) else "Review" if bool_value(updated.get("needs_review")) else "Ready"
    if "queue_bucket" not in changed or not clean_text(updated.get("queue_bucket")):
        updated["queue_bucket"] = bucket_for_row(updated)["label"]
    updated["updated_at"] = iso_now()
    refresh_derived(updated)
    return updated, changed


# -----------------------------------------------------------------------------
# Streamlit presentation helpers
# -----------------------------------------------------------------------------


PAGE_NAMES = [
    "Overview",
    "Process Workbook",
    "Execution",
    "Analyst Workbench",
    "Reports",
    "Rules Catalog",
    "Simulator",
    "Settings",
]


def require_streamlit() -> None:
    if st is None:
        raise RuntimeError("Streamlit is required to run the user interface.")
    if pd is None:
        raise RuntimeError(f"Pandas is required to run the user interface: {PANDAS_IMPORT_ERROR}")



def migrate_session_state() -> dict[str, Any]:
    require_streamlit()
    raw_version = st.session_state.get("_rules_engine_state_schema", 0)
    try:
        previous_version = int(raw_version or 0)
    except (TypeError, ValueError):
        previous_version = 0
    report: dict[str, Any] = {
        "timestamp_utc": iso_now(),
        "from_version": previous_version,
        "to_version": SESSION_STATE_SCHEMA_VERSION,
        "removed_keys": [],
        "converted_keys": [],
        "status": "unchanged",
    }
    # Never retain ParsedWorkbook class instances across Streamlit reruns.
    for key in ("_parsed_upload", "_parsed_upload_key", "_parsed_upload_error", "_workbook_parse_cache", "_workbook_parse_cache_v6"):
        if key in st.session_state:
            st.session_state.pop(key, None)
            report["removed_keys"].append(key)
    # Convert legacy execution objects to plain payloads instead of discarding them.
    for key in ("_last_execution_result", "_workbench_run_result"):
        if key not in st.session_state:
            continue
        converted = run_result_from_payload(st.session_state.get(key))
        if converted is None:
            st.session_state.pop(key, None)
            report["removed_keys"].append(key)
        else:
            st.session_state[key] = run_result_to_payload(converted)
            report["converted_keys"].append(key)
    if previous_version != SESSION_STATE_SCHEMA_VERSION:
        report["status"] = "migrated"
        report["root_cause"] = (
            "Older builds stored script-defined dataclass instances in Streamlit Session State. "
            "Streamlit re-executes the script on interaction, so the next rerun can see a different class identity."
        )
        report["corrective_action"] = (
            "Legacy parser state was removed and execution state was converted to plain dictionaries/lists. "
            "Uploads are now parsed directly from the current source bytes on every rerun; no workbook parser cache is used."
        )
        for key in ("_rules_engine_initialized", "_startup_seed_report", "_startup_reference_count", "_application_self_check"):
            st.session_state.pop(key, None)
    elif report["removed_keys"] or report["converted_keys"]:
        report["status"] = "repaired"
    st.session_state["_rules_engine_state_schema"] = SESSION_STATE_SCHEMA_VERSION
    st.session_state["_rules_engine_app_version"] = APP_VERSION
    if report["status"] != "unchanged":
        st.session_state["_session_migration_report"] = _plain_data(report)
        if previous_version:
            st.session_state["_rules_engine_flash"] = {
                "message": "Recovered incompatible transient state from an older Rules Engine build; parser and execution state are now rerun-safe.",
                "kind": "info",
            }
    return report


def _runtime_exception_classification(exc: Exception, component: str) -> dict[str, str]:
    message = f"{type(exc).__name__}: {exc}"
    lowered = message.lower()
    component_text = clean_text(component) or "application"
    if "060119" in lowered or "tables cannot currently be created in a personal database" in lowered:
        return {
            "code": "OUTDATED_DDL_BUILD_OR_PERSONAL_DATABASE",
            "summary": "A runtime build attempted table DDL while connected to a personal database.",
            "recommended_action": f"Confirm the deployed sidebar shows {APP_VERSION} and sentinel {DEPLOYMENT_SENTINEL}. This build performs no runtime DDL and targets {TARGET_DATABASE}.{TARGET_SCHEMA} explicitly.",
        }
    if "could not acquire a snowflake session" in lowered or "no active session" in lowered or "session does not exist" in lowered:
        return {
            "code": "SNOWPARK_SESSION_UNAVAILABLE",
            "summary": "The application could not obtain or retain an active Snowpark session.",
            "recommended_action": "Run the file as Streamlit in Snowflake, verify the application warehouse is assigned and running, then reload the app.",
        }
    if "warehouse" in lowered and any(token in lowered for token in ("suspended", "not running", "does not exist", "not authorized")):
        return {
            "code": "WAREHOUSE_UNAVAILABLE",
            "summary": "The configured Snowflake warehouse is unavailable to the application owner role.",
            "recommended_action": f"Verify {TARGET_WAREHOUSE} exists, is resumed or auto-resume enabled, and {TARGET_ROLE} has USAGE on it.",
        }
    if "does not exist or not authorized" in lowered or "002003" in lowered:
        return {
            "code": "BACKEND_OBJECT_MISSING_OR_HIDDEN",
            "summary": "A required backend object is absent or not visible to the application owner role.",
            "recommended_action": f"Verify the seven {TARGET_DATABASE}.{TARGET_SCHEMA}.{TABLE_PREFIX}_* tables and grant DML access to {TARGET_ROLE}. Use Settings → Verify backend tables for the exact failing object.",
        }
    if "insufficient privileges" in lowered or "not authorized" in lowered or "access control error" in lowered:
        return {
            "code": "SNOWFLAKE_PRIVILEGE_FAILURE",
            "summary": "The application owner role lacks a required Snowflake privilege.",
            "recommended_action": f"Grant {TARGET_ROLE} USAGE on {TARGET_WAREHOUSE}, {TARGET_DATABASE}, and {TARGET_DATABASE}.{TARGET_SCHEMA}, plus SELECT/INSERT/UPDATE/DELETE on the backend tables.",
        }
    if "invalid identifier" in lowered or "unknown column" in lowered:
        return {
            "code": "BACKEND_COLUMN_CONTRACT_MISMATCH",
            "summary": "The Python application and backend table definitions disagree on one or more columns.",
            "recommended_action": "Run the backend SQL that ships with this build, then use Settings → Verify backend tables and download the diagnostic JSON.",
        }
    if any(token in lowered for token in ("statement timeout", "warehouse timeout", "operation timed out", "connection reset", "network is unreachable")):
        return {
            "code": "SNOWFLAKE_TRANSIENT_CONNECTIVITY_OR_TIMEOUT",
            "summary": "Snowflake execution was interrupted by a timeout or transient connection failure.",
            "recommended_action": "Retry once. If it repeats, inspect warehouse load, statement timeout settings, and the query ID in the technical diagnostic.",
        }
    if "is not json serializable" in lowered or "serializable session state" in lowered or "cannot pickle" in lowered:
        return {
            "code": "NON_SERIALIZABLE_TRANSIENT_STATE",
            "summary": "A non-plain Python object reached a persistence, cache, or Session State boundary.",
            "recommended_action": f"Use build {APP_VERSION}; it converts workbook and execution state to plain dictionaries/lists. Verify sentinel {DEPLOYMENT_SENTINEL} in the sidebar.",
        }
    if isinstance(exc, json.JSONDecodeError):
        return {
            "code": "INVALID_JSON_INPUT",
            "summary": f"The {component_text} operation received malformed JSON.",
            "recommended_action": f"Correct the JSON at line {exc.lineno}, column {exc.colno}. The technical diagnostic includes the exact parser message.",
        }
    if isinstance(exc, KeyError):
        return {
            "code": "DATA_CONTRACT_KEY_MISSING",
            "summary": f"The {component_text} operation expected a field that was not present.",
            "recommended_action": "Inspect the diagnostic context and traceback to identify the missing field, then verify the workbook/backend contract.",
        }
    if isinstance(exc, TypeError):
        return {
            "code": "DATA_TYPE_CONTRACT_FAILURE",
            "summary": f"The {component_text} operation received a value of the wrong type.",
            "recommended_action": "Inspect the diagnostic context and traceback. The source values and operation inputs are included without retaining the uploaded file bytes.",
        }
    if isinstance(exc, ValueError):
        return {
            "code": "INPUT_OR_RULE_VALIDATION_FAILURE",
            "summary": clean_text(exc) or f"The {component_text} input failed validation.",
            "recommended_action": "Correct the highlighted input or rule definition. Open the technical diagnostic for the exact validation path and operation context.",
        }
    return {
        "code": "UNCLASSIFIED_RUNTIME_FAILURE",
        "summary": f"The {component_text} operation raised {type(exc).__name__}.",
        "recommended_action": "Use the diagnostic ID, source fingerprint, operation context, and traceback below to isolate the failing operation.",
    }


def build_runtime_diagnostic(component: str, exc: Exception, context: Mapping[str, Any] | None = None) -> dict[str, Any]:
    return {
        "diagnostic_id": f"RT-{uuid.uuid4().hex[:12].upper()}",
        "timestamp_utc": iso_now(),
        "component": clean_text(component),
        "status": "failed",
        "classification": _runtime_exception_classification(exc, component),
        "exception": {
            "type": type(exc).__name__,
            "message": clean_text(exc),
            "traceback": "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))[-32000:],
        },
        "context": _plain_data(context or {}),
        "environment": runtime_environment_snapshot(),
    }


def record_diagnostic_event(event: Mapping[str, Any]) -> None:
    if st is None:
        return
    payload = _plain_data(event)
    events = st.session_state.get("_diagnostic_events")
    if not isinstance(events, list):
        events = []
    diagnostic_id = clean_text(payload.get("diagnostic_id")) if isinstance(payload, Mapping) else ""
    if diagnostic_id and any(isinstance(item, Mapping) and clean_text(item.get("diagnostic_id")) == diagnostic_id for item in events):
        return
    events.append(payload)
    st.session_state["_diagnostic_events"] = events[-MAX_DIAGNOSTIC_EVENTS:]


def render_actionable_exception(title: str, exc: Exception, *, component: str, context: Mapping[str, Any] | None = None) -> dict[str, Any]:
    require_streamlit()
    event = build_runtime_diagnostic(component, exc, context)
    record_diagnostic_event(event)
    classification = event.get("classification") or {}
    st.error(clean_text(title))
    st.markdown(f"**Likely root cause:** {clean_text(classification.get('summary'))}")
    st.info(f"**Corrective action:** {clean_text(classification.get('recommended_action'))}")
    st.caption(f"Diagnostic ID: {clean_text(event.get('diagnostic_id'))} · Code: {clean_text(classification.get('code'))} · Build: {APP_VERSION}")
    with st.expander("Technical diagnostic", expanded=False):
        st.json({key: value for key, value in event.items() if key != "exception"})
        st.code(clean_text((event.get("exception") or {}).get("traceback")), language="text")
        st.download_button(
            "Download diagnostic JSON",
            data=json_dumps(event, pretty=True),
            file_name=f"rules_engine_diagnostic_{clean_text(event.get('diagnostic_id'))}.json",
            mime="application/json",
            key=f"download_diag_{clean_text(event.get('diagnostic_id'))}",
        )
    return event


def render_diagnostic_log() -> None:
    require_streamlit()
    events = st.session_state.get("_diagnostic_events")
    if not isinstance(events, list) or not events:
        st.caption("No runtime failures have been captured in this browser session.")
        return
    records = []
    for event in reversed(events):
        classification = event.get("classification") or event.get("root_cause") or {}
        records.append({
            "Time": clean_text(event.get("timestamp_utc"))[:19].replace("T", " "),
            "ID": clean_text(event.get("diagnostic_id")),
            "Component": clean_text(event.get("component")),
            "Code": clean_text(classification.get("code")),
            "Summary": clean_text(classification.get("summary")),
        })
    dataframe(pd.DataFrame(records), height=min(520, 38 + len(records) * 35))
    st.download_button(
        "Download current-session diagnostics",
        data=json_dumps(events, pretty=True),
        file_name="rules_engine_session_diagnostics.json",
        mime="application/json",
        key="download_session_diagnostics",
    )
    if st.button("Clear current-session diagnostics", key="clear_session_diagnostics"):
        st.session_state["_diagnostic_events"] = []
        safe_rerun()


def acquire_snowflake_session() -> Any:
    require_streamlit()
    errors: list[str] = []
    if get_active_session is not None:
        try:
            return get_active_session()
        except Exception as exc:
            errors.append(f"active Snowpark session: {exc}")
    try:
        return st.connection("snowflake").session()
    except Exception as exc:
        errors.append(f"Streamlit Snowflake connection: {exc}")
    raise RuntimeError("Could not acquire a Snowflake session. " + " | ".join(errors))


def initialize_store() -> SnowflakeRulesStore:
    require_streamlit()
    # Acquire the Snowpark session on each rerun instead of storing a potentially
    # non-serializable session object in Streamlit session state. The store uses
    # fully qualified names and never relies on CURRENT_DATABASE/CURRENT_SCHEMA.
    store = SnowflakeRulesStore(
        acquire_snowflake_session(),
        database=TARGET_DATABASE,
        schema=TARGET_SCHEMA,
    )
    if not bool_value(st.session_state.get("_rules_engine_initialized")):
        store.verify_backend()
        seed_report = store.seed_bundled_rules(force=False)
        reference_count = store.seed_reference_lists()
        st.session_state["_startup_seed_report"] = seed_report
        st.session_state["_startup_reference_count"] = reference_count
        st.session_state["_rules_engine_initialized"] = True
    return store


def safe_rerun() -> None:
    require_streamlit()
    if hasattr(st, "rerun"):
        st.rerun()
    else:  # pragma: no cover - compatibility with older Streamlit releases
        st.experimental_rerun()


def set_flash(message: str, kind: str = "success") -> None:
    if st is not None:
        st.session_state["_rules_engine_flash"] = {"message": clean_text(message), "kind": clean_text(kind)}


def render_flash() -> None:
    require_streamlit()
    flash = st.session_state.pop("_rules_engine_flash", None)
    if not isinstance(flash, Mapping):
        return
    message = clean_text(flash.get("message"))
    kind = clean_text(flash.get("kind"))
    if not message:
        return
    renderer = getattr(st, kind, st.info)
    renderer(message)


def app_styles() -> None:
    require_streamlit()
    st.markdown(
        """
        <style>
        .block-container {padding-top: 1.2rem; padding-bottom: 3rem; max-width: 1500px;}
        [data-testid="stMetricValue"] {font-size: 1.7rem;}
        .rules-kicker {font-size: .78rem; letter-spacing: .08em; text-transform: uppercase; opacity: .68; font-weight: 700;}
        .rules-subtitle {font-size: .95rem; opacity: .78; margin-top: -.35rem; margin-bottom: 1rem;}
        .rules-card {border: 1px solid rgba(128,128,128,.25); border-radius: .7rem; padding: .8rem 1rem; margin: .35rem 0;}
        .rules-muted {opacity: .7;}
        </style>
        """,
        unsafe_allow_html=True,
    )


def dataframe(records: Any, *, use_container_width: bool = True, hide_index: bool = True, height: int | None = None) -> None:
    require_streamlit()
    kwargs: dict[str, Any] = {"use_container_width": use_container_width, "hide_index": hide_index}
    if height is not None:
        kwargs["height"] = height
    st.dataframe(records, **kwargs)


def batch_label(batch: Mapping[str, Any]) -> str:
    return (
        f"{clean_text(batch.get('name')) or clean_text(batch.get('source_file_name'))}"
        f" · {int(batch.get('row_count') or 0):,} rows · {clean_text(batch.get('status')) or 'Unknown'}"
    )


def choose_batch_sidebar(store: SnowflakeRulesStore) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    require_streamlit()
    batches = store.list_batches(include_archived=False)
    valid_ids = {clean_text(batch.get("id")) for batch in batches}
    current = clean_text(st.session_state.get("selected_batch_id"))
    pending = st.session_state.pop("_pending_batch_picker", None)
    if pending is not None and (clean_text(pending) in valid_ids or not clean_text(pending)):
        current = clean_text(pending)
        st.session_state["selected_batch_id"] = current
    if current not in valid_ids:
        current = clean_text(batches[0].get("id")) if batches else ""
        st.session_state["selected_batch_id"] = current
    options = [""] + [clean_text(batch.get("id")) for batch in batches]
    index = options.index(current) if current in options else 0
    if st.session_state.get("_batch_picker") not in options:
        st.session_state["_batch_picker"] = current
    selected_id = st.sidebar.selectbox(
        "Active batch",
        options,
        index=index,
        format_func=lambda value: "No batch selected" if not value else batch_label(next(batch for batch in batches if batch["id"] == value)),
        key="_batch_picker",
    )
    st.session_state["selected_batch_id"] = selected_id
    selected = next((batch for batch in batches if clean_text(batch.get("id")) == selected_id), None)
    return batches, selected


def render_page_header(title: str, subtitle: str = "", kicker: str = "Rules Operations") -> None:
    require_streamlit()
    st.markdown(f'<div class="rules-kicker">{xml_escape(kicker)}</div>', unsafe_allow_html=True)
    st.title(title)
    if subtitle:
        st.markdown(f'<div class="rules-subtitle">{xml_escape(subtitle)}</div>', unsafe_allow_html=True)


def require_selected_batch(selected_batch: Mapping[str, Any] | None) -> bool:
    require_streamlit()
    if selected_batch is not None:
        return True
    st.info("Select or ingest a batch to use this workspace.")
    return False


def safe_download_filename(value: Any, extension: str, fallback: str = "rules_engine_export") -> str:
    base = re.sub(r"[^A-Za-z0-9._-]+", "_", clean_text(value)).strip("._-")
    base = base[:120] or fallback
    suffix = extension if extension.startswith(".") else f".{extension}"
    return base if base.lower().endswith(suffix.lower()) else f"{base}{suffix}"


def options_with_current(defaults: Sequence[str], current: Any, include_blank: bool = True) -> list[str]:
    output: list[str] = [""] if include_blank else []
    for value in [clean_text(current), *[clean_text(item) for item in defaults]]:
        if value and value not in output:
            output.append(value)
    return output


def run_summary_records(result: RunResult) -> list[dict[str, Any]]:
    rows_before = {clean_text(item.get("workflow_row_id")): item.get("before_state") for item in result.results}
    records: list[dict[str, Any]] = []
    for row in result.rows:
        row_id = clean_text(row.get("id"))
        before = rows_before.get(row_id)
        if before is None:
            continue
        records.append(
            {
                "Source Row": row.get("source_row_number"),
                "Case#": clean_text(row.get("case_number")),
                "Vendor": clean_text(row.get("vendor")),
                "Before ACTION": clean_text(before.get("action")) if isinstance(before, Mapping) else "",
                "After ACTION": clean_text(row.get("action")),
                "BuySmart": clean_text(row.get("buysmart_action")),
                "Outcome": clean_text(row.get("outcome_reporting")),
                "Review": bool_value(row.get("needs_review")),
                "Rules": clean_text(row.get("rule_applied")),
            }
        )
    return records


def render_run_result(result: RunResult, rules: Sequence[Mapping[str, Any]] | None = None) -> None:
    require_streamlit()
    run = result.run
    if result.dry_run:
        st.warning("Dry run complete. No Snowflake rows, run records, or results were changed.")
    else:
        st.success("Rules execution completed and was committed to Snowflake.")
    columns = st.columns(5)
    columns[0].metric("Evaluated", f"{int(run.get('selected_row_count') or 0):,}")
    columns[1].metric("Changed", f"{int(run.get('changed_row_count') or 0):,}")
    columns[2].metric("Needs review", f"{int(run.get('review_row_count') or 0):,}")
    columns[3].metric("Result records", f"{len(result.results):,}")
    columns[4].metric("Mode", clean_text(run.get("mode")).replace("_", " ").title())
    records = run_summary_records(result)
    if records:
        st.subheader("Decision changes")
        dataframe(pd.DataFrame(records), height=min(520, 38 + len(records[:50]) * 35))
    else:
        st.info("No decision state changed in this execution scope.")
    if result.dry_run:
        export_rules = list(rules or [])
        st.download_button(
            "Download dry-run outcomes (CSV)",
            data=export_csv(result.rows, export_rules),
            file_name=f"rules_engine_dry_run_{clean_text(run.get('id'))[:8]}.csv",
            mime="text/csv",
            key=f"dry_csv_{clean_text(run.get('id'))}",
        )


def persist_rule_change(
    store: SnowflakeRulesStore,
    before: Mapping[str, Any] | None,
    after: Mapping[str, Any],
    action: str,
) -> None:
    with store.transaction():
        store.upsert_rules([after])
        store.log_event(
            entity_type="rule",
            entity_id=clean_text(after.get("id")),
            action=action,
            before=before,
            after=after,
            details={"rule_id": clean_text(after.get("rule_id"))},
        )


def row_table_records(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "Source Row": row.get("source_row_number"),
            "Case#": clean_text(row.get("case_number")),
            "Business": clean_text(row.get("business")),
            "Type": clean_text(row.get("request_type")),
            "Vendor": clean_text(row.get("vendor")),
            "DIN": clean_text(row.get("din")),
            "MIN": clean_text(row.get("min")),
            "Description": clean_text(row.get("description")),
            "ACTION": clean_text(row.get("action")),
            "If In Stock": clean_text(row.get("if_in_stock_action")),
            "BuySmart": clean_text(row.get("buysmart_action")),
            "Outcome": clean_text(row.get("outcome_reporting")),
            "Queue": clean_text(row.get("queue_bucket")),
            "Review": bool_value(row.get("needs_review")),
            "Excluded": bool_value(row.get("excluded")),
            "Rules": clean_text(row.get("rule_applied")),
        }
        for row in rows
    ]


# -----------------------------------------------------------------------------
# Streamlit pages: overview, ingestion, execution
# -----------------------------------------------------------------------------


def render_overview_page(
    store: SnowflakeRulesStore,
    batches: Sequence[Mapping[str, Any]],
    selected_batch: Mapping[str, Any] | None,
) -> None:
    render_page_header(
        "Operations Overview",
        "A Snowflake-native view of ingestion, automated decisions, review demand, and catalog readiness.",
    )
    rules = store.load_rules()
    snapshot = catalog_snapshot(rules)
    total_rows = sum(int(batch.get("row_count") or 0) for batch in batches)
    recent_runs = store.list_runs(limit=25)
    metrics = st.columns(5)
    metrics[0].metric("Active batches", f"{len(batches):,}")
    metrics[1].metric("Stored rows", f"{total_rows:,}")
    metrics[2].metric("Rule definitions", f"{len(rules):,}")
    metrics[3].metric("Executable variants", f"{len(snapshot['executionOrder']):,}")
    metrics[4].metric("Recorded runs", f"{len(recent_runs):,}")

    if selected_batch:
        rows = store.load_rows(clean_text(selected_batch.get("id")))
        summary = summarize_batch(rows)
        st.subheader(clean_text(selected_batch.get("name")))
        selected_metrics = st.columns(6)
        selected_metrics[0].metric("Rows", f"{summary['row_count']:,}")
        selected_metrics[1].metric("Automation coverage", f"{summary['automation_coverage_pct']:.1f}%")
        selected_metrics[2].metric("Approved", f"{summary['approved_count']:,}")
        selected_metrics[3].metric("Denied", f"{summary['denied_count']:,}")
        selected_metrics[4].metric("Review", f"{summary['review_count']:,}")
        selected_metrics[5].metric("Excluded", f"{summary['excluded_count']:,}")
        left, right = st.columns([1.15, 1])
        with left:
            st.markdown("#### Compliance buckets")
            bucket_records = [
                {
                    "Bucket": item["label"],
                    "Rows": item["count"],
                    "Needs Review": item["review_count"],
                    "Rule Variants": ", ".join(item["rule_ids"][:8]),
                }
                for item in summary["bucket_summaries"]
            ]
            dataframe(pd.DataFrame(bucket_records) if bucket_records else pd.DataFrame())
        with right:
            st.markdown("#### Outcomes")
            if summary["outcome_counts"]:
                chart = pd.DataFrame(
                    {"Count": list(summary["outcome_counts"].values())},
                    index=list(summary["outcome_counts"].keys()),
                )
                st.bar_chart(chart)
            else:
                st.info("Run the engine to populate outcome reporting.")
    else:
        st.info("Ingest a workbook to establish the first operational batch.")

    lower_left, lower_right = st.columns([1.2, 1])
    with lower_left:
        st.subheader("Recent batches")
        batch_records = [
            {
                "Batch": clean_text(batch.get("name")),
                "Rows": int(batch.get("row_count") or 0),
                "Status": clean_text(batch.get("status")),
                "Source": clean_text(batch.get("source_file_name")),
                "Created": timestamp_text(batch.get("created_at"))[:19].replace("T", " "),
            }
            for batch in batches[:15]
        ]
        if batch_records:
            dataframe(pd.DataFrame(batch_records))
        else:
            st.caption("No batches have been ingested.")
    with lower_right:
        st.subheader("Recent executions")
        run_records = [
            {
                "Batch": clean_text(run.get("batch_name")),
                "Mode": clean_text(run.get("mode")).replace("_", " ").title(),
                "Rows": int(run.get("selected_row_count") or 0),
                "Changed": int(run.get("changed_row_count") or 0),
                "Review": int(run.get("review_row_count") or 0),
                "Completed": timestamp_text(run.get("completed_at"))[:19].replace("T", " "),
            }
            for run in recent_runs[:15]
        ]
        if run_records:
            dataframe(pd.DataFrame(run_records))
        else:
            st.caption("No committed execution runs have been recorded.")


def normalized_preview_records(parsed: ParsedWorkbook, limit: int = 25) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    source_numbers = parsed.source_row_numbers or list(range(2, len(parsed.rows) + 2))
    for position, raw_row in enumerate(parsed.rows[:limit]):
        index = source_numbers[position] if position < len(source_numbers) else position + 2
        normalized = create_normalized_row(raw_row)
        fields = normalized["fields"]
        output.append(
            {
                "Source Row": index,
                "Business": fields.get("business"),
                "Type": fields.get("requestType"),
                "Case#": fields.get("caseNumber"),
                "Vendor": fields.get("vendor"),
                "DIN": fields.get("din"),
                "MIN": fields.get("min"),
                "Description": fields.get("description"),
                "Usage": fields.get("usageQty"),
                "ACTION": fields.get("upstreamAction"),
                "If In Stock": fields.get("upstreamIfInStockAction"),
                "BuySmart": fields.get("upstreamBuysmartAction"),
            }
        )
    return output



def render_workbook_diagnostics(diagnostics: Mapping[str, Any], *, expanded: bool = False) -> None:
    require_streamlit()
    diagnostic_id = clean_text(diagnostics.get("diagnostic_id")) or "workbook"
    status = clean_text(diagnostics.get("status")) or "unknown"
    root_cause = diagnostics.get("root_cause") if isinstance(diagnostics.get("root_cause"), Mapping) else {}
    with st.expander(f"Workbook diagnostics · {diagnostic_id} · {status.upper()}", expanded=expanded):
        file_info = diagnostics.get("file") if isinstance(diagnostics.get("file"), Mapping) else {}
        workbook_info = diagnostics.get("workbook") if isinstance(diagnostics.get("workbook"), Mapping) else {}
        metrics = st.columns(5)
        metrics[0].metric("Status", status.upper())
        metrics[1].metric("File bytes", f"{int(file_info.get('size_bytes') or 0):,}")
        metrics[2].metric("Rows", f"{int(workbook_info.get('row_count') or 0):,}")
        metrics[3].metric("Columns", f"{int(workbook_info.get('column_count') or 0):,}")
        metrics[4].metric("Elapsed", f"{float(diagnostics.get('total_elapsed_ms') or 0):,.1f} ms")
        if clean_text(root_cause.get("code")) not in {"", "NONE"}:
            st.markdown(f"**Root-cause code:** `{clean_text(root_cause.get('code'))}`")
            st.markdown(f"**Finding:** {clean_text(root_cause.get('summary'))}")
            st.info(f"**Corrective action:** {clean_text(root_cause.get('recommended_action'))}")
        stages = diagnostics.get("stages")
        if isinstance(stages, list) and stages:
            st.markdown("#### Validation stages")
            dataframe(pd.DataFrame(stages), height=min(420, 38 + len(stages) * 35))
        package = diagnostics.get("package_preflight")
        if isinstance(package, Mapping):
            st.markdown("#### Open XML package preflight")
            st.json(package)
        with st.expander("Complete diagnostic payload", expanded=False):
            st.json(diagnostics)
            exception = diagnostics.get("exception") if isinstance(diagnostics.get("exception"), Mapping) else {}
            if clean_text(exception.get("traceback")):
                st.code(clean_text(exception.get("traceback")), language="text")
        st.download_button(
            "Download workbook diagnostic JSON",
            data=json_dumps(diagnostics, pretty=True),
            file_name=f"workbook_diagnostic_{diagnostic_id}.json",
            mime="application/json",
            key=f"download_workbook_diag_{diagnostic_id}",
        )


def render_workbook_parse_failure(diagnostics: Mapping[str, Any], source_hash: str) -> None:
    require_streamlit()
    record_diagnostic_event(diagnostics)
    root_cause = diagnostics.get("root_cause") if isinstance(diagnostics.get("root_cause"), Mapping) else {}
    code = clean_text(root_cause.get("code")) or "UNCLASSIFIED_WORKBOOK_FAILURE"
    st.error(clean_text(root_cause.get("summary")) or "Workbook processing failed before a valid row payload was produced.")
    st.markdown(f"**Root-cause code:** `{code}`")
    st.info(f"**Corrective action:** {clean_text(root_cause.get('recommended_action')) or 'Inspect the diagnostic details below.'}")
    st.caption(f"Diagnostic ID: {clean_text(diagnostics.get('diagnostic_id'))} · Parser: {WORKBOOK_PARSER_VERSION} · SHA-256: {source_hash}")
    retry_key = f"_workbook_retry_nonce_{source_hash[:16]}"
    columns = st.columns([1, 4])
    if columns[0].button("Re-parse from source bytes", type="primary", key=f"retry_parse_{source_hash[:16]}"):
        st.session_state[retry_key] = int(st.session_state.get(retry_key) or 0) + 1
        safe_rerun()
    columns[1].caption("Retry repeats byte validation, package inspection, parsing, and payload reconstruction. This build uses no workbook parser cache.")
    render_workbook_diagnostics(diagnostics, expanded=True)


def render_process_workbook_page(store: SnowflakeRulesStore) -> None:
    render_page_header(
        "Process Workbook",
        "Upload a daily action file, inspect normalization, create a persistent batch, and optionally execute immediately.",
        kicker="Ingestion",
    )
    uploaded = st.file_uploader(
        "Daily action file",
        type=["csv", "txt", "tsv", "xlsx", "xlsm"],
        help="Every rerun reads the current uploader bytes directly, validates the Open XML package, parses the source worksheet, and records downloadable root-cause diagnostics.",
    )
    if uploaded is None:
        st.markdown(
            "Upload a CSV, XLSX, or XLSM file. The source is normalized into workflow rows; the original values remain available for audit and export."
        )
        return

    try:
        source_bytes = uploaded.getvalue()
        if not isinstance(source_bytes, bytes):
            source_bytes = bytes(source_bytes)
        if not source_bytes:
            raise ValueError("The uploaded file contains zero bytes.")
        source_hash = hashlib.sha256(source_bytes).hexdigest()
        declared_size = getattr(uploaded, "size", None)
        if declared_size not in (None, 0) and int(declared_size) != len(source_bytes):
            raise RuntimeError(
                f"Upload byte-count mismatch: Streamlit reported {int(declared_size):,} bytes but returned {len(source_bytes):,} bytes."
            )
        retry_key = f"_workbook_retry_nonce_{source_hash[:16]}"
        retry_nonce = int(st.session_state.get(retry_key) or 0)
        outcome = parse_source_workbook_for_ui(uploaded.name, source_hash, source_bytes, retry_nonce)
    except Exception as exc:
        render_actionable_exception(
            "The uploaded file could not reach the workbook parser.",
            exc,
            component="Workbook upload/cache boundary",
            context={
                "file_name": clean_text(getattr(uploaded, "name", "")),
                "declared_size": getattr(uploaded, "size", None),
                "mime_type": clean_text(getattr(uploaded, "type", "")),
                "parser_version": WORKBOOK_PARSER_VERSION,
            },
        )
        return
    diagnostics = outcome.get("diagnostics") if isinstance(outcome, Mapping) else None
    if not isinstance(diagnostics, Mapping):
        diagnostics = {
            "diagnostic_id": f"WB-{source_hash[:12]}-CONTRACT",
            "timestamp_utc": iso_now(),
            "component": "Workbook parser",
            "status": "failed",
            "root_cause": {
                "code": "PARSER_OUTCOME_CONTRACT_MISSING",
                "summary": "The parser returned no diagnostic payload.",
                "recommended_action": f"Redeploy build {APP_VERSION}; the parser result contract is incomplete.",
            },
            "file": {"name": uploaded.name, "size_bytes": len(source_bytes), "sha256": source_hash},
            "environment": runtime_environment_snapshot(),
            "stages": [],
        }
    if not bool_value(outcome.get("ok") if isinstance(outcome, Mapping) else False):
        render_workbook_parse_failure(diagnostics, source_hash)
        return
    parsed = parsed_workbook_from_payload(outcome.get("workbook") if isinstance(outcome, Mapping) else None)
    if parsed is None:
        render_actionable_exception(
            "Workbook payload reconstruction failed.",
            RuntimeError("Successful parser outcome could not be reconstructed from its plain-data payload."),
            component="Workbook payload reconstruction",
            context={"file_name": uploaded.name, "source_hash": source_hash, "diagnostics": diagnostics},
        )
        return

    known_columns = [column for column in parsed.columns if column in EXPECTED_HEADERS]
    required_missing = [column for column in ("Business", "Type") if column not in parsed.columns]
    metrics = st.columns(4)
    metrics[0].metric("Rows", f"{len(parsed.rows):,}")
    metrics[1].metric("Columns", f"{len(parsed.columns):,}")
    metrics[2].metric("Recognized columns", f"{len(known_columns):,}")
    metrics[3].metric("Worksheet", clean_text(parsed.sheet_name))
    st.caption(
        f"Parser PASS · Diagnostic {clean_text(diagnostics.get('diagnostic_id'))} · "
        f"Upload SHA-256 {source_hash} · Build source {clean_text(source_code_fingerprint().get('sha256_short'))}"
    )
    for warning in parsed.warnings:
        st.warning(warning)
    if required_missing:
        st.warning(
            "These recommended routing columns were not detected: " + ", ".join(required_missing) + ". Rows can still be ingested."
        )
    duplicate = store.find_batch_by_hash(source_hash)
    allow_duplicate = True
    if duplicate:
        st.warning(
            f"This exact file content was already ingested as **{clean_text(duplicate.get('name'))}** on "
            f"{timestamp_text(duplicate.get('created_at'))[:19].replace('T', ' ')}."
        )
        allow_duplicate = st.checkbox("Ingest another copy intentionally", value=False)

    st.subheader("Normalized preview")
    preview = normalized_preview_records(parsed)
    dataframe(pd.DataFrame(preview), height=min(620, 38 + len(preview) * 35))
    with st.expander("Source columns and ingestion warnings"):
        st.write(parsed.columns)
        if parsed.warnings:
            st.write(parsed.warnings)
        else:
            st.caption("No parser warnings.")
    render_workbook_diagnostics(diagnostics, expanded=False)

    stem = uploaded.name.rsplit(".", 1)[0]
    form_key = source_hash[:12]
    with st.form(f"ingest_form_{form_key}"):
        batch_name = st.text_input("Batch name", value=stem)
        include_reporting_date = st.checkbox("Set a reporting date", value=False)
        reporting_date = st.date_input("Reporting date", value=date.today(), disabled=not include_reporting_date)
        execute_immediately = st.checkbox("Execute all approved rules immediately after ingestion", value=False)
        submitted = st.form_submit_button("Ingest workbook", type="primary", disabled=not allow_duplicate)
    if submitted:
        try:
            batch, _ = store.create_batch(
                parsed,
                source_bytes,
                batch_name=batch_name,
                reporting_date=reporting_date if include_reporting_date else None,
            )
            execution_message = ""
            if execute_immediately:
                result = run_batch(store, batch["id"], dry_run=False)
                execution_message = f" Rules changed {int(result.run.get('changed_row_count') or 0):,} row(s)."
            st.session_state["selected_batch_id"] = batch["id"]
            st.session_state["_pending_batch_picker"] = batch["id"]
            set_flash(f"Ingested {len(parsed.rows):,} rows into **{batch['name']}**.{execution_message}")
            safe_rerun()
        except Exception as exc:
            render_actionable_exception(
                "Workbook parsing succeeded, but Snowflake ingestion failed.",
                exc,
                component="Workbook ingestion",
                context={
                    "file_name": uploaded.name,
                    "source_hash": source_hash,
                    "sheet_name": parsed.sheet_name,
                    "row_count": len(parsed.rows),
                    "column_count": len(parsed.columns),
                },
            )


def execution_plan_records(rules: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "Priority": int(variant.get("execution_priority") or 0),
            "Runtime Rule": clean_text(variant.get("runtime_rule_id")),
            "Rule": clean_text(variant.get("rule_id")),
            "Kind": clean_text(variant.get("runtime_kind")),
            "Stop": bool_value(variant.get("stop_processing")),
            "Description": clean_text(variant.get("description")),
            "Actions": summarize_actions(variant.get("action_json") or []),
        }
        for variant in executable_variants(rules)
    ]


def render_execution_page(store: SnowflakeRulesStore, selected_batch: Mapping[str, Any] | None) -> None:
    render_page_header(
        "Rules Execution",
        "Run the ordered approved catalog against a full batch or an explicitly selected row set, with an optional no-write dry run.",
        kicker="Automation",
    )
    if not require_selected_batch(selected_batch):
        return
    batch_id = clean_text(selected_batch.get("id"))
    rules = store.load_rules()
    row_count = store.row_count(batch_id)
    plan = execution_plan_records(rules)
    metrics = st.columns(4)
    metrics[0].metric("Batch rows", f"{row_count:,}")
    metrics[1].metric("Rule definitions", f"{len(rules):,}")
    metrics[2].metric("Executable variants", f"{len(plan):,}")
    metrics[3].metric("Batch status", clean_text(selected_batch.get("status")))

    scope = st.radio("Execution scope", ["Full batch", "Selected rows"], horizontal=True)
    dry_run = st.checkbox("Dry run — calculate decisions without writing rows or run history", value=False)
    selected_ids: list[str] = []
    if scope == "Selected rows":
        filter_columns = st.columns([2, 1, 1])
        search = filter_columns[0].text_input("Find rows", key="exec_search", placeholder="Case, vendor, DIN, description…")
        request_types = ["All", *store.distinct_row_values(batch_id, "request_type")]
        request_type = filter_columns[1].selectbox("Request type", request_types, key="exec_type")
        preview_limit = int(filter_columns[2].selectbox("Rows to show", [100, 250, 500, 1000], index=1))
        filters = {"search": search, "request_type": request_type}
        candidate_rows = store.load_rows(batch_id, filters=filters, limit=preview_limit)
        records = []
        index_values = []
        for row in candidate_rows:
            records.append(
                {
                    "Select": False,
                    "Source Row": row.get("source_row_number"),
                    "Case#": clean_text(row.get("case_number")),
                    "Vendor": clean_text(row.get("vendor")),
                    "DIN": clean_text(row.get("din")),
                    "Description": clean_text(row.get("description")),
                    "Current ACTION": clean_text(row.get("action")),
                    "Outcome": clean_text(row.get("outcome_reporting")),
                }
            )
            index_values.append(clean_text(row.get("id")))
        if records:
            select_frame = pd.DataFrame(records, index=index_values)
            edited = st.data_editor(
                select_frame,
                use_container_width=True,
                hide_index=True,
                disabled=[column for column in select_frame.columns if column != "Select"],
                num_rows="fixed",
                key=f"exec_selected_{batch_id}_{hashlib.sha256(json_dumps(filters).encode('utf-8')).hexdigest()[:10]}",
            )
            selected_ids = [clean_text(index) for index, value in edited["Select"].items() if bool_value(value)]
            st.caption(f"{len(selected_ids):,} of {len(candidate_rows):,} displayed rows selected.")
        else:
            st.info("No rows match the execution filters.")

    button_label = "Run dry preview" if dry_run else "Execute rules"
    execute_disabled = scope == "Selected rows" and not selected_ids
    if st.button(button_label, type="primary", disabled=execute_disabled):
        try:
            with st.spinner("Evaluating the ordered rule catalog…"):
                result = run_batch(
                    store,
                    batch_id,
                    dry_run=dry_run,
                    row_ids=selected_ids if scope == "Selected rows" else None,
                )
            st.session_state["_last_execution_result"] = run_result_to_payload(result)
            st.session_state["_last_execution_batch"] = batch_id
        except Exception as exc:
            render_actionable_exception(
                "Rules execution failed.",
                exc,
                component="Rules execution",
                context={"batch_id": batch_id, "scope": scope, "dry_run": dry_run, "selected_row_count": len(selected_ids)},
            )
    last_result = run_result_from_payload(st.session_state.get("_last_execution_result"))
    if last_result is not None and st.session_state.get("_last_execution_batch") == batch_id:
        st.session_state["_last_execution_result"] = run_result_to_payload(last_result)
        st.divider()
        render_run_result(last_result, rules)

    with st.expander("Approved execution order", expanded=False):
        if plan:
            dataframe(pd.DataFrame(plan), height=min(700, 38 + len(plan) * 35))
        else:
            st.warning("No approved executable variants are enabled.")


# -----------------------------------------------------------------------------
# Streamlit pages: analyst workbench and reporting
# -----------------------------------------------------------------------------


def workbench_filters(store: SnowflakeRulesStore, batch_id: str) -> dict[str, Any]:
    with st.expander("Filters", expanded=True):
        first = st.columns([2.2, 1, 1, 1])
        search = first[0].text_input(
            "Search",
            key=f"wb_search_{batch_id}",
            placeholder="Case, vendor, DIN, MIN, description, rule…",
        )
        request_type = first[1].selectbox(
            "Request type",
            ["All", *store.distinct_row_values(batch_id, "request_type")],
            key=f"wb_type_{batch_id}",
        )
        outcome = first[2].selectbox(
            "Outcome",
            ["All", *store.distinct_row_values(batch_id, "outcome_reporting")],
            key=f"wb_outcome_{batch_id}",
        )
        review = first[3].selectbox("Needs review", ["All", "Yes", "No"], key=f"wb_review_{batch_id}")
        second = st.columns([1, 1, 1, 1])
        business = second[0].selectbox(
            "Business",
            ["All", *store.distinct_row_values(batch_id, "business")],
            key=f"wb_business_{batch_id}",
        )
        queue_bucket = second[1].selectbox(
            "Queue",
            ["All", *store.distinct_row_values(batch_id, "queue_bucket")],
            key=f"wb_queue_{batch_id}",
        )
        action = second[2].selectbox(
            "ACTION",
            ["All", *store.distinct_row_values(batch_id, "action")],
            key=f"wb_action_{batch_id}",
        )
        excluded = second[3].selectbox("Excluded", ["All", "Yes", "No"], key=f"wb_excluded_{batch_id}")
    return {
        "search": search,
        "request_type": request_type,
        "outcome_reporting": outcome,
        "needs_review": review,
        "business": business,
        "queue_bucket": queue_bucket,
        "action": action,
        "excluded": excluded,
    }


def render_trace_table(row: Mapping[str, Any]) -> None:
    traces = [item for item in (row.get("execution_trace") or []) if isinstance(item, Mapping)]
    if not traces:
        st.caption("No rule trace is recorded for this row.")
        return
    records = [
        {
            "Order": index,
            "Priority": int(trace.get("executionPriority") or 0),
            "Runtime Rule": clean_text(trace.get("runtimeRuleId")),
            "Rule": clean_text(trace.get("ruleId")),
            "Kind": clean_text(trace.get("runtimeKind")),
            "Action": clean_text(trace.get("actionSummary")),
            "Description": clean_text(trace.get("description")),
            "Matched": timestamp_text(trace.get("matchedAt"))[:19].replace("T", " "),
        }
        for index, trace in enumerate(traces, start=1)
    ]
    dataframe(pd.DataFrame(records), height=min(600, 38 + len(records) * 35))


def audit_table_records(events: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "When": timestamp_text(event.get("created_at"))[:19].replace("T", " "),
            "User": clean_text(event.get("user_name")),
            "Action": clean_text(event.get("action")).replace("_", " ").title(),
            "Entity": clean_text(event.get("entity_type")),
            "Changed Fields": ", ".join((event.get("details") or {}).get("changed_fields") or [])
            if isinstance(event.get("details"), Mapping)
            else "",
        }
        for event in events
    ]


def render_workbench_page(store: SnowflakeRulesStore, selected_batch: Mapping[str, Any] | None) -> None:
    render_page_header(
        "Analyst Workbench",
        "Filter workflow rows, review trace evidence, apply auditable overrides, and re-run an individual decision.",
        kicker="Human Review",
    )
    if not require_selected_batch(selected_batch):
        return
    batch_id = clean_text(selected_batch.get("id"))
    filters = workbench_filters(store, batch_id)
    filter_token = hashlib.sha256(json_dumps(filters).encode("utf-8")).hexdigest()[:10]
    total = store.row_count(batch_id, filters)
    pagination_columns = st.columns([1, 1, 3])
    page_size = int(pagination_columns[0].selectbox("Rows per page", [25, 50, 100, 250], index=1, key=f"wb_size_{batch_id}"))
    max_page = max(1, math.ceil(total / page_size))
    page_key = f"wb_page_value_{batch_id}_{filter_token}"
    stored_page = max(1, min(int(st.session_state.get(page_key, 1)), max_page))
    if st.session_state.get(page_key) != stored_page:
        st.session_state[page_key] = stored_page
    current_page = int(
        pagination_columns[1].number_input(
            "Page",
            min_value=1,
            max_value=max_page,
            value=stored_page,
            step=1,
            key=page_key,
        )
    )
    pagination_columns[2].markdown(
        f"<div style='padding-top:1.9rem'><strong>{total:,}</strong> matching rows · page {current_page:,} of {max_page:,}</div>",
        unsafe_allow_html=True,
    )
    rows = store.load_rows(batch_id, filters=filters, limit=page_size, offset=(current_page - 1) * page_size)
    if not rows:
        st.info("No rows match the current filters.")
        return
    dataframe(pd.DataFrame(row_table_records(rows)), height=min(650, 38 + len(rows) * 35))

    row_ids = [clean_text(row.get("id")) for row in rows]
    selected_row_id = st.selectbox(
        "Open row",
        row_ids,
        format_func=lambda value: next(
            (
                f"Row {row.get('source_row_number')} · {clean_text(row.get('case_number')) or 'No case'} · "
                f"{clean_text(row.get('vendor')) or 'No vendor'} · {clean_text(row.get('description'))[:90]}"
                for row in rows
                if clean_text(row.get("id")) == value
            ),
            value,
        ),
        key=f"wb_selected_row_{batch_id}_{filter_token}_{current_page}",
    )
    selected_row = next(row for row in rows if clean_text(row.get("id")) == selected_row_id)
    st.divider()
    heading_left, heading_right = st.columns([4, 1])
    with heading_left:
        st.subheader(
            f"Row {selected_row.get('source_row_number')} · {clean_text(selected_row.get('case_number')) or 'No case number'}"
        )
        st.caption(
            " · ".join(
                value
                for value in (
                    clean_text(selected_row.get("business")),
                    clean_text(selected_row.get("request_type")),
                    clean_text(selected_row.get("vendor")),
                    clean_text(selected_row.get("din")),
                    clean_text(selected_row.get("min")),
                )
                if value
            )
        )
    with heading_right:
        st.metric("Outcome", clean_text(selected_row.get("outcome_reporting")) or "Pending")

    decision_tab, trace_tab, data_tab, history_tab = st.tabs(
        ["Decision & override", "Execution trace", "Source & normalized data", "Audit history"]
    )
    with decision_tab:
        with st.form(f"row_override_{selected_row_id}"):
            first = st.columns(3)
            action_options = options_with_current(ACTION_OPTIONS, selected_row.get("action"))
            action = first[0].selectbox(
                "ACTION",
                action_options,
                index=action_options.index(clean_text(selected_row.get("action"))) if clean_text(selected_row.get("action")) in action_options else 0,
            )
            if_stock_options = options_with_current(IF_STOCK_OPTIONS, selected_row.get("if_in_stock_action"))
            if_stock = first[1].selectbox(
                "If In Stock: Action",
                if_stock_options,
                index=if_stock_options.index(clean_text(selected_row.get("if_in_stock_action"))) if clean_text(selected_row.get("if_in_stock_action")) in if_stock_options else 0,
            )
            buysmart_options = options_with_current(BUYSMART_OPTIONS, selected_row.get("buysmart_action"))
            buysmart = first[2].selectbox(
                "BuySmart Action",
                buysmart_options,
                index=buysmart_options.index(clean_text(selected_row.get("buysmart_action"))) if clean_text(selected_row.get("buysmart_action")) in buysmart_options else 0,
            )
            second = st.columns([1, 1, 2])
            needs_review = second[0].checkbox("Needs review", value=bool_value(selected_row.get("needs_review")))
            excluded = second[1].checkbox("Excluded", value=bool_value(selected_row.get("excluded")))
            queue_values = options_with_current(
                [bucket["label"] for bucket in BUCKETS],
                selected_row.get("queue_bucket"),
                include_blank=False,
            )
            queue_bucket = second[2].selectbox(
                "Assigned queue",
                queue_values,
                index=queue_values.index(clean_text(selected_row.get("queue_bucket")))
                if clean_text(selected_row.get("queue_bucket")) in queue_values
                else 0,
            )
            excluded_reason = st.text_input("Excluded reason", value=clean_text(selected_row.get("excluded_reason")))
            validation_status = st.text_area(
                "Validation status",
                value=clean_text(selected_row.get("validation_status")),
                help="Use semicolon-separated validation messages when recording multiple data issues.",
            )
            analyst_notes = st.text_area("Analyst notes", value=clean_text(selected_row.get("analyst_notes")), height=120)
            assignment = st.text_input("Assignment", value=clean_text(selected_row.get("assignment")))
            save_override = st.form_submit_button("Save auditable override", type="primary")
        if save_override:
            try:
                changes = {
                    "action": action,
                    "if_in_stock_action": if_stock,
                    "buysmart_action": buysmart,
                    "needs_review": needs_review,
                    "excluded": excluded,
                    "queue_bucket": queue_bucket,
                    "excluded_reason": excluded_reason,
                    "validation_status": validation_status,
                    "analyst_notes": analyst_notes,
                    "assignment": assignment,
                }
                updated, changed_fields = apply_analyst_changes(selected_row, changes)
                if not changed_fields:
                    st.info("No decision fields changed.")
                else:
                    store.save_row_override(updated, before=selected_row, changed_fields=changed_fields)
                    set_flash(
                        f"Saved row {selected_row.get('source_row_number')} override: {', '.join(changed_fields)}."
                    )
                    safe_rerun()
            except Exception as exc:
                render_actionable_exception(
                    "The analyst override could not be saved.",
                    exc,
                    component="Analyst override save",
                    context={
                        "batch_id": batch_id,
                        "workflow_row_id": selected_row_id,
                        "source_row_number": selected_row.get("source_row_number"),
                    },
                )

        run_columns = st.columns(2)
        if run_columns[0].button("Dry-run this row", key=f"wb_dry_{selected_row_id}"):
            try:
                result = run_batch(store, batch_id, dry_run=True, row_ids=[selected_row_id])
                st.session_state["_workbench_run_result"] = run_result_to_payload(result)
                st.session_state["_workbench_run_row"] = selected_row_id
            except Exception as exc:
                render_actionable_exception(
                    "Row simulation failed.",
                    exc,
                    component="Analyst row dry run",
                    context={"batch_id": batch_id, "workflow_row_id": selected_row_id},
                )
        if run_columns[1].button("Execute rules on this row", type="primary", key=f"wb_run_{selected_row_id}"):
            try:
                result = run_batch(store, batch_id, dry_run=False, row_ids=[selected_row_id])
                set_flash(
                    f"Executed row {selected_row.get('source_row_number')}; {int(result.run.get('changed_row_count') or 0)} decision changed."
                )
                safe_rerun()
            except Exception as exc:
                render_actionable_exception(
                    "Rule execution for the selected row failed.",
                    exc,
                    component="Analyst row execution",
                    context={"batch_id": batch_id, "workflow_row_id": selected_row_id},
                )
        workbench_result = run_result_from_payload(st.session_state.get("_workbench_run_result"))
        if workbench_result is not None and st.session_state.get("_workbench_run_row") == selected_row_id:
            st.session_state["_workbench_run_result"] = run_result_to_payload(workbench_result)
            st.divider()
            render_run_result(workbench_result, store.load_rules())

    with trace_tab:
        render_trace_table(selected_row)
        st.markdown("#### Final decision")
        decision_record = {
            "ACTION": clean_text(selected_row.get("action")),
            "If In Stock": clean_text(selected_row.get("if_in_stock_action")),
            "BuySmart": clean_text(selected_row.get("buysmart_action")),
            "Outcome": clean_text(selected_row.get("outcome_reporting")),
            "Queue": clean_text(selected_row.get("queue_bucket")),
            "Validation": clean_text(selected_row.get("validation_status")),
            "Review": bool_value(selected_row.get("needs_review")),
            "Excluded": bool_value(selected_row.get("excluded")),
        }
        dataframe(pd.DataFrame([decision_record]))

    with data_tab:
        raw_column, normalized_column = st.columns(2)
        with raw_column:
            st.markdown("#### Original source row")
            st.json(selected_row.get("raw_row") or {})
        with normalized_column:
            st.markdown("#### Normalized fields and derived flags")
            st.json(selected_row.get("normalized_row") or {})

    with history_tab:
        events = store.list_audit(entity_id=selected_row_id, limit=100)
        if events:
            dataframe(pd.DataFrame(audit_table_records(events)))
            selected_event_index = st.selectbox(
                "Inspect event",
                range(len(events)),
                format_func=lambda index: (
                    f"{timestamp_text(events[index].get('created_at'))[:19].replace('T', ' ')} · "
                    f"{clean_text(events[index].get('action')).replace('_', ' ').title()} · "
                    f"{clean_text(events[index].get('user_name'))}"
                ),
                key=f"wb_event_{selected_row_id}",
            )
            event = events[selected_event_index]
            before_column, after_column = st.columns(2)
            with before_column:
                st.markdown("#### Before")
                st.json(event.get("before"))
            with after_column:
                st.markdown("#### After")
                st.json(event.get("after"))
        else:
            st.caption("No analyst audit events are recorded for this row.")


def count_chart(values: Mapping[str, int], label: str = "Count") -> Any:
    if not values:
        return pd.DataFrame(columns=[label])
    return pd.DataFrame({label: list(values.values())}, index=list(values.keys()))


def result_change_records(results: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for result in results:
        before = result.get("before_state") if isinstance(result.get("before_state"), Mapping) else {}
        after = result.get("after_state") if isinstance(result.get("after_state"), Mapping) else {}
        records.append(
            {
                "Source Row": after.get("source_row_number"),
                "Case#": clean_text(after.get("case_number")),
                "Vendor": clean_text(after.get("vendor")),
                "Before ACTION": clean_text(before.get("action")),
                "After ACTION": clean_text(after.get("action")),
                "Before BuySmart": clean_text(before.get("buysmart_action")),
                "After BuySmart": clean_text(after.get("buysmart_action")),
                "Outcome": clean_text(after.get("outcome_reporting")),
                "Rules": ", ".join(result.get("rules_applied") or []),
            }
        )
    return records


def render_reports_page(store: SnowflakeRulesStore, selected_batch: Mapping[str, Any] | None) -> None:
    render_page_header(
        "Reports & Exports",
        "Analyze compliance outcomes, inspect execution runs, and export both final decisions and applied-rule evidence.",
        kicker="Evidence",
    )
    if not require_selected_batch(selected_batch):
        return
    batch_id = clean_text(selected_batch.get("id"))
    rows = store.load_rows(batch_id)
    rules = store.load_rules()
    summary = summarize_batch(rows)
    metrics = st.columns(7)
    metrics[0].metric("Rows", f"{summary['row_count']:,}")
    metrics[1].metric("Coverage", f"{summary['automation_coverage_pct']:.1f}%")
    metrics[2].metric("Approved", f"{summary['approved_count']:,}")
    metrics[3].metric("Denied", f"{summary['denied_count']:,}")
    metrics[4].metric("Assigned", f"{summary['assigned_count']:,}")
    metrics[5].metric("Review", f"{summary['review_count']:,}")
    metrics[6].metric("Excluded", f"{summary['excluded_count']:,}")

    export_columns = st.columns([1, 1, 3])
    export_columns[0].download_button(
        "Download outcomes CSV",
        data=export_csv(rows, rules),
        file_name=safe_download_filename(selected_batch.get("name"), ".csv", "rules_outcomes"),
        mime="text/csv",
        key=f"report_csv_{batch_id}",
        use_container_width=True,
    )
    export_columns[1].download_button(
        "Download evidence XLSX",
        data=export_xlsx(rows, rules),
        file_name=safe_download_filename(selected_batch.get("name"), ".xlsx", "rules_outcomes"),
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key=f"report_xlsx_{batch_id}",
        use_container_width=True,
    )
    export_columns[2].caption(
        "The XLSX contains an Outcomes worksheet plus a row-per-rule Applied Rules worksheet for trace-level evidence."
    )

    chart_left, chart_middle, chart_right = st.columns(3)
    with chart_left:
        st.markdown("#### Outcomes")
        if summary["outcome_counts"]:
            st.bar_chart(count_chart(summary["outcome_counts"]))
        else:
            st.caption("No outcomes available.")
    with chart_middle:
        st.markdown("#### Request types")
        if summary["type_counts"]:
            st.bar_chart(count_chart(summary["type_counts"]))
        else:
            st.caption("No request types available.")
    with chart_right:
        st.markdown("#### Businesses")
        if summary["business_counts"]:
            st.bar_chart(count_chart(summary["business_counts"]))
        else:
            st.caption("No business values available.")

    st.subheader("Compliance buckets")
    bucket_records = [
        {
            "Bucket": item["label"],
            "Rows": item["count"],
            "Needs Review": item["review_count"],
            "Outcomes": ", ".join(item["outcome_keys"]),
            "Applied Rules": ", ".join(item["rule_ids"]),
            "Purpose": item["description"],
        }
        for item in summary["bucket_summaries"]
    ]
    if bucket_records:
        dataframe(pd.DataFrame(bucket_records), height=min(650, 38 + len(bucket_records) * 35))

    st.subheader("Execution history")
    runs = store.list_runs(batch_id=batch_id, limit=250)
    if not runs:
        st.caption("No committed runs have been recorded for this batch.")
        return
    run_records = [
        {
            "Completed": timestamp_text(run.get("completed_at"))[:19].replace("T", " "),
            "Mode": clean_text(run.get("mode")).replace("_", " ").title(),
            "Evaluated": int(run.get("selected_row_count") or 0),
            "Changed": int(run.get("changed_row_count") or 0),
            "Review": int(run.get("review_row_count") or 0),
            "User": clean_text(run.get("executed_by")),
            "Run ID": clean_text(run.get("id")),
        }
        for run in runs
    ]
    dataframe(pd.DataFrame(run_records), height=min(520, 38 + len(run_records[:20]) * 35))
    selected_run_id = st.selectbox(
        "Inspect run evidence",
        [clean_text(run.get("id")) for run in runs],
        format_func=lambda value: next(
            (
                f"{timestamp_text(run.get('completed_at'))[:19].replace('T', ' ')} · "
                f"{clean_text(run.get('mode')).replace('_', ' ').title()} · "
                f"{int(run.get('changed_row_count') or 0):,} changed"
                for run in runs
                if clean_text(run.get("id")) == value
            ),
            value,
        ),
        key=f"report_run_{batch_id}",
    )
    selected_run = next(run for run in runs if clean_text(run.get("id")) == selected_run_id)
    run_details, run_results_tab = st.tabs(["Run metadata", "Changed-row evidence"])
    with run_details:
        st.json(selected_run)
    with run_results_tab:
        results = store.load_run_results(selected_run_id)
        if results:
            dataframe(pd.DataFrame(result_change_records(results)), height=min(650, 38 + len(results[:50]) * 35))
            result_index = st.selectbox(
                "Inspect before/after result",
                range(len(results)),
                format_func=lambda index: (
                    f"Row {(results[index].get('after_state') or {}).get('source_row_number')} · "
                    f"{clean_text((results[index].get('after_state') or {}).get('case_number')) or 'No case'}"
                ),
                key=f"report_result_{selected_run_id}",
            )
            chosen = results[result_index]
            before_column, after_column = st.columns(2)
            with before_column:
                st.markdown("#### Before")
                st.json(chosen.get("before_state"))
            with after_column:
                st.markdown("#### After")
                st.json(chosen.get("after_state"))
        else:
            st.caption("This run produced no changed-row result records.")


# -----------------------------------------------------------------------------
# Streamlit pages: rule catalog and simulator
# -----------------------------------------------------------------------------


def rule_catalog_records(rules: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for rule in rules:
        variants = [item for item in (rule.get("variants") or []) if isinstance(item, Mapping)]
        executable = [item for item in variants if bool_value(item.get("is_executable"))]
        enabled = [item for item in executable if bool_value(item.get("enabled")) and clean_text(item.get("status")) == "approved"]
        priorities = [int(item.get("execution_priority") or 0) for item in variants]
        records.append(
            {
                "Rule ID": clean_text(rule.get("rule_id")),
                "Name": clean_text(rule.get("name")),
                "Group": clean_text(rule.get("rule_group")),
                "Business": clean_text(rule.get("business_scope")),
                "Request Types": ", ".join(rule.get("request_types") or []),
                "Status": clean_text(rule.get("status")),
                "Automation": clean_text(rule.get("automation_level")),
                "Priority": min(priorities) if priorities else 0,
                "Variants": len(variants),
                "Executable": len(executable),
                "Enabled": len(enabled),
                "Bundled": is_bundled_rule(rule),
            }
        )
    return records


def variant_records(rule: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "Runtime Rule": clean_text(variant.get("runtime_rule_id")),
            "Priority": int(variant.get("execution_priority") or 0),
            "Kind": clean_text(variant.get("runtime_kind")),
            "Status": clean_text(variant.get("status")),
            "Automation": clean_text(variant.get("automation_level")),
            "Executable": bool_value(variant.get("is_executable")),
            "Enabled": bool_value(variant.get("enabled")),
            "Stop": bool_value(variant.get("stop_processing")),
            "Filter": filter_logic_text(variant.get("predicate_json"))
            if isinstance(variant.get("predicate_json"), Mapping)
            else clean_text((variant.get("source") or {}).get("fieldFilterLogic")),
            "Actions": aggregate_logic_text(variant.get("action_json") or [])
            if isinstance(variant.get("action_json"), list)
            else clean_text((variant.get("source") or {}).get("aggregateLogic")),
        }
        for variant in (rule.get("variants") or [])
        if isinstance(variant, Mapping)
    ]


def render_rule_detail(store: SnowflakeRulesStore, rule: Mapping[str, Any]) -> None:
    variants = [item for item in (rule.get("variants") or []) if isinstance(item, Mapping)]
    enabled = any(bool_value(item.get("enabled")) for item in variants if bool_value(item.get("is_executable")))
    st.subheader(f"{clean_text(rule.get('rule_id'))} · {clean_text(rule.get('name'))}")
    metadata_columns = st.columns(5)
    metadata_columns[0].metric("Status", clean_text(rule.get("status")).title())
    metadata_columns[1].metric("Automation", clean_text(rule.get("automation_level")).title())
    metadata_columns[2].metric("Version", int(rule.get("version_number") or 1))
    metadata_columns[3].metric("Variants", len(variants))
    metadata_columns[4].metric("Origin", "Bundled DAF" if is_bundled_rule(rule) else "User managed")
    if clean_text(rule.get("notes")):
        st.caption(clean_text(rule.get("notes")))

    controls = st.columns([1, 1, 1, 3])
    toggle_label = "Disable executable variants" if enabled else "Enable executable variants"
    if controls[0].button(toggle_label, key=f"toggle_rule_{clean_text(rule.get('rule_id'))}"):
        try:
            updated = set_rule_enabled(rule, not enabled)
            persist_rule_change(store, rule, updated, "enable_rule" if not enabled else "disable_rule")
            set_flash(f"{clean_text(rule.get('rule_id'))} is now {'enabled' if not enabled else 'disabled'}.")
            safe_rerun()
        except Exception as exc:
            render_actionable_exception(
                "Rule status could not be changed.",
                exc,
                component="Rule enable/disable",
                context={"rule_id": clean_text(rule.get("rule_id")), "requested_enabled": not enabled},
            )
    if is_bundled_rule(rule):
        if controls[1].button("Restore bundled definition", key=f"restore_rule_{clean_text(rule.get('rule_id'))}"):
            try:
                store.restore_bundled_rule(clean_text(rule.get("rule_id")))
                set_flash(f"Restored {clean_text(rule.get('rule_id'))} from the embedded DAF catalog.")
                safe_rerun()
            except Exception as exc:
                render_actionable_exception(
                    "Bundled rule could not be restored.",
                    exc,
                    component="Bundled rule restore",
                    context={"rule_id": clean_text(rule.get("rule_id"))},
                )
    else:
        delete_confirmed = controls[1].checkbox("Confirm delete", key=f"delete_confirm_{clean_text(rule.get('rule_id'))}")
        if controls[2].button(
            "Delete user rule",
            disabled=not delete_confirmed,
            key=f"delete_rule_{clean_text(rule.get('rule_id'))}",
        ):
            try:
                store.delete_user_rule(clean_text(rule.get("rule_id")))
                set_flash(f"Deleted user rule {clean_text(rule.get('rule_id'))}.")
                safe_rerun()
            except Exception as exc:
                render_actionable_exception(
                    "User rule could not be deleted.",
                    exc,
                    component="User rule deletion",
                    context={"rule_id": clean_text(rule.get("rule_id"))},
                )

    st.markdown("#### Variants")
    records = variant_records(rule)
    dataframe(pd.DataFrame(records), height=min(650, 38 + len(records) * 35))
    if variants:
        variant_ids = [clean_text(item.get("runtime_rule_id")) for item in variants]
        selected_variant_id = st.selectbox(
            "Inspect variant",
            variant_ids,
            key=f"inspect_variant_{clean_text(rule.get('rule_id'))}",
        )
        variant = next(item for item in variants if clean_text(item.get("runtime_rule_id")) == selected_variant_id)
        predicate_column, action_column = st.columns(2)
        with predicate_column:
            st.markdown("##### Predicate")
            if isinstance(variant.get("predicate_json"), Mapping):
                st.code(json_dumps(variant.get("predicate_json"), pretty=True), language="json")
            else:
                st.info("This guided/manual DAF variant has no executable predicate.")
        with action_column:
            st.markdown("##### Actions")
            if isinstance(variant.get("action_json"), list):
                st.code(json_dumps(variant.get("action_json"), pretty=True), language="json")
            else:
                st.info("This guided/manual DAF variant has no executable action sequence.")
        with st.expander("Source DAF metadata"):
            st.json(variant.get("source") or {})


def editor_column_config() -> tuple[dict[str, Any], dict[str, Any]]:
    if st is None or not hasattr(st, "column_config"):
        return {}, {}
    filter_config = {
        "field": st.column_config.SelectboxColumn("Field", options=list(FIELD_LABELS.keys()), required=True),
        "op": st.column_config.SelectboxColumn("Operator", options=list(OPERATOR_LABELS.keys()), required=True),
        "value": st.column_config.TextColumn("Value"),
    }
    action_config = {
        "type": st.column_config.SelectboxColumn("Action type", options=USER_ACTION_TYPES, required=True),
        "value": st.column_config.TextColumn("Value"),
        "reason": st.column_config.TextColumn("Reason (for exclusion)"),
    }
    return filter_config, action_config


def rule_builder_defaults(rule: Mapping[str, Any] | None) -> dict[str, Any]:
    if not rule:
        return {
            "rule_id": "",
            "name": "",
            "rule_group": "User Managed",
            "business_scope": "All",
            "request_types": "PRF, SORF, SRF",
            "priority": USER_RULE_PRIORITY_FLOOR,
            "runtime_kind": "row_rule",
            "enabled": True,
            "stop_processing": False,
            "predicate": {"field": "request_type_key", "op": "in", "value": ["PRF", "SORF", "SRF"]},
            "actions": [{"type": "set_review", "value": True}],
            "notes": "",
        }
    variants = [item for item in (rule.get("variants") or []) if isinstance(item, Mapping)]
    primary = variants[0] if variants else {}
    return {
        "rule_id": clean_text(rule.get("rule_id")),
        "name": clean_text(rule.get("name")),
        "rule_group": clean_text(rule.get("rule_group")) or "User Managed",
        "business_scope": clean_text(rule.get("business_scope")) or "All",
        "request_types": ", ".join(rule.get("request_types") or []),
        "priority": int(primary.get("execution_priority") or USER_RULE_PRIORITY_FLOOR),
        "runtime_kind": clean_text(primary.get("runtime_kind")) or "row_rule",
        "enabled": bool_value(primary.get("enabled")),
        "stop_processing": bool_value(primary.get("stop_processing")),
        "predicate": deepcopy(primary.get("predicate_json") or {}),
        "actions": deepcopy(primary.get("action_json") or []),
        "notes": clean_text(rule.get("notes")),
    }


def render_rule_builder(store: SnowflakeRulesStore, rules: Sequence[Mapping[str, Any]], preferred_rule_id: str = "") -> None:
    editable_rules = [rule for rule in rules if not is_bundled_rule(rule)]
    options = ["__new__", *[clean_text(rule.get("rule_id")) for rule in editable_rules]]
    default_target = preferred_rule_id if preferred_rule_id in options else "__new__"
    target = st.selectbox(
        "Builder target",
        options,
        index=options.index(default_target),
        format_func=lambda value: "Create a new user rule" if value == "__new__" else f"Edit {value}",
        key="rule_builder_target",
    )
    existing = next((rule for rule in editable_rules if clean_text(rule.get("rule_id")) == target), None)
    defaults = rule_builder_defaults(existing)
    simple = predicate_is_simple(defaults["predicate"])
    mode = st.radio(
        "Predicate editor",
        ["Structured filters", "Advanced JSON"],
        index=0 if simple else 1,
        horizontal=True,
        key=f"rule_editor_mode_{target}",
    )
    if mode == "Structured filters":
        st.caption(
            "Structured mode supports a flat ALL/ANY group. Use Advanced JSON for nested all/any/not predicates. "
            "Valid fields: " + ", ".join(FIELD_LABELS.keys())
        )
    else:
        st.caption("Advanced mode accepts the same predicate and action JSON consumed by the execution engine.")

    filter_rows, join = filters_from_simple_predicate(defaults["predicate"] if simple else None)
    action_rows = action_rows_from_json(defaults["actions"])
    filter_config, action_config = editor_column_config()
    form_key = f"rule_builder_form_{target}_{mode.replace(' ', '_')}"
    with st.form(form_key):
        metadata = st.columns([1, 2, 1, 1])
        requested_rule_id = metadata[0].text_input(
            "Rule ID",
            value=defaults["rule_id"],
            disabled=existing is not None,
            help="Leave blank to assign the next U### identifier.",
        )
        name = metadata[1].text_input("Rule name", value=defaults["name"])
        rule_group = metadata[2].text_input("Rule group", value=defaults["rule_group"])
        priority = int(metadata[3].number_input("Execution priority", min_value=1, value=int(defaults["priority"]), step=1))
        scope_columns = st.columns([1, 2, 1])
        business_scope = scope_columns[0].text_input("Business scope", value=defaults["business_scope"])
        request_types = scope_columns[1].text_input("Request types", value=defaults["request_types"])
        runtime_kind_options = list(RUNTIME_KIND_ORDER.keys())
        runtime_kind_current = defaults["runtime_kind"]
        if runtime_kind_current not in runtime_kind_options:
            runtime_kind_options.append(runtime_kind_current)
        runtime_kind = scope_columns[2].selectbox(
            "Runtime kind",
            runtime_kind_options,
            index=runtime_kind_options.index(runtime_kind_current),
        )
        flag_columns = st.columns(2)
        enabled = flag_columns[0].checkbox("Enabled and approved", value=bool(defaults["enabled"]))
        stop_processing = flag_columns[1].checkbox("Stop processing after a match", value=bool(defaults["stop_processing"]))
        notes = st.text_area("Notes", value=defaults["notes"], height=90)

        if mode == "Structured filters":
            join_mode = st.radio(
                "Match",
                ["all", "any"],
                index=0 if join == "all" else 1,
                horizontal=True,
            )
            st.markdown("##### Filters")
            filters_frame = st.data_editor(
                pd.DataFrame(filter_rows, columns=["field", "op", "value"]),
                num_rows="dynamic",
                use_container_width=True,
                hide_index=True,
                column_config=filter_config or None,
                key=f"rule_filters_{target}",
            )
            st.markdown("##### Actions")
            actions_frame = st.data_editor(
                pd.DataFrame(action_rows, columns=["type", "value", "reason"]),
                num_rows="dynamic",
                use_container_width=True,
                hide_index=True,
                column_config=action_config or None,
                key=f"rule_actions_{target}",
            )
            predicate_text = ""
            actions_text = ""
        else:
            join_mode = "all"
            filters_frame = pd.DataFrame()
            actions_frame = pd.DataFrame()
            predicate_text = st.text_area(
                "Predicate JSON",
                value=json_dumps(defaults["predicate"], pretty=True),
                height=260,
            )
            actions_text = st.text_area(
                "Action JSON",
                value=json_dumps(defaults["actions"], pretty=True),
                height=220,
            )
        submitted = st.form_submit_button("Save rule", type="primary")
    if not submitted:
        return
    try:
        if mode == "Structured filters":
            filter_records = [
                record
                for record in filters_frame.to_dict("records")
                if any(clean_text(value) for value in record.values())
            ]
            action_records = [
                record
                for record in actions_frame.to_dict("records")
                if any(clean_text(value) for value in record.values())
            ]
            predicate = predicate_from_filter_rows(filter_records, join_mode)
            actions = action_json_from_rows(action_records)
        else:
            predicate = json.loads(predicate_text)
            actions = json.loads(actions_text)
            if not isinstance(predicate, Mapping):
                raise ValueError("Predicate JSON must be an object.")
            if not isinstance(actions, list):
                raise ValueError("Action JSON must be an array.")
            validate_predicate_definition(predicate)
            validate_action_definition(actions)
        request = {
            "rule_id": requested_rule_id,
            "name": name,
            "rule_group": rule_group,
            "business_scope": business_scope,
            "request_types": request_types,
            "execution_priority": priority,
            "runtime_kind": runtime_kind,
            "enabled": enabled,
            "stop_processing": stop_processing,
            "predicate_json": predicate,
            "action_json": actions,
            "notes": notes,
        }
        if existing is None:
            saved = create_user_rule(request, rules)
            persist_rule_change(store, None, saved, "create_rule")
            message = f"Created user rule {clean_text(saved.get('rule_id'))}."
        else:
            saved = update_rule(existing, request)
            persist_rule_change(store, existing, saved, "update_rule")
            message = f"Updated {clean_text(saved.get('rule_id'))} to version {int(saved.get('version_number') or 1)}."
        set_flash(message)
        safe_rerun()
    except Exception as exc:
        render_actionable_exception(
            "Rule validation or persistence failed.",
            exc,
            component="Rule builder save",
            context={
                "builder_target": target,
                "requested_rule_id": requested_rule_id,
                "editing_existing_rule": existing is not None,
                "editor_mode": mode,
                "execution_priority": priority,
                "runtime_kind": runtime_kind,
            },
        )


def render_rules_catalog_page(store: SnowflakeRulesStore) -> None:
    render_page_header(
        "Rules Catalog",
        "Inspect the embedded DAF catalog, control executable variants, and create versioned user-managed rules.",
        kicker="Governance",
    )
    rules = store.load_rules()
    catalog_tab, builder_tab, audit_tab = st.tabs(["Catalog", "Create / edit user rule", "Rule audit"])
    selected_rule_id = ""
    with catalog_tab:
        controls = st.columns([2, 1, 1, 1])
        search = controls[0].text_input("Search rules", placeholder="ID, name, group, notes, business…", key="rules_search")
        statuses = ["All", *sorted({clean_text(rule.get("status")) for rule in rules if clean_text(rule.get("status"))})]
        status = controls[1].selectbox("Status", statuses, key="rules_status")
        automations = ["All", *sorted({clean_text(rule.get("automation_level")) for rule in rules if clean_text(rule.get("automation_level"))})]
        automation = controls[2].selectbox("Automation", automations, key="rules_automation")
        origins = controls[3].selectbox("Origin", ["All", "Bundled DAF", "User managed"], key="rules_origin")
        filtered: list[dict[str, Any]] = []
        query = search.lower().strip()
        for rule in rules:
            haystack = " ".join(
                clean_text(rule.get(key))
                for key in ("rule_id", "name", "rule_group", "business_scope", "notes", "discovery_reference")
            ).lower()
            if query and query not in haystack:
                continue
            if status != "All" and clean_text(rule.get("status")) != status:
                continue
            if automation != "All" and clean_text(rule.get("automation_level")) != automation:
                continue
            if origins == "Bundled DAF" and not is_bundled_rule(rule):
                continue
            if origins == "User managed" and is_bundled_rule(rule):
                continue
            filtered.append(rule)
        summary_columns = st.columns(5)
        summary_columns[0].metric("Definitions", len(rules))
        summary_columns[1].metric("Visible", len(filtered))
        summary_columns[2].metric("Executable variants", len(executable_variants(rules)))
        summary_columns[3].metric("Bundled", sum(is_bundled_rule(rule) for rule in rules))
        summary_columns[4].metric("User managed", sum(not is_bundled_rule(rule) for rule in rules))
        if not filtered:
            st.info("No rules match the current catalog filters.")
        else:
            dataframe(pd.DataFrame(rule_catalog_records(filtered)), height=min(720, 38 + len(filtered) * 35))
            selected_rule_id = st.selectbox(
                "Open rule",
                [clean_text(rule.get("rule_id")) for rule in filtered],
                format_func=lambda value: next(
                    f"{value} · {clean_text(rule.get('name'))}" for rule in filtered if clean_text(rule.get("rule_id")) == value
                ),
                key="rules_selected_rule",
            )
            selected_rule = next(rule for rule in filtered if clean_text(rule.get("rule_id")) == selected_rule_id)
            st.divider()
            render_rule_detail(store, selected_rule)
    with builder_tab:
        render_rule_builder(store, rules, preferred_rule_id=selected_rule_id)
    with audit_tab:
        events = [event for event in store.list_audit(limit=500) if clean_text(event.get("entity_type")) in {"rule", "rule_catalog"}]
        if events:
            dataframe(pd.DataFrame(audit_table_records(events)), height=min(700, 38 + len(events[:50]) * 35))
            event_index = st.selectbox(
                "Inspect rule event",
                range(len(events)),
                format_func=lambda index: (
                    f"{timestamp_text(events[index].get('created_at'))[:19].replace('T', ' ')} · "
                    f"{clean_text(events[index].get('action')).replace('_', ' ').title()} · "
                    f"{clean_text(events[index].get('entity_id'))}"
                ),
                key="rule_audit_event",
            )
            st.json(events[event_index])
        else:
            st.caption("No rule governance events have been recorded.")


SIMULATION_PRESETS = {
    "Compass PRF — permanent catalog item": {
        "Business": "Compass USA",
        "Type": "PRF",
        "Case#": "SIM-001",
        "Vendor": "National Vendor",
        "DIN": "100001",
        "MIN": "200001",
        "Description": "Permanent catalog item",
        "One-Time or Permanent": "Permanent",
        "In CAT": "Y",
        "ACTION": "OK",
    },
    "Local vendor exclusion": {
        "Business": "Compass USA",
        "Type": "PRF",
        "Case#": "SIM-002",
        "Vendor": "Baldor",
        "DIN": "100002",
        "MIN": "200002",
        "Description": "Local vendor item",
        "One-Time or Permanent": "Permanent",
    },
    "Missing DIN and MIN": {
        "Business": "Compass USA",
        "Type": "PRF",
        "Case#": "SIM-003",
        "Vendor": "National Vendor",
        "DIN": "",
        "MIN": "",
        "Description": "Incomplete item request",
        "One-Time or Permanent": "Permanent",
    },
    "One-time low usage": {
        "Business": "Compass USA",
        "Type": "PRF",
        "Case#": "SIM-004",
        "Vendor": "National Vendor",
        "DIN": "100004",
        "MIN": "200004",
        "Description": "Low-use one-time item",
        "Usage": 10,
        "One-Time or Permanent": "One-Time",
        "In CAT": "N",
    },
}


def simulation_decision_record(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "ACTION": clean_text(row.get("action")),
        "If In Stock": clean_text(row.get("if_in_stock_action")),
        "BuySmart": clean_text(row.get("buysmart_action")),
        "Outcome": clean_text(row.get("outcome_reporting")),
        "Queue": clean_text(row.get("queue_bucket")),
        "Needs Review": bool_value(row.get("needs_review")),
        "Validation": clean_text(row.get("validation_status")),
        "Excluded": bool_value(row.get("excluded")),
        "Rules": clean_text(row.get("rule_applied")),
    }


def render_simulator_page(store: SnowflakeRulesStore) -> None:
    render_page_header(
        "Rule Simulator",
        "Exercise all approved rules or isolate one definition against a synthetic row without writing to Snowflake.",
        kicker="Safe Testing",
    )
    rules = store.load_rules()
    preset_name = st.selectbox("Scenario preset", list(SIMULATION_PRESETS), key="sim_preset")
    preset = deepcopy(SIMULATION_PRESETS[preset_name])
    input_mode = st.radio("Input editor", ["Form", "Raw JSON"], horizontal=True, key="sim_input_mode")
    scope_options = ["All approved rules", *[clean_text(rule.get("rule_id")) for rule in rules]]
    scope = st.selectbox(
        "Catalog scope",
        scope_options,
        format_func=lambda value: value
        if value == "All approved rules"
        else next(f"{value} · {clean_text(rule.get('name'))}" for rule in rules if clean_text(rule.get("rule_id")) == value),
        key="sim_rule_scope",
    )
    with st.form(f"simulator_form_{input_mode}_{preset_name}_{scope}"):
        if input_mode == "Form":
            first = st.columns(4)
            business = first[0].text_input("Business", value=clean_text(preset.get("Business")))
            request_type = first[1].text_input("Type", value=clean_text(preset.get("Type")))
            case_number = first[2].text_input("Case#", value=clean_text(preset.get("Case#")))
            vendor = first[3].text_input("Vendor", value=clean_text(preset.get("Vendor")))
            second = st.columns(4)
            din = second[0].text_input("DIN", value=clean_text(preset.get("DIN")))
            min_value = second[1].text_input("MIN", value=clean_text(preset.get("MIN")))
            duration = second[2].text_input(
                "One-Time or Permanent",
                value=clean_text(preset.get("One-Time or Permanent")),
            )
            usage = second[3].number_input(
                "Usage",
                min_value=0.0,
                value=float(parse_number(preset.get("Usage")) or 0.0),
                step=1.0,
            )
            description = st.text_input("Description", value=clean_text(preset.get("Description")))
            third = st.columns(4)
            in_cat = third[0].text_input("In CAT", value=clean_text(preset.get("In CAT")))
            pantry = third[1].text_input("Pantry", value=clean_text(preset.get("Pantry")))
            meets_criteria = third[2].text_input("Meets Criteria", value=clean_text(preset.get("Meets Criteria")))
            upstream_action = third[3].text_input("Upstream ACTION", value=clean_text(preset.get("ACTION")))
            raw_json = ""
        else:
            raw_json = st.text_area("Source row JSON", value=json_dumps(preset, pretty=True), height=420)
            business = request_type = case_number = vendor = din = min_value = duration = description = in_cat = pantry = meets_criteria = upstream_action = ""
            usage = 0.0
        simulate = st.form_submit_button("Simulate", type="primary")
    if simulate:
        try:
            if input_mode == "Raw JSON":
                raw_row = json.loads(raw_json)
                if not isinstance(raw_row, Mapping):
                    raise ValueError("Source row JSON must be an object.")
            else:
                raw_row = {
                    "Business": business,
                    "Type": request_type,
                    "Case#": case_number,
                    "Vendor": vendor,
                    "DIN": din,
                    "MIN": min_value,
                    "Description": description,
                    "Usage": usage,
                    "One-Time or Permanent": duration,
                    "Meets Criteria": meets_criteria,
                    "In CAT": in_cat,
                    "Pantry": pantry,
                    "ACTION": upstream_action,
                }
            before = create_workflow_row("simulation", raw_row, 2)
            scoped_rules = rules if scope == "All approved rules" else [rule for rule in rules if clean_text(rule.get("rule_id")) == scope]
            after = execute_row(before, executable_variants(scoped_rules), store.load_reference_lists())
            st.session_state["_simulation_result"] = {"before": before, "after": after, "scope": scope}
        except Exception as exc:
            render_actionable_exception(
                "Rule simulation failed.",
                exc,
                component="Rule simulator",
                context={"preset": preset_name, "input_mode": input_mode, "rule_scope": scope},
            )
    result = st.session_state.get("_simulation_result")
    if isinstance(result, Mapping):
        before = result.get("before") or {}
        after = result.get("after") or {}
        st.divider()
        st.subheader("Simulation result")
        before_column, after_column = st.columns(2)
        with before_column:
            st.markdown("#### Before")
            dataframe(pd.DataFrame([simulation_decision_record(before)]))
        with after_column:
            st.markdown("#### After")
            dataframe(pd.DataFrame([simulation_decision_record(after)]))
        trace_tab, context_tab, json_tab = st.tabs(["Trace", "Evaluation context", "Full row JSON"])
        with trace_tab:
            render_trace_table(after)
        with context_tab:
            st.json(context_for_row(after))
        with json_tab:
            st.json(after)


# -----------------------------------------------------------------------------
# Streamlit page: operational settings
# -----------------------------------------------------------------------------


def health_count_records(health: Mapping[str, Any]) -> list[dict[str, Any]]:
    target_tables = ((health.get("target") or {}).get("tables") or {})
    return [
        {
            "Object": key.replace("_", " ").title(),
            "Rows": value,
            "Table": clean_text(target_tables.get(key)) or f"{TARGET_DATABASE}.{TARGET_SCHEMA}.{TABLE_PREFIX}_{TABLE_SUFFIXES[key]}",
        }
        for key, value in (health.get("counts") or {}).items()
    ]


def render_reference_list_settings(store: SnowflakeRulesStore) -> None:
    references = store.load_reference_lists(include_defaults=False)
    choices = ["__new__", *sorted(references)]
    selected = st.selectbox(
        "Reference list",
        choices,
        format_func=lambda value: "Create a new list" if value == "__new__" else value,
        key="settings_reference_choice",
    )
    current_name = "" if selected == "__new__" else selected
    current_values = references.get(current_name, [])
    with st.form(f"reference_form_{selected}"):
        list_name = st.text_input("List name", value=current_name, disabled=selected != "__new__")
        values_text = st.text_area(
            "Values — one per line",
            value="\n".join(current_values),
            height=260,
        )
        notes = st.text_input("Change note", value="Managed in Streamlit settings")
        save = st.form_submit_button("Save reference list", type="primary")
    if save:
        try:
            values = [line.strip() for line in values_text.splitlines() if line.strip()]
            store.replace_reference_list(list_name or current_name, values, notes=notes)
            set_flash(f"Saved reference list {list_name or current_name} with {len(values):,} value(s).")
            safe_rerun()
        except Exception as exc:
            render_actionable_exception(
                "Reference list could not be saved.",
                exc,
                component="Reference list save",
                context={
                    "selected_list": selected,
                    "requested_list_name": list_name or current_name,
                    "submitted_value_count": len([line for line in values_text.splitlines() if line.strip()]),
                },
            )
    st.caption(
        "Predicates using in_ref/not_in_ref read these values at execution time. The bundled local_vendors list is seeded on first launch."
    )


def render_batch_administration(store: SnowflakeRulesStore) -> None:
    batches = store.list_batches(include_archived=True, limit=1000)
    if not batches:
        st.caption("No batches are stored.")
        return
    records = [
        {
            "Batch": clean_text(batch.get("name")),
            "Rows": int(batch.get("row_count") or 0),
            "Status": clean_text(batch.get("status")),
            "Archived": bool_value(batch.get("archived")),
            "Source": clean_text(batch.get("source_file_name")),
            "Created": timestamp_text(batch.get("created_at"))[:19].replace("T", " "),
            "Batch ID": clean_text(batch.get("id")),
        }
        for batch in batches
    ]
    dataframe(pd.DataFrame(records), height=min(600, 38 + len(records[:25]) * 35))
    selected_id = st.selectbox(
        "Administer batch",
        [clean_text(batch.get("id")) for batch in batches],
        format_func=lambda value: next(
            (
                f"{clean_text(batch.get('name'))} · {int(batch.get('row_count') or 0):,} rows · "
                f"{'Archived' if bool_value(batch.get('archived')) else clean_text(batch.get('status'))}"
                for batch in batches
                if clean_text(batch.get("id")) == value
            ),
            value,
        ),
        key="settings_batch_admin",
    )
    selected = next(batch for batch in batches if clean_text(batch.get("id")) == selected_id)
    controls = st.columns([1, 1, 2])
    archived = bool_value(selected.get("archived"))
    if controls[0].button("Restore batch" if archived else "Archive batch", key=f"archive_{selected_id}"):
        try:
            store.archive_batch(selected_id, archived=not archived)
            if not archived and st.session_state.get("selected_batch_id") == selected_id:
                st.session_state["selected_batch_id"] = ""
                st.session_state["_pending_batch_picker"] = ""
            set_flash(f"{'Restored' if archived else 'Archived'} {clean_text(selected.get('name'))}.")
            safe_rerun()
        except Exception as exc:
            render_actionable_exception(
                "Batch status could not be changed.",
                exc,
                component="Batch archive/restore",
                context={"batch_id": selected_id, "batch_name": clean_text(selected.get("name")), "requested_archived": not archived},
            )
    delete_confirmation = controls[1].checkbox("Confirm permanent delete", key=f"delete_batch_confirm_{selected_id}")
    if controls[2].button(
        "Permanently delete batch, rows, runs, and results",
        disabled=not delete_confirmation,
        key=f"delete_batch_{selected_id}",
    ):
        try:
            store.delete_batch(selected_id)
            if st.session_state.get("selected_batch_id") == selected_id:
                st.session_state["selected_batch_id"] = ""
                st.session_state["_pending_batch_picker"] = ""
            set_flash(f"Permanently deleted {clean_text(selected.get('name'))} and its execution records.", "warning")
            safe_rerun()
        except Exception as exc:
            render_actionable_exception(
                "Permanent batch deletion failed.",
                exc,
                component="Batch deletion",
                context={"batch_id": selected_id, "batch_name": clean_text(selected.get("name"))},
            )


def render_settings_page(store: SnowflakeRulesStore) -> None:
    render_page_header(
        "Settings & Administration",
        "Inspect the Snowflake context, verify the separately provisioned backend, restore bundled rules, manage reference values, and review audit events.",
        kicker="Operations Control",
    )
    system_tab, reference_tab, batch_tab, audit_tab = st.tabs(
        ["System health", "Reference lists", "Batch administration", "Audit log"]
    )
    with system_tab:
        try:
            health = store.health()
            context = health.get("context") or {}
            context_columns = st.columns(5)
            context_columns[0].metric("Database", clean_text(context.get("database_name")) or "—")
            context_columns[1].metric("Schema", clean_text(context.get("schema_name")) or "—")
            context_columns[2].metric("Warehouse", clean_text(context.get("warehouse_name")) or "—")
            context_columns[3].metric("Role", clean_text(context.get("role_name")) or "—")
            context_columns[4].metric("User", clean_text(context.get("user_name")) or "—")
            dataframe(pd.DataFrame(health_count_records(health)))
            with st.expander("Snowflake and application context"):
                st.json(health)
        except Exception as exc:
            render_actionable_exception(
                "Snowflake health inspection failed.",
                exc,
                component="Settings health inspection",
                context={"database": TARGET_DATABASE, "schema": TARGET_SCHEMA, "role": TARGET_ROLE},
            )

        st.markdown("#### Backend verification and catalog recovery")
        controls = st.columns([1, 1, 1, 2])
        if controls[0].button("Verify backend tables", key="settings_bootstrap"):
            try:
                store.verify_backend()
                inserted = store.seed_reference_lists()
                set_flash(f"Backend tables are accessible. Seeded {inserted:,} missing reference value(s).")
                safe_rerun()
            except Exception as exc:
                render_actionable_exception(
                    "Backend verification failed.",
                    exc,
                    component="Backend verification",
                    context={"tables": store.tables, "database": TARGET_DATABASE, "schema": TARGET_SCHEMA},
                )
        if controls[1].button("Seed missing bundled rules", key="settings_seed_missing"):
            try:
                report = store.seed_bundled_rules(force=False)
                set_flash(
                    f"Bundled catalog verified: {report.get('created', 0)} created, "
                    f"{report.get('unchanged', 0)} unchanged."
                )
                safe_rerun()
            except Exception as exc:
                render_actionable_exception(
                    "Bundled rule seeding failed.",
                    exc,
                    component="Bundled rule seed",
                    context={"force_restore": False, "target_rule_count": len(load_bundled_catalog())},
                )
        restore_confirm = controls[2].checkbox("Confirm full restore", key="settings_restore_confirm")
        if controls[3].button(
            "Restore all bundled DAF rules",
            disabled=not restore_confirm,
            key="settings_restore_all",
        ):
            try:
                report = store.seed_bundled_rules(force=True)
                set_flash(
                    f"Restored {report.get('updated', 0) + report.get('created', 0):,} bundled rule definitions."
                )
                safe_rerun()
            except Exception as exc:
                render_actionable_exception(
                    "Bundled catalog restore failed.",
                    exc,
                    component="Bundled catalog restore",
                    context={"force_restore": True, "target_rule_count": len(load_bundled_catalog())},
                )
        startup_report = st.session_state.get("_startup_seed_report")
        if isinstance(startup_report, Mapping):
            with st.expander("Current-session startup seed report"):
                st.json(startup_report)

        st.markdown("#### Application self-check")
        self_check = ensure_application_self_check()
        self_columns = st.columns([1, 1, 1, 2])
        self_columns[0].metric("Status", clean_text(self_check.get("status")).upper())
        self_columns[1].metric("Passed", int(self_check.get("tests_passed") or 0))
        self_columns[2].metric("Failed", int(self_check.get("tests_failed") or 0))
        if self_columns[3].button("Run self-check again", key="settings_run_self_check"):
            refreshed = ensure_application_self_check(force=True)
            set_flash(
                "Application self-check passed all internal contracts."
                if clean_text(refreshed.get("status")) == "passed"
                else "Application self-check found a failure. Open Settings → System health for details.",
                "success" if clean_text(refreshed.get("status")) == "passed" else "error",
            )
            safe_rerun()
        with st.expander("Self-check details", expanded=clean_text(self_check.get("status")) != "passed"):
            st.json(self_check)
        with st.expander("Exact deployed source identity", expanded=False):
            st.json({
                "app_version": APP_VERSION,
                "parser_version": WORKBOOK_PARSER_VERSION,
                "session_state_schema_version": SESSION_STATE_SCHEMA_VERSION,
                "deployment_sentinel": DEPLOYMENT_SENTINEL,
                "source": source_code_fingerprint(),
            })
        migration_report = st.session_state.get("_session_migration_report")
        if isinstance(migration_report, Mapping):
            with st.expander("Session State migration and recovery report", expanded=False):
                st.json(migration_report)

        st.markdown("#### Current-browser diagnostic history")
        render_diagnostic_log()

        st.markdown("#### Physical objects")
        dataframe(
            pd.DataFrame(
                [{"Purpose": key, "Table": name} for key, name in store.tables.items()]
            )
        )

    with reference_tab:
        render_reference_list_settings(store)
    with batch_tab:
        render_batch_administration(store)
    with audit_tab:
        events = store.list_audit(limit=1000)
        if not events:
            st.caption("No audit events are recorded.")
        else:
            controls = st.columns([1, 1, 2])
            entity_types = ["All", *sorted({clean_text(event.get("entity_type")) for event in events})]
            entity_type = controls[0].selectbox("Entity type", entity_types, key="settings_audit_type")
            actions = ["All", *sorted({clean_text(event.get("action")) for event in events})]
            action = controls[1].selectbox("Action", actions, key="settings_audit_action")
            query = controls[2].text_input("Find user, entity, or detail", key="settings_audit_search")
            filtered = []
            for event in events:
                if entity_type != "All" and clean_text(event.get("entity_type")) != entity_type:
                    continue
                if action != "All" and clean_text(event.get("action")) != action:
                    continue
                haystack = json_dumps(event).lower()
                if query and query.lower() not in haystack:
                    continue
                filtered.append(event)
            dataframe(pd.DataFrame(audit_table_records(filtered)), height=min(700, 38 + len(filtered[:50]) * 35))
            if filtered:
                event_index = st.selectbox(
                    "Inspect audit payload",
                    range(len(filtered)),
                    format_func=lambda index: (
                        f"{timestamp_text(filtered[index].get('created_at'))[:19].replace('T', ' ')} · "
                        f"{clean_text(filtered[index].get('action')).replace('_', ' ').title()} · "
                        f"{clean_text(filtered[index].get('user_name'))}"
                    ),
                    key="settings_audit_event",
                )
                st.json(filtered[event_index])


# -----------------------------------------------------------------------------
# Application entry point
# -----------------------------------------------------------------------------


def render_live_build_proof() -> None:
    """Render an unmistakable identity before Snowflake initialization."""
    require_streamlit()
    identity = source_code_fingerprint()
    st.sidebar.markdown(f"### {APP_TITLE}")
    st.sidebar.success(LIVE_BUILD_BADGE)
    st.sidebar.caption(APP_VERSION)
    st.sidebar.code(DEPLOYMENT_SENTINEL, language="text")
    st.sidebar.caption(
        f"Parser {WORKBOOK_PARSER_VERSION} · State v{SESSION_STATE_SCHEMA_VERSION} · "
        f"Source {clean_text(identity.get('sha256_short')) or 'unavailable'} · Runtime DDL OFF"
    )


def render_sidebar(store: SnowflakeRulesStore) -> tuple[str, list[dict[str, Any]], dict[str, Any] | None]:
    page = st.sidebar.radio("Workspace", PAGE_NAMES, key="_page_navigation")
    st.sidebar.divider()
    batches, selected_batch = choose_batch_sidebar(store)
    if selected_batch:
        st.sidebar.caption(
            f"{clean_text(selected_batch.get('source_file_name'))} · "
            f"{timestamp_text(selected_batch.get('created_at'))[:10]}"
        )
    try:
        context = store.context()
        st.sidebar.divider()
        st.sidebar.caption(
            " · ".join(
                value
                for value in (
                    clean_text(context.get("database_name")),
                    clean_text(context.get("schema_name")),
                    clean_text(context.get("role_name")),
                )
                if value
            )
        )
    except Exception as exc:
        st.sidebar.warning(f"Snowflake context unavailable: {type(exc).__name__}")
    return page, batches, selected_batch


def main() -> None:
    require_streamlit()
    st.set_page_config(page_title=APP_TITLE, page_icon="⚙️", layout="wide", initial_sidebar_state="expanded")
    app_styles()
    render_live_build_proof()
    migrate_session_state()
    self_check = ensure_application_self_check()
    if clean_text(self_check.get("status")) == "failed":
        classification = self_check.get("classification") if isinstance(self_check.get("classification"), Mapping) else {}
        st.error("The application self-check detected an internal contract failure before startup.")
        st.markdown(f"**Likely root cause:** {clean_text(classification.get('summary'))}")
        st.info(f"**Corrective action:** {clean_text(classification.get('recommended_action'))}")
        st.caption(
            f"Diagnostic ID: {clean_text(self_check.get('diagnostic_id'))} · Build: {APP_VERSION} · "
            f"Source: {clean_text(source_code_fingerprint().get('sha256_short'))}"
        )
        with st.expander("Self-check technical diagnostic", expanded=True):
            st.json(self_check)
            st.download_button(
                "Download self-check diagnostic JSON",
                data=json_dumps(self_check, pretty=True),
                file_name=f"rules_engine_{clean_text(self_check.get('diagnostic_id'))}.json",
                mime="application/json",
                key="download_startup_self_check_failure",
            )
        st.stop()
        return
    try:
        store = initialize_store()
    except Exception as exc:
        render_actionable_exception(
            "The Rules Engine could not initialize its Snowflake session or backend tables.",
            exc,
            component="Application initialization",
            context={
                "target_role": TARGET_ROLE,
                "target_warehouse": TARGET_WAREHOUSE,
                "target_database": TARGET_DATABASE,
                "target_schema": TARGET_SCHEMA,
                "table_prefix": TABLE_PREFIX,
                "runtime_table_ddl": False,
            },
        )
        st.stop()
        return
    render_flash()
    page, batches, selected_batch = render_sidebar(store)
    try:
        if page == "Overview":
            render_overview_page(store, batches, selected_batch)
        elif page == "Process Workbook":
            render_process_workbook_page(store)
        elif page == "Execution":
            render_execution_page(store, selected_batch)
        elif page == "Analyst Workbench":
            render_workbench_page(store, selected_batch)
        elif page == "Reports":
            render_reports_page(store, selected_batch)
        elif page == "Rules Catalog":
            render_rules_catalog_page(store)
        elif page == "Simulator":
            render_simulator_page(store)
        elif page == "Settings":
            render_settings_page(store)
        else:
            st.error(f"Unknown page: {page}")
    except Exception as exc:
        render_actionable_exception(
            f"{page} encountered an unexpected error.",
            exc,
            component=f"Page: {page}",
            context={
                "page": page,
                "selected_batch_id": clean_text(selected_batch.get("id")) if isinstance(selected_batch, Mapping) else "",
                "selected_batch_name": clean_text(selected_batch.get("name")) if isinstance(selected_batch, Mapping) else "",
            },
        )


if __name__ == "__main__":
    main()
