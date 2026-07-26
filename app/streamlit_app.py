from __future__ import annotations

"""
ONE ENGINE — Snowflake-native compliance rules platform
========================================================

"""

import base64
import csv
import gzip
import hashlib
import io
import json
import math
import os
import platform
import re
import sys
import traceback
import uuid
import zipfile
from collections import Counter, defaultdict, deque
from contextlib import contextmanager
from difflib import SequenceMatcher
from importlib import metadata as importlib_metadata
from time import perf_counter
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Iterable, Iterator, Mapping, MutableMapping, Sequence
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
APP_VERSION = "2026.07.26-one-engine-literal-distillery-v6"
SESSION_STATE_SCHEMA_VERSION = 7
WORKBOOK_PARSER_VERSION = "2026.07.24-v7-uncached"
MAX_DIAGNOSTIC_EVENTS = 50
DEPLOYMENT_SENTINEL = (
    "ONE_ENGINE_FOODBUY_DESIGN_SYSTEM_LITERAL_FILTER_DISTILLERY_20260726"
)
LIVE_BUILD_BADGE = "ONE ENGINE · SNOWFLAKE · LIVE"
FOODBUY_DESIGN_SYSTEM_REFERENCE = (
    "https://69925e4ee40e16a198c7c5cf-xdindjzhxi.chromatic.com/"
)
TARGET_ROLE = "FOODBUY_AXIOM_COMPLIANCE_PROD"
TARGET_WAREHOUSE = "COMPLIANCE_PROD_WH"
TARGET_DATABASE = "FOODBUY_MASALA_PROD"
TARGET_SCHEMA = "COMPLIANCE_LAB"
LIVE_PRODUCT_REQUEST_VIEW = "V_OE_PRODUCTREQUESTS"
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
    "catalog_versions": "CATALOG_VERSIONS",
    "catalog_rules": "CATALOG_VERSION_RULES",
    "distillery_gaps": "DISTILLERY_GAPS",
    "outcome_aliases": "OUTCOME_ALIASES",
}

HEADER_ALIASES = {
    "case #": "Case#",
    "case": "Case#",
    "case#": "Case#",
    "case number": "Case#",
    "request type": "Type",
    "created date": "Date Created",
    "subcategory": "Sub Category",
    "sub category": "Sub Category",
    "buy smart action": "Buysmart Action",
    "buysmartaction": "Buysmart Action",
    "buysmart action": "Buysmart Action",
    "if in-stock action": "If In Stock: Action",
    "if in stock action": "If In Stock: Action",
    "if in stock: action": "If In Stock: Action",
    "one time or permanent": "One-Time or Permanent",
    "conversion va pct": "Conversion VA%",
    "conversion va percent": "Conversion VA%",
    "audit action": "Audit Action",
    "auditaction": "Audit Action",
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
    "Audit Action",
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
    "date_created": "Date created",
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
    "date_before": "is before",
    "date_on_or_before": "is on or before",
    "date_after": "is after",
    "date_on_or_after": "is on or after",
}
SUPPORTED_OPERATORS = set(OPERATOR_LABELS)
NO_VALUE_OPERATORS = {"blank", "not_blank", "is_true", "is_false"}
NUMERIC_OPERATORS = {"gt", "ge", "lt", "le"}
DATE_OPERATORS = {
    "date_before",
    "date_on_or_before",
    "date_after",
    "date_on_or_after",
}
LIST_OPERATORS = {"in", "not_in"}

ACTION_LABELS = {
    "set_action": "Set ACTION",
    "set_action_by_duration": "Set ACTION by duration",
    "set_if_stock": "Set If In Stock",
    "set_audit_action": "Set Audit Action",
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
    "set_audit_action",
    "set_buysmart",
    "set_review",
    "append_validation",
    "add_note",
    "exclude",
]

ACTION_OPTIONS = ["OK", "1X", "Use Right", "Find Alt First", "Cannot Add", "Invalid Information", "Review"]
IF_STOCK_OPTIONS = ["OK", "Review"]
AUDIT_ACTION_OPTIONS = [
    "DAOG",
    "SRF",
    "REPLACE",
    "KEEP AND DAOG",
    "KEEP AND SRF",
    "KEEP AND REPLACE",
]
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
    "Audit Action",
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
    "Final Audit Action",
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
        "upstreamAuditAction": clean_text(source.get("Audit Action")),
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
    upstream_audit_action = clean_text(fields["upstreamAuditAction"])
    request_type = clean_text(fields["requestType"])
    case_number = clean_text(fields["caseNumber"])
    row: dict[str, Any] = {
        "id": new_id(),
        "batch_id": batch_id,
        "source_row_number": int(source_row_number),
        "workflow_request_key": f"{case_number or 'row'}-{source_row_number}",
        "ruleset_id": "product_request",
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
        "upstream_audit_action": upstream_audit_action,
        "action": upstream_action,
        "if_in_stock_action": upstream_if_stock,
        "audit_action": upstream_audit_action,
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
    source["Audit Action"] = row.get("upstream_audit_action", "")
    source["Buysmart Action"] = row.get("buysmart_action", "")
    normalized = create_normalized_row(source)
    normalized["derived"]["current_action_key"] = normalize_key(row.get("action"))
    normalized["derived"]["current_buysmart_key"] = normalize_key(row.get("buysmart_action"))
    row["normalized_row"] = normalized
    if not clean_text(row.get("queue_bucket")):
        row["queue_bucket"] = queue_bucket_for_type(row.get("request_type"))
    return row


def _distillery_field(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", clean_text(value).lower()).strip("_")


def _distillery_stable_value(value: Any) -> str:
    if not isinstance(value, bool) and isinstance(value, (int, float, Decimal)):
        number = float(value)
        if not math.isnan(number):
            return f"n:{number:.12g}"
    return f"s:{normalize_key(value)}"


def _distillery_evidence_hash(raw_row: Mapping[str, Any]) -> str:
    canonical: dict[str, Any] = {}
    for key, value in raw_row.items():
        field = _distillery_field(key)
        if not field:
            continue
        if field not in canonical or not clean_text(canonical[field]):
            canonical[field] = value
    payload = [
        (key, _distillery_stable_value(value))
        for key, value in sorted(canonical.items())
    ]
    return hashlib.sha256(
        json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _distillery_number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float, Decimal)):
        number = float(value)
        return None if math.isnan(number) else number
    text = clean_text(value).replace(",", "").strip()
    if not text or text.lower() == "blank":
        return None
    is_percent = text.endswith("%")
    if is_percent:
        text = text[:-1].strip()
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    number = float(match.group(0))
    return number / 100.0 if is_percent else number


def context_for_row(row: Mapping[str, Any]) -> dict[str, Any]:
    normalized = row.get("normalized_row") or {}
    raw_row = row.get("raw_row") or {}
    context: dict[str, Any] = {}
    for key, value in raw_row.items():
        field = _distillery_field(key)
        if field and (field not in context or not clean_text(context[field])):
            context[field] = value
    context.update(deepcopy(normalized.get("fields") or {}))
    context.update(deepcopy(normalized.get("derived") or {}))
    context["input_action"] = raw_row.get("ACTION", row.get("upstream_action"))
    context["input_if_in_stock_action"] = raw_row.get(
        "If In Stock: Action",
        row.get("upstream_if_in_stock_action"),
    )
    context["input_audit_action"] = raw_row.get(
        "Audit Action",
        row.get("upstream_audit_action"),
    )
    context["input_buysmart_action"] = raw_row.get(
        "Buysmart Action",
        row.get("buysmart_action"),
    )
    context["conversion_va_num"] = _distillery_number(
        context.get("conversion_va")
    )
    context["__ruleset_id"] = clean_text(
        row.get("ruleset_id") or "product_request"
    )
    context["current_action_key"] = normalize_key(row.get("action"))
    context["current_buysmart_key"] = normalize_key(row.get("buysmart_action"))
    context["action"] = clean_text(row.get("action"))
    context["audit_action"] = clean_text(row.get("audit_action"))
    context["buysmartAction"] = clean_text(row.get("buysmart_action"))
    return context


def _number_for_compare(value: Any) -> float:
    parsed = parse_number(value)
    return parsed if parsed is not None else 0.0


def _date_for_compare(value: Any) -> datetime | None:
    normalized = normalize_date(value)
    if not normalized:
        return None
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


_IN_LIST_CACHE: dict[int, tuple[Any, frozenset[str]]] = {}


def _in_list(left: Any, right: Any) -> bool:
    value = normalize_key(left)
    if isinstance(right, list):
        options = right
        cache_key = id(right)
    else:
        options = [item.strip() for item in clean_text(right).split(",") if item.strip()]
        cache_key = 0
    cached = _IN_LIST_CACHE.get(cache_key)
    normalized_options = cached[1] if cached and cached[0] is right else None
    if normalized_options is None:
        normalized_options = frozenset(normalize_key(item) for item in options)
        if cache_key:
            _IN_LIST_CACHE[cache_key] = (right, normalized_options)
    return value in normalized_options


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
    if op in DATE_OPERATORS:
        left_date = _date_for_compare(left)
        right_date = _date_for_compare(right)
        if left_date is None or right_date is None:
            return False
        if op == "date_before":
            return left_date < right_date
        if op == "date_on_or_before":
            return left_date <= right_date
        if op == "date_after":
            return left_date > right_date
        return left_date >= right_date
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
        elif action_type == "set_audit_action":
            row["audit_action"] = clean_text(node.get("value"))
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

    runtime_context = context_for_row(row)
    for variant in ordered:
        predicate = variant.get("predicate_json")
        if not isinstance(predicate, Mapping) or not evaluate_predicate(
            predicate,
            runtime_context,
            reference_lists,
        ):
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
        runtime_context = context_for_row(row)

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
            clean_text(row.get("audit_action")),
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
    snapshot = {
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
    snapshot["sha256"] = hashlib.sha256(
        json.dumps(
            {
                "rules": [_plain_data(rule) for rule in rules],
                "executionOrder": snapshot["executionOrder"],
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=json_default,
        ).encode("utf-8")
    ).hexdigest()
    return snapshot


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
    if operator in DATE_OPERATORS:
        parsed = normalize_date(value)
        if _date_for_compare(parsed) is None:
            raise ValueError("Date filter operators require a valid date.")
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
        elif action_type in {
            "set_action",
            "set_if_stock",
            "set_audit_action",
            "set_buysmart",
            "append_validation",
            "add_note",
        }:
            permits_blank = action_type in {
                "set_action",
                "set_if_stock",
                "set_audit_action",
            }
            if not value and not (permits_blank and "value" in action_row):
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
        elif action_type == "set_audit_action":
            parts.append(f"Set Audit Action: {value}")
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
            "set_action",
            "set_if_stock",
            "set_audit_action",
            "set_buysmart",
            "append_validation",
            "add_note",
            "preserve_action_set_if_stock",
        } and not clean_text(node.get("value")) and not (
            action_type in {"set_action", "set_if_stock", "set_audit_action"}
            and "value" in node
        ):
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


def rule_workflow_id(rule: Mapping[str, Any]) -> str:
    source = rule.get("source") if isinstance(rule.get("source"), Mapping) else {}
    return (
        clean_text(rule.get("ruleset_id"))
        or clean_text(source.get("ruleset_id"))
        or "product_request"
    )


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


# -----------------------------------------------------------------------------
# Rules Distillery — single-file BEFORE/AFTER rule induction
# -----------------------------------------------------------------------------


DISTILLERY_VERSION = "2026.07.26-literal-filter-v2"
DISTILLERY_SUPPORTED_EXTENSIONS = {
    "csv",
    "tsv",
    "txt",
    "xlsx",
    "xlsm",
    "json",
    "jsonl",
    "ndjson",
    "parquet",
    "feather",
}
DISTILLERY_STOP_WORDS = {
    "and",
    "are",
    "for",
    "from",
    "into",
    "not",
    "the",
    "this",
    "with",
    "without",
    "item",
    "product",
    "case",
    "each",
    "pack",
    "count",
}

PRODUCT_REQUEST_DISTILLERY_PROFILE: dict[str, Any] = {
    "profile_id": "product_request",
    "version": "2.0.0",
    "description": "Product Request PRF/SORF/SRF daily decision sources",
    "output_fields": [
        {
            "source": "ACTION",
            "target": "action",
            "action_type": "set_action",
            "normalizer": "action",
        },
        {
            "source": "If In Stock: Action",
            "target": "if_in_stock_action",
            "action_type": "set_if_stock",
            "normalizer": "action",
        },
        {
            "source": "Audit Action",
            "target": "audit_action",
            "action_type": "set_audit_action",
            "normalizer": "audit",
        },
    ],
    "column_aliases": {
        "Case": "Case#",
        "Case #": "Case#",
        "Subcategory": "Sub Category",
        "Buy Smart Action": "Buysmart Action",
        "If In-Stock Action": "If In Stock: Action",
        "If In Stock Action": "If In Stock: Action",
        "AuditAction": "Audit Action",
    },
    "matching": {
        "identity_groups": [
            ["Case#"],
            ["Unit Number", "DIN", "MIN", "Vendor"],
            ["Unit Number", "DIN", "Vendor", "Description"],
            ["Business", "Type", "DIN", "MIN", "Description"],
        ],
        "ignored_fields": ["Buysmart Action"],
        "volatile_fields": [
            "Request Assignee",
            "Case Owner",
            "Status",
            "Last Modified",
        ],
        "similarity_fields": [
            "Business",
            "Type",
            "Case#",
            "Sector",
            "Division",
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
            "Meets Criteria",
            "In CAT",
            "On MOG",
            "Pantry",
            "K12 APL",
            "Compass APL",
            "Conversion DIN",
            "Conversion VA%",
        ],
        "minimum_similarity": 0.72,
        "ambiguity_margin": 0.05,
    },
    "induction": {
        "feature_fields": [
            "business",
            "type",
            "sector",
            "division",
            "vendor",
            "manufacturer",
            "brand",
            "description",
            "parent_category",
            "sub_category",
            "usage",
            "one_time_or_permanent",
            "reason_for_request",
            "dpl",
            "meets_criteria",
            "in_cat",
            "on_mog",
            "pantry",
            "k12_apl",
            "compass_apl",
            "conversion_din",
            "conversion_manufacturer",
            "conversion_brand",
            "conversion_item_description",
            "conversion_va",
            "supply_chain_description",
            "pack",
            "parent",
            "dst",
            "input_action",
            "input_if_in_stock_action",
            "input_audit_action",
            "input_buysmart_action",
            "business_key",
            "request_type_key",
            "usage_num",
            "meets_criteria_num",
            "conversion_va_num",
            "is_one_time",
            "is_permanent",
            "is_in_catalog",
            "is_in_cat_y",
            "is_temp_available",
            "is_pantry",
            "is_k12_apl",
            "is_core_apl",
            "is_s1",
            "is_foh",
            "is_diverse",
            "has_conversion",
            "is_levy",
            "is_schools",
        ],
        "numeric_fields": [
            "usage_num",
            "meets_criteria_num",
            "conversion_va_num",
        ],
        "date_fields": [
            "date_created",
        ],
        "token_fields": [
            "vendor",
            "manufacturer",
            "brand",
            "description",
            "parent_category",
            "sub_category",
            "reason_for_request",
            "on_mog",
            "compass_apl",
            "conversion_manufacturer",
            "conversion_brand",
            "conversion_item_description",
            "audit_action",
            "supply_chain_description",
            "input_action",
            "input_if_in_stock_action",
        ],
        "minimum_leaf_size": 1,
        "maximum_depth": 16,
        "minimum_gain": 1e-9,
        "maximum_category_splits": 16,
        "maximum_numeric_splits": 16,
        "maximum_token_splits": 24,
        "literal_maximum_category_splits": 128,
        "literal_maximum_token_splits": 64,
        "maximum_in_list_size": 64,
        "minimum_negated_category_support": 3,
        "minimum_token_support": 5,
        "minimum_general_support": 3,
        "minimum_auto_support_dates": 2,
        "maximum_filter_depth": 4,
        "maximum_greedy_filter_depth": 8,
        "filter_beam_width": 32,
        "maximum_atoms_per_outcome": 32,
        "maximum_filters_per_outcome": 16,
        "governed_fields": [
            "business",
            "type",
            "sector",
            "division",
            "vendor",
            "manufacturer",
            "brand",
            "description",
            "parent_category",
            "sub_category",
            "usage",
            "one_time_or_permanent",
            "reason_for_request",
            "dpl",
            "meets_criteria",
            "in_cat",
            "on_mog",
            "pantry",
            "k12_apl",
            "compass_apl",
            "conversion_din",
            "conversion_manufacturer",
            "conversion_brand",
            "conversion_item_description",
            "conversion_va",
            "supply_chain_description",
            "pack",
            "parent",
            "dst",
            "input_action",
            "input_if_in_stock_action",
            "input_audit_action",
            "date_created",
            "business_key",
            "request_type_key",
            "usage_num",
            "meets_criteria_num",
            "conversion_va_num",
            "is_one_time",
            "is_permanent",
            "is_in_catalog",
            "is_in_cat_y",
            "is_temp_available",
            "is_pantry",
            "is_k12_apl",
            "is_core_apl",
            "is_s1",
            "is_foh",
            "is_diverse",
            "has_conversion",
            "is_levy",
            "is_schools",
        ],
        "prohibited_predicate_fields": [
            "__evidence_hash",
            "case",
            "case_number",
            "case_",
            "pair_id",
            "workflow_request_key",
        ],
    },
    "feature_projector": "product_request",
}

DISTILLERY_PROFILES: dict[str, dict[str, Any]] = {
    "product_request": PRODUCT_REQUEST_DISTILLERY_PROFILE,
}


@dataclass(frozen=True)
class DistilleryAtom:
    field: str
    operator: str
    value: Any = None


@dataclass(frozen=True)
class DistilleryPair:
    pair_id: str
    source_group: str
    before_index: int
    after_index: int
    before: Mapping[str, Any]
    after: Mapping[str, Any]
    outputs: Mapping[str, Any]
    method: str
    score: float
    ambiguous: bool = False
    changed_input_fields: tuple[str, ...] = ()


@dataclass(frozen=True)
class DistilleryProjected:
    pair: DistilleryPair
    features: Mapping[str, Any]
    label: tuple[tuple[str, Any], ...]


@dataclass(frozen=True)
class DistilleryRule:
    rule_id: str
    priority: int
    predicates: tuple[DistilleryAtom, ...]
    outputs: Mapping[str, Any]
    support: int
    confidence: float
    source_groups: tuple[str, ...]
    kind: str
    evidence_ids: tuple[str, ...]
    validation_accuracy: float = 0.0


@dataclass(frozen=True)
class DistillerySplit:
    atom: DistilleryAtom
    gain: float
    true_indices: tuple[int, ...]
    false_indices: tuple[int, ...]


@dataclass(frozen=True)
class DistilleryLeaf:
    path: tuple[DistilleryAtom, ...]
    indices: tuple[int, ...]
    predicted: tuple[tuple[str, Any], ...]
    confidence: float


def distillery_profile(profile_id: str) -> dict[str, Any]:
    profile = DISTILLERY_PROFILES.get(clean_text(profile_id))
    if profile is None:
        raise ValueError(f"Unknown Distillery profile: {profile_id}")
    return deepcopy(profile)


def distillery_field(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", clean_text(value).lower()).strip("_")


def distillery_number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float, Decimal)):
        number = float(value)
        return None if math.isnan(number) else number
    text = clean_text(value).replace(",", "").strip()
    if not text or text.lower() == "blank":
        return None
    is_percent = text.endswith("%")
    if is_percent:
        text = text[:-1].strip()
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    number = float(match.group(0))
    return number / 100.0 if is_percent else number


def distillery_stable_value(value: Any) -> str:
    if (
        not isinstance(value, bool)
        and isinstance(value, (int, float, Decimal))
        and (number := distillery_number(value)) is not None
    ):
        return f"n:{number:.12g}"
    return f"s:{normalize_key(value)}"


def distillery_action(value: Any) -> str:
    text = clean_text(value)
    aliases = {
        "1 X": "1X",
        "1X": "1X",
        "APPROVED 1 X": "Approved - 1X",
        "APPROVED 1X": "Approved - 1X",
        "FIND ALT 1ST": "Find Alt First",
        "FIND ALT FIRST": "Find Alt First",
        "OK": "OK",
        "BLANK": "",
    }
    return aliases.get(normalize_key(text), text)


def distillery_outcome_value(
    target: str,
    value: Any,
    aliases: Mapping[str, Mapping[str, Any]] | None = None,
) -> str:
    """Canonicalize one literal AFTER outcome without fuzzy guessing."""
    raw = clean_text(value)
    key = normalize_key(raw)
    field_aliases = (aliases or {}).get(clean_text(target)) or {}
    explicit = field_aliases.get(key)
    if explicit is None and raw in field_aliases:
        explicit = field_aliases.get(raw)
    if explicit is not None:
        return clean_text(explicit)
    if clean_text(target) in {"action", "if_in_stock_action"}:
        return distillery_action(raw)
    if clean_text(target) == "audit_action":
        known = {
            "DAOG": "DAOG",
            "SRF": "SRF",
            "REPLACE": "REPLACE",
            "KEEP AND DAOG": "KEEP AND DAOG",
            "KEEP AND SRF": "KEEP AND SRF",
            "KEEP AND REPLACE": "KEEP AND REPLACE",
        }
        return known.get(key, raw)
    return raw


def distillery_outcome_alias_registry(
    after_documents: Sequence[Mapping[str, Any]],
    profile: Mapping[str, Any],
    aliases: Mapping[str, Mapping[str, Any]] | None = None,
    approved_keys: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Inventory raw outcomes, safe aliases, and review-only fuzzy suggestions."""
    approved = {clean_text(value) for value in (approved_keys or [])}
    approve_all = "*" in approved
    entries: list[dict[str, Any]] = []
    suggestions: list[dict[str, Any]] = []
    by_target: dict[str, Counter[str]] = defaultdict(Counter)
    raw_display: dict[tuple[str, str], str] = {}
    contracts = profile.get("output_fields") or []
    for document in after_documents:
        for row in document.get("rows") or []:
            for contract in contracts:
                source = clean_text(contract.get("source"))
                target = clean_text(contract.get("target"))
                raw = clean_text(row.get(source))
                raw_key = normalize_key(raw)
                by_target[target][raw_key] += 1
                raw_display.setdefault((target, raw_key), raw)
    for target, counts in sorted(by_target.items()):
        for raw_key, count in counts.most_common():
            raw = raw_display[(target, raw_key)]
            canonical = distillery_outcome_value(target, raw, aliases)
            entry_key = f"{target}|{raw_key}"
            entries.append(
                {
                    "field_name": target,
                    "raw_value": raw,
                    "raw_key": raw_key,
                    "canonical_value": canonical,
                    "row_count": int(count),
                    "status": (
                        "approved"
                        if entry_key in approved
                        or (aliases or {}).get(target, {}).get(raw_key) is not None
                        else "automatic"
                    ),
                }
            )
        non_blank = [key for key in counts if key]
        for left_index, left in enumerate(non_blank):
            for right in non_blank[left_index + 1 :]:
                if left == right:
                    continue
                stored = (aliases or {}).get(target, {})
                if left in stored and right in stored:
                    # Both raw values were explicitly reviewed. They may
                    # intentionally stay distinct or map to one canonical
                    # value; either choice is persisted in Snowflake.
                    continue
                similarity = SequenceMatcher(None, left, right).ratio()
                if similarity < 0.9:
                    continue
                review_key = f"{target}|{left}|{right}"
                if approve_all or review_key in approved:
                    continue
                suggestions.append(
                    {
                        "review_key": review_key,
                        "field_name": target,
                        "left_value": raw_display[(target, left)],
                        "right_value": raw_display[(target, right)],
                        "similarity": round(similarity, 4),
                        "status": "review",
                    }
                )
    return {
        "entries": entries,
        "suggestions": suggestions,
        "review_required": len(suggestions),
        "raw_value_count": len(entries),
    }


def distillery_evidence_hash(row: Mapping[str, Any]) -> str:
    canonical: dict[str, Any] = {}
    for key, value in row.items():
        field = distillery_field(key)
        if field and (field not in canonical or not clean_text(canonical[field])):
            canonical[field] = value
    payload = [
        (key, distillery_stable_value(value))
        for key, value in sorted(canonical.items())
    ]
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()


def distillery_canonicalize_row(
    row: Mapping[str, Any],
    profile: Mapping[str, Any],
) -> dict[str, Any]:
    aliases = {
        clean_text(key).lower(): clean_text(value)
        for key, value in (profile.get("column_aliases") or {}).items()
    }
    output: dict[str, Any] = {}
    for raw_key, value in row.items():
        key = clean_text(raw_key)
        canonical = aliases.get(key.lower(), key)
        if canonical and (
            canonical not in output or not clean_text(output[canonical])
        ):
            output[canonical] = value
    return output


def distillery_records_from_bytes(
    name: str,
    data: bytes,
) -> tuple[Mapping[str, Any], ...]:
    extension = name.rsplit(".", 1)[-1].lower() if "." in name else ""
    if extension in {"csv", "tsv", "txt", "xlsx", "xlsm"}:
        parsed = parse_source_workbook(name, data)
        return tuple(dict(row) for row in parsed.rows)
    if extension in {"json", "jsonl", "ndjson"}:
        text = data.decode("utf-8-sig")
        if extension in {"jsonl", "ndjson"}:
            values = [
                json.loads(line) for line in text.splitlines() if line.strip()
            ]
        else:
            payload = json.loads(text)
            if isinstance(payload, list):
                values = payload
            elif isinstance(payload, Mapping):
                candidate = (
                    payload.get("rows")
                    or payload.get("records")
                    or payload.get("data")
                )
                values = (
                    candidate if isinstance(candidate, list) else [payload]
                )
            else:
                raise ValueError(f"JSON source {name!r} must contain records.")
        return tuple(dict(item) for item in values if isinstance(item, Mapping))
    if extension in {"parquet", "feather"}:
        if pd is None:
            raise RuntimeError("Pandas is required for columnar sources.")
        buffer = io.BytesIO(data)
        frame = (
            pd.read_parquet(buffer)
            if extension == "parquet"
            else pd.read_feather(buffer)
        )
        frame = frame.where(frame.notna(), None)
        return tuple(frame.to_dict(orient="records"))
    raise ValueError(f"No Distillery adapter is registered for {name!r}.")


def distillery_source_group(name: str) -> str:
    base = clean_text(name).replace("\\", "/").rsplit("/", 1)[-1]
    return base.rsplit(".", 1)[0] if "." in base else base


def distillery_documents_from_upload(
    file_name: str,
    data: bytes,
    profile: Mapping[str, Any],
) -> tuple[dict[str, Any], ...]:
    members: list[tuple[str, bytes]] = []
    extension = file_name.replace("\\", "/").rsplit("/", 1)[-1]
    extension = extension.rsplit(".", 1)[-1].lower() if "." in extension else ""
    if extension == "zip":
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            for member in sorted(archive.namelist()):
                if member.endswith("/"):
                    continue
                extension = (
                    member.rsplit(".", 1)[-1].lower() if "." in member else ""
                )
                if extension in DISTILLERY_SUPPORTED_EXTENSIONS:
                    members.append((member, archive.read(member)))
    else:
        members.append((file_name, data))
    documents: list[dict[str, Any]] = []
    groups: set[str] = set()
    for member, member_data in members:
        extension = member.rsplit(".", 1)[-1].lower()
        base_name = member.replace("\\", "/").rsplit("/", 1)[-1]
        group = distillery_source_group(base_name)
        if group in groups:
            raise ValueError(
                f"Duplicate source group {group!r} in {file_name!r}."
            )
        groups.add(group)
        records = distillery_records_from_bytes(base_name, member_data)
        rows = tuple(
            distillery_canonicalize_row(row, profile) for row in records
        )
        documents.append(
            {
                "name": base_name,
                "source_type": extension,
                "source_group": group,
                "rows": rows,
                "row_count": len(rows),
            }
        )
    if not documents:
        raise ValueError(
            f"{file_name!r} contains no supported Distillery source files."
        )
    return tuple(documents)


def distillery_non_output_fields(
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    profile: Mapping[str, Any],
) -> tuple[str, ...]:
    output_fields = {
        clean_text(item.get("source"))
        for item in profile.get("output_fields") or []
    }
    matching = profile.get("matching") or {}
    excluded = (
        output_fields
        | set(matching.get("ignored_fields") or [])
        | set(matching.get("volatile_fields") or [])
    )
    return tuple(sorted((set(before) | set(after)) - excluded))


def distillery_fingerprint(
    row: Mapping[str, Any],
    fields: Iterable[str],
) -> str:
    payload = [
        (field, distillery_stable_value(row.get(field)))
        for field in sorted(fields)
    ]
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def distillery_identity_key(
    row: Mapping[str, Any],
    fields: Sequence[str],
) -> tuple[str, ...] | None:
    values = tuple(normalize_key(row.get(field)) for field in fields)
    return values if values and all(values) else None


def distillery_similarity(
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    fields: Sequence[str],
) -> float:
    comparable = 0
    matched = 0.0
    for field in fields:
        left = normalize_key(before.get(field))
        right = normalize_key(after.get(field))
        if not left and not right:
            continue
        comparable += 1
        if left == right:
            matched += 1.0
        elif left and right and (left in right or right in left):
            matched += 0.6
    return matched / comparable if comparable else 0.0


def distillery_outputs(
    after: Mapping[str, Any],
    profile: Mapping[str, Any],
    aliases: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for contract in profile.get("output_fields") or []:
        value = after.get(contract.get("source"))
        target = clean_text(contract.get("target"))
        output[target] = (
            distillery_outcome_value(target, value, aliases)
            if clean_text(contract.get("normalizer")) in {"action", "audit"}
            else clean_text(value)
        )
    return output


def distillery_make_pair(
    before_document: Mapping[str, Any],
    after_document: Mapping[str, Any],
    before_index: int,
    after_index: int,
    profile: Mapping[str, Any],
    method: str,
    score: float,
    ambiguous: bool = False,
    outcome_aliases: Mapping[str, Mapping[str, Any]] | None = None,
) -> DistilleryPair:
    before = before_document["rows"][before_index]
    after = after_document["rows"][after_index]
    fields = distillery_non_output_fields(before, after, profile)
    changed = tuple(
        field
        for field in fields
        if distillery_stable_value(before.get(field))
        != distillery_stable_value(after.get(field))
    )
    pair_payload = json.dumps(
        dict(before),
        default=str,
        sort_keys=True,
        ensure_ascii=False,
    )
    pair_id = hashlib.sha256(
        (
            f"{before_document['source_group']}|{before_index}|"
            f"{after_index}|{pair_payload}"
        ).encode("utf-8")
    ).hexdigest()[:24]
    return DistilleryPair(
        pair_id=pair_id,
        source_group=clean_text(before_document["source_group"]),
        before_index=before_index,
        after_index=after_index,
        before=before,
        after=after,
        outputs=distillery_outputs(after, profile, outcome_aliases),
        method=method,
        score=score,
        ambiguous=ambiguous,
        changed_input_fields=changed,
    )


def distillery_match_document_pair(
    before_document: Mapping[str, Any],
    after_document: Mapping[str, Any],
    profile: Mapping[str, Any],
    outcome_aliases: Mapping[str, Mapping[str, Any]] | None = None,
) -> tuple[list[DistilleryPair], list[dict[str, Any]]]:
    before_rows = before_document["rows"]
    after_rows = after_document["rows"]
    unmatched_before = set(range(len(before_rows)))
    unmatched_after = set(range(len(after_rows)))
    pairs: list[DistilleryPair] = []
    all_fields = sorted(
        set().union(*(row.keys() for row in before_rows))
        | set().union(*(row.keys() for row in after_rows))
    )
    output_fields = {
        clean_text(item.get("source"))
        for item in profile.get("output_fields") or []
    }
    matching = profile.get("matching") or {}
    all_fields = [
        field
        for field in all_fields
        if field
        not in (
            output_fields
            | set(matching.get("ignored_fields") or [])
            | set(matching.get("volatile_fields") or [])
        )
    ]
    before_fingerprints: dict[str, deque[int]] = defaultdict(deque)
    after_fingerprints: dict[str, deque[int]] = defaultdict(deque)
    for index in unmatched_before:
        before_fingerprints[
            distillery_fingerprint(before_rows[index], all_fields)
        ].append(index)
    for index in unmatched_after:
        after_fingerprints[
            distillery_fingerprint(after_rows[index], all_fields)
        ].append(index)
    for fingerprint in sorted(
        set(before_fingerprints) & set(after_fingerprints)
    ):
        left = before_fingerprints[fingerprint]
        right = after_fingerprints[fingerprint]
        while left and right:
            before_index = left.popleft()
            after_index = right.popleft()
            unmatched_before.discard(before_index)
            unmatched_after.discard(after_index)
            pairs.append(
                distillery_make_pair(
                    before_document,
                    after_document,
                    before_index,
                    after_index,
                    profile,
                    "exact_payload",
                    1.0,
                    outcome_aliases=outcome_aliases,
                )
            )
    for identity_fields in matching.get("identity_groups") or []:
        before_index: dict[tuple[str, ...], list[int]] = defaultdict(list)
        after_index: dict[tuple[str, ...], list[int]] = defaultdict(list)
        for index in unmatched_before:
            key = distillery_identity_key(
                before_rows[index], identity_fields
            )
            if key:
                before_index[key].append(index)
        for index in unmatched_after:
            key = distillery_identity_key(after_rows[index], identity_fields)
            if key:
                after_index[key].append(index)
        for key in sorted(set(before_index) & set(after_index)):
            left = before_index[key]
            right = after_index[key]
            if len(left) != 1 or len(right) != 1:
                continue
            before_row_index = left[0]
            after_row_index = right[0]
            unmatched_before.discard(before_row_index)
            unmatched_after.discard(after_row_index)
            pairs.append(
                distillery_make_pair(
                    before_document,
                    after_document,
                    before_row_index,
                    after_row_index,
                    profile,
                    "unique_identity",
                    0.98,
                    outcome_aliases=outcome_aliases,
                )
            )
    similarity_fields = matching.get("similarity_fields") or all_fields
    proposals: list[tuple[float, float, int, int]] = []
    minimum_similarity = float(matching.get("minimum_similarity", 0.72))
    ambiguity_margin = float(matching.get("ambiguity_margin", 0.05))
    for before_index in unmatched_before:
        scores = sorted(
            (
                (
                    distillery_similarity(
                        before_rows[before_index],
                        after_rows[after_index],
                        similarity_fields,
                    ),
                    after_index,
                )
                for after_index in unmatched_after
            ),
            reverse=True,
        )
        if not scores or scores[0][0] < minimum_similarity:
            continue
        margin = scores[0][0] - (scores[1][0] if len(scores) > 1 else 0.0)
        for score, after_index in scores:
            if score < minimum_similarity:
                break
            proposals.append((score, margin, before_index, after_index))
    for score, margin, before_index, after_index in sorted(
        proposals,
        key=lambda item: (item[0], item[1]),
        reverse=True,
    ):
        if (
            before_index not in unmatched_before
            or after_index not in unmatched_after
        ):
            continue
        unmatched_before.remove(before_index)
        unmatched_after.remove(after_index)
        pairs.append(
            distillery_make_pair(
                before_document,
                after_document,
                before_index,
                after_index,
                profile,
                "similarity",
                score,
                ambiguous=margin < ambiguity_margin,
                outcome_aliases=outcome_aliases,
            )
        )
    unmatched = [
        {
            "side": "before",
            "source_group": before_document["source_group"],
            "row_index": index,
        }
        for index in sorted(unmatched_before)
    ]
    unmatched.extend(
        {
            "side": "after",
            "source_group": after_document["source_group"],
            "row_index": index,
        }
        for index in sorted(unmatched_after)
    )
    return sorted(pairs, key=lambda pair: pair.before_index), unmatched


def distillery_match_documents(
    before_documents: Sequence[Mapping[str, Any]],
    after_documents: Sequence[Mapping[str, Any]],
    profile: Mapping[str, Any],
    outcome_aliases: Mapping[str, Mapping[str, Any]] | None = None,
) -> tuple[tuple[DistilleryPair, ...], tuple[dict[str, Any], ...]]:
    def index(
        documents: Sequence[Mapping[str, Any]],
        side: str,
    ) -> dict[str, Mapping[str, Any]]:
        output: dict[str, Mapping[str, Any]] = {}
        for document in documents:
            group = clean_text(document.get("source_group"))
            if group in output:
                raise ValueError(f"Duplicate {side} source group {group!r}.")
            output[group] = document
        return output

    before_by_group = index(before_documents, "BEFORE")
    after_by_group = index(after_documents, "AFTER")
    missing_after = sorted(set(before_by_group) - set(after_by_group))
    missing_before = sorted(set(after_by_group) - set(before_by_group))
    if missing_after or missing_before:
        raise ValueError(
            f"Unpaired source groups: missing AFTER={missing_after}, "
            f"missing BEFORE={missing_before}"
        )
    pairs: list[DistilleryPair] = []
    unmatched: list[dict[str, Any]] = []
    for group in sorted(before_by_group):
        group_pairs, group_unmatched = distillery_match_document_pair(
            before_by_group[group],
            after_by_group[group],
            profile,
            outcome_aliases,
        )
        pairs.extend(group_pairs)
        unmatched.extend(group_unmatched)
    return tuple(pairs), tuple(unmatched)


def distillery_product_request_features(
    row: Mapping[str, Any],
) -> dict[str, Any]:
    values = {distillery_field(key): value for key, value in row.items()}
    values["input_action"] = values.get("action")
    values["input_if_in_stock_action"] = values.get("if_in_stock_action")
    values["input_audit_action"] = values.get("audit_action")
    values["input_buysmart_action"] = values.get("buysmart_action")
    business = clean_text(values.get("business"))
    request_type = clean_text(values.get("type"))
    sector = clean_text(values.get("sector")).lower()
    division = clean_text(values.get("division")).lower()
    compass_apl = clean_text(values.get("compass_apl")).lower()
    pantry = clean_text(values.get("pantry")).lower()
    in_cat = clean_text(values.get("in_cat")).lower()
    duration = clean_text(values.get("one_time_or_permanent"))
    conversion_din = clean_text(values.get("conversion_din"))
    values.update(
        {
            "business_key": normalize_key(business),
            "request_type_key": normalize_key(request_type),
            "usage_num": distillery_number(values.get("usage")),
            "meets_criteria_num": distillery_number(
                values.get("meets_criteria")
            ),
            "conversion_va_num": distillery_number(
                values.get("conversion_va")
            ),
            "is_one_time": bool(
                re.search(r"one-time|one time|seasonal", duration, re.I)
            ),
            "is_permanent": "permanent" in duration.lower(),
            "is_in_cat_y": in_cat == "y",
            "is_temp_available": "temp available" in in_cat or in_cat == "ta",
            "is_in_catalog": in_cat == "y" or "temp available" in in_cat,
            "is_pantry": "item" in pantry
            or "subcategory" in pantry
            or pantry == "y",
            "is_k12_apl": clean_text(values.get("k12_apl")).lower() == "y",
            "is_core_apl": "core apl" in compass_apl,
            "is_s1": bool(re.search(r"\bs1\b", compass_apl, re.I)),
            "is_foh": "front of house" in compass_apl
            or bool(re.search(r"\bfoh\b", compass_apl, re.I)),
            "is_diverse": "diverse" in compass_apl,
            "has_conversion": bool(conversion_din),
            "is_levy": "levy" in sector or "levy" in division,
            "is_schools": "school" in division
            or "chartwells" in division,
        }
    )
    return values


DISTILLERY_PROJECTORS: dict[
    str,
    Callable[[Mapping[str, Any]], Mapping[str, Any]],
] = {
    "product_request": distillery_product_request_features,
}


def distillery_project_pairs(
    pairs: Sequence[DistilleryPair],
    profile: Mapping[str, Any],
) -> tuple[DistilleryProjected, ...]:
    projector_name = clean_text(profile.get("feature_projector"))
    projector = DISTILLERY_PROJECTORS.get(projector_name)
    if projector is None:
        projector = lambda row: {
            distillery_field(key): value for key, value in row.items()
        }
    output: list[DistilleryProjected] = []
    for pair in pairs:
        features = dict(projector(pair.before))
        features["__evidence_hash"] = distillery_evidence_hash(pair.before)
        output.append(
            DistilleryProjected(
                pair=pair,
                features=features,
                label=tuple(sorted(pair.outputs.items())),
            )
        )
    return tuple(output)


def distillery_evaluate_atom(
    atom: DistilleryAtom,
    features: Mapping[str, Any],
) -> bool:
    left = features.get(atom.field)
    right = atom.value
    operator = atom.operator
    if operator == "eq":
        return normalize_key(left) == normalize_key(right)
    if operator == "ne":
        return normalize_key(left) != normalize_key(right)
    if operator in {"in", "not_in"}:
        options = (
            right if isinstance(right, (list, tuple, set)) else [right]
        )
        matched = normalize_key(left) in {
            normalize_key(item) for item in options
        }
        return matched if operator == "in" else not matched
    if operator == "blank":
        return not clean_text(left)
    if operator == "not_blank":
        return bool(clean_text(left))
    if operator == "is_true":
        return bool(left)
    if operator == "is_false":
        return not bool(left)
    if operator == "contains":
        return clean_text(right).lower() in clean_text(left).lower()
    if operator == "not_contains":
        return clean_text(right).lower() not in clean_text(left).lower()
    if operator in DATE_OPERATORS:
        left_date = _date_for_compare(left)
        right_date = _date_for_compare(right)
        if left_date is None or right_date is None:
            return False
        if operator == "date_before":
            return left_date < right_date
        if operator == "date_on_or_before":
            return left_date <= right_date
        if operator == "date_after":
            return left_date > right_date
        return left_date >= right_date
    left_number = distillery_number(left)
    right_number = distillery_number(right)
    if left_number is None or right_number is None:
        return False
    comparisons = {
        "ge": left_number >= right_number,
        "gt": left_number > right_number,
        "lt": left_number < right_number,
        "le": left_number <= right_number,
    }
    return comparisons.get(operator, False)


def distillery_inverse_atom(atom: DistilleryAtom) -> DistilleryAtom:
    inverse = {
        "eq": "ne",
        "ne": "eq",
        "in": "not_in",
        "not_in": "in",
        "blank": "not_blank",
        "not_blank": "blank",
        "is_true": "is_false",
        "is_false": "is_true",
        "contains": "not_contains",
        "not_contains": "contains",
        "ge": "lt",
        "lt": "ge",
        "gt": "le",
        "le": "gt",
        "date_before": "date_on_or_after",
        "date_on_or_after": "date_before",
        "date_after": "date_on_or_before",
        "date_on_or_before": "date_after",
    }
    return DistilleryAtom(atom.field, inverse[atom.operator], atom.value)


def distillery_tokens(value: Any) -> set[str]:
    tokens = {
        token.lower()
        for token in re.findall(
            r"[A-Za-z0-9][A-Za-z0-9&'/-]{2,}",
            clean_text(value),
        )
    }
    return {
        token for token in tokens if token not in DISTILLERY_STOP_WORDS
    }


def distillery_sample(values: Sequence[Any], maximum: int) -> list[Any]:
    if len(values) <= maximum:
        return list(values)
    if maximum <= 1:
        return [values[len(values) // 2]]
    indexes = {
        round(index * (len(values) - 1) / (maximum - 1))
        for index in range(maximum)
    }
    return [values[index] for index in sorted(indexes)]


class SingleFileRuleInducer:
    def __init__(self, profile: Mapping[str, Any]):
        self.profile = profile
        self.config = profile.get("induction") or {}
        self.rows: Sequence[DistilleryProjected] = ()
        self.candidate_coverages: tuple[
            tuple[DistilleryAtom, frozenset[int]], ...
        ] = ()
        self.identity_labels: dict[
            tuple[str, ...],
            dict[tuple[str, ...], frozenset[tuple[tuple[str, Any], ...]]],
        ] = {}
        self.normalized_values: dict[str, tuple[str, ...]] = {}
        self.normalized_indexes: dict[
            str,
            dict[str, frozenset[int]],
        ] = {}
        self.lower_values: dict[str, tuple[str, ...]] = {}
        self.numeric_values: dict[str, tuple[float | None, ...]] = {}
        self.date_values: dict[str, tuple[datetime | None, ...]] = {}
        self.token_values: dict[str, tuple[frozenset[str], ...]] = {}
        self.candidate_matrix: Any = None
        self.label_codes: Any = None
        self.label_count = 0
        self.universe_indexes: frozenset[int] = frozenset()

    def prepare_feature_caches(self) -> None:
        self.universe_indexes = frozenset(range(len(self.rows)))
        fields = {
            field
            for field in self.config.get("feature_fields") or []
            if field != "*"
        }
        fields.update(
            distillery_field(field)
            for group in self.config.get("exception_identity_groups") or []
            for field in group
        )
        numeric_fields = set(self.config.get("numeric_fields") or [])
        date_fields = set(self.config.get("date_fields") or [])
        token_fields = set(self.config.get("token_fields") or [])
        for field in fields:
            values = tuple(row.features.get(field) for row in self.rows)
            self.normalized_values[field] = tuple(
                normalize_key(value) for value in values
            )
            field_indexes: dict[str, set[int]] = defaultdict(set)
            for index, value in enumerate(self.normalized_values[field]):
                field_indexes[value].add(index)
            self.normalized_indexes[field] = {
                value: frozenset(indexes)
                for value, indexes in field_indexes.items()
            }
            self.lower_values[field] = tuple(
                clean_text(value).lower() for value in values
            )
            if field in numeric_fields:
                self.numeric_values[field] = tuple(
                    distillery_number(value) for value in values
                )
            if field in date_fields:
                self.date_values[field] = tuple(
                    _date_for_compare(value) for value in values
                )
            if field in token_fields:
                self.token_values[field] = tuple(
                    frozenset(distillery_tokens(value)) for value in values
                )

    def candidate_atoms(
        self,
        indices: Sequence[int],
    ) -> Iterable[DistilleryAtom]:
        numeric_fields = set(self.config.get("numeric_fields") or [])
        date_fields = set(self.config.get("date_fields") or [])
        token_fields = set(self.config.get("token_fields") or [])
        for field in self.config.get("feature_fields") or []:
            if field == "*":
                continue
            values = [self.rows[index].features.get(field) for index in indices]
            normalized_values = self.normalized_values[field]
            if field in date_fields:
                dates = sorted(
                    {
                        value
                        for index in indices
                        if (value := self.date_values[field][index])
                        is not None
                    }
                )
                for threshold in distillery_sample(
                    dates[1:],
                    int(self.config.get("maximum_date_splits", 16)),
                ):
                    value = threshold.date().isoformat()
                    yield DistilleryAtom(
                        field,
                        "date_on_or_after",
                        value,
                    )
                    yield DistilleryAtom(field, "date_before", value)
                if any(
                    value is None or not clean_text(value)
                    for value in values
                ):
                    yield DistilleryAtom(field, "blank")
                    yield DistilleryAtom(field, "not_blank")
                continue
            if field in numeric_fields:
                numbers = sorted(
                    {
                        number
                        for index in indices
                        if (
                            number := self.numeric_values[field][index]
                        )
                        is not None
                    }
                )
                if len(numbers) > 1:
                    thresholds = [
                        (left + right) / 2.0
                        for left, right in zip(numbers, numbers[1:])
                    ]
                    for threshold in distillery_sample(
                        thresholds,
                        int(self.config.get("maximum_numeric_splits", 16)),
                    ):
                        yield DistilleryAtom(field, "ge", threshold)
                        yield DistilleryAtom(field, "lt", threshold)
                if any(
                    value is None or not clean_text(value) for value in values
                ):
                    yield DistilleryAtom(field, "blank")
                    yield DistilleryAtom(field, "not_blank")
                continue
            non_blank_indices = [
                index for index in indices if normalized_values[index]
            ]
            non_blank = [
                self.rows[index].features.get(field)
                for index in non_blank_indices
            ]
            if len(non_blank_indices) < len(indices) and non_blank:
                yield DistilleryAtom(field, "blank")
                yield DistilleryAtom(field, "not_blank")
            if not non_blank:
                continue
            if all(isinstance(value, bool) for value in non_blank):
                yield DistilleryAtom(field, "is_true")
                yield DistilleryAtom(field, "is_false")
                continue
            counts = Counter(
                normalized_values[index] for index in non_blank_indices
            )
            display_values: dict[str, Any] = {}
            for index in non_blank_indices:
                display_values.setdefault(
                    normalized_values[index],
                    self.rows[index].features.get(field),
                )
            maximum_categories = int(
                self.config.get("maximum_category_splits", 16)
            )
            for value, _ in counts.most_common(maximum_categories):
                yield DistilleryAtom(field, "eq", display_values[value])
            labels_by_value: dict[
                str,
                Counter[tuple[tuple[str, Any], ...]],
            ] = defaultdict(Counter)
            for index in indices:
                if normalized_values[index]:
                    labels_by_value[normalized_values[index]][
                        self.rows[index].label
                    ] += 1
            grouped: dict[
                tuple[tuple[str, Any], ...],
                list[str],
            ] = defaultdict(list)
            for value, label_counts in labels_by_value.items():
                grouped[label_counts.most_common(1)[0][0]].append(value)
            for grouped_values in grouped.values():
                if 1 < len(grouped_values) <= int(
                    self.config.get("maximum_in_list_size", 64)
                ):
                    ordered = sorted(
                        grouped_values,
                        key=lambda value: counts[value],
                        reverse=True,
                    )
                    yield DistilleryAtom(
                        field,
                        "in",
                        [display_values[value] for value in ordered],
                    )
            if field in token_fields:
                token_counts: Counter[str] = Counter()
                for index in indices:
                    token_counts.update(self.token_values[field][index])
                for token, count in token_counts.most_common(
                    int(self.config.get("maximum_token_splits", 24))
                ):
                    if (
                        count
                        >= int(self.config.get("minimum_token_support", 5))
                        and count < len(indices)
                    ):
                        yield DistilleryAtom(field, "contains", token)

    def coverage_for_atom(self, atom: DistilleryAtom) -> frozenset[int]:
        field = atom.field
        operator = atom.operator
        if operator in {
            "eq",
            "ne",
            "in",
            "not_in",
            "blank",
            "not_blank",
        }:
            values = self.normalized_values[field]
            universe = self.universe_indexes
            if operator in {"eq", "ne"}:
                target = normalize_key(atom.value)
                matched = set(
                    self.normalized_indexes[field].get(
                        target,
                        frozenset(),
                    )
                )
                return frozenset(
                    matched if operator == "eq" else universe - matched
                )
            if operator in {"in", "not_in"}:
                options = (
                    atom.value
                    if isinstance(atom.value, (list, tuple, set))
                    else [atom.value]
                )
                targets = {normalize_key(value) for value in options}
                matched: set[int] = set()
                for target in targets:
                    matched.update(
                        self.normalized_indexes[field].get(
                            target,
                            frozenset(),
                        )
                    )
                return frozenset(
                    matched if operator == "in" else universe - matched
                )
            matched = set(
                self.normalized_indexes[field].get("", frozenset())
            )
            return frozenset(
                matched if operator == "blank" else universe - matched
            )
        if operator in {"is_true", "is_false"}:
            matched = {
                index
                for index, row in enumerate(self.rows)
                if bool(row.features.get(field))
            }
            universe = self.universe_indexes
            return frozenset(
                matched if operator == "is_true" else universe - matched
            )
        if operator in {"contains", "not_contains"}:
            target = clean_text(atom.value).lower()
            matched = {
                index
                for index, value in enumerate(self.lower_values[field])
                if target in value
            }
            universe = self.universe_indexes
            return frozenset(
                matched if operator == "contains" else universe - matched
            )
        if operator in DATE_OPERATORS:
            target_date = _date_for_compare(atom.value)
            if target_date is None:
                return frozenset()
            comparisons = {
                "date_before": lambda value: value < target_date,
                "date_on_or_before": lambda value: value <= target_date,
                "date_after": lambda value: value > target_date,
                "date_on_or_after": lambda value: value >= target_date,
            }
            comparator = comparisons[operator]
            return frozenset(
                index
                for index, value in enumerate(self.date_values[field])
                if value is not None and comparator(value)
            )
        target = distillery_number(atom.value)
        if target is None:
            return frozenset()
        comparisons = {
            "ge": lambda value: value >= target,
            "gt": lambda value: value > target,
            "lt": lambda value: value < target,
            "le": lambda value: value <= target,
        }
        comparator = comparisons[operator]
        return frozenset(
            index
            for index, value in enumerate(self.numeric_values[field])
            if value is not None and comparator(value)
        )

    def prepare_indexes(self) -> None:
        self.prepare_feature_caches()
        all_indices = tuple(range(len(self.rows)))
        candidates: list[tuple[DistilleryAtom, frozenset[int]]] = []
        seen: set[tuple[str, str, str]] = set()
        for atom in self.candidate_atoms(all_indices):
            signature = (atom.field, atom.operator, repr(atom.value))
            if signature in seen:
                continue
            seen.add(signature)
            coverage = self.coverage_for_atom(atom)
            if coverage and len(coverage) < len(self.rows):
                candidates.append((atom, coverage))
        self.candidate_coverages = tuple(candidates)
        try:
            import numpy as np
        except Exception as exc:
            raise RuntimeError(
                "Rules Distillery requires numpy, which is included with pandas."
            ) from exc
        matrix = np.zeros(
            (len(self.rows), len(self.candidate_coverages)),
            dtype=np.bool_,
        )
        for column, (_, coverage) in enumerate(self.candidate_coverages):
            if coverage:
                matrix[
                    np.fromiter(coverage, dtype=np.int64),
                    column,
                ] = True
        labels = list(dict.fromkeys(row.label for row in self.rows))
        label_code = {label: index for index, label in enumerate(labels)}
        self.candidate_matrix = matrix
        self.label_codes = np.fromiter(
            (label_code[row.label] for row in self.rows),
            dtype=np.int32,
        )
        self.label_count = len(labels)
        identity_labels: dict[
            tuple[str, ...],
            dict[tuple[str, ...], set[tuple[tuple[str, Any], ...]]],
        ] = {}
        for group in self.config.get("exception_identity_groups") or []:
            canonical_group = tuple(distillery_field(field) for field in group)
            index: dict[
                tuple[str, ...],
                set[tuple[tuple[str, Any], ...]],
            ] = defaultdict(set)
            for row in self.rows:
                key = tuple(
                    normalize_key(row.features.get(field))
                    for field in canonical_group
                )
                if key and all(key):
                    index[key].add(row.label)
            identity_labels[canonical_group] = index
        self.identity_labels = {
            group: {
                key: frozenset(labels) for key, labels in values.items()
            }
            for group, values in identity_labels.items()
        }

    def best_split(
        self,
        indices: Sequence[int],
    ) -> DistillerySplit | None:
        import numpy as np

        node_indices = np.asarray(indices, dtype=np.int64)
        node_labels = self.label_codes[node_indices]
        parent_counts = np.bincount(
            node_labels,
            minlength=self.label_count,
        ).astype(np.float64)
        probabilities = parent_counts[parent_counts > 0] / len(node_indices)
        parent_entropy = float(
            -(probabilities * np.log2(probabilities)).sum()
        )
        if parent_entropy <= 0.0 or self.candidate_matrix.shape[1] == 0:
            return None
        matrix = self.candidate_matrix[node_indices]
        true_totals = matrix.sum(axis=0, dtype=np.int64)
        false_totals = len(node_indices) - true_totals
        minimum_leaf = int(self.config.get("minimum_leaf_size", 1))
        valid = (true_totals >= minimum_leaf) & (
            false_totals >= minimum_leaf
        )
        if not bool(valid.any()):
            return None
        true_counts = np.zeros(
            (self.label_count, matrix.shape[1]),
            dtype=np.float64,
        )
        for label_code in range(self.label_count):
            label_mask = node_labels == label_code
            if bool(label_mask.any()):
                true_counts[label_code] = matrix[label_mask].sum(
                    axis=0,
                    dtype=np.int64,
                )
        false_counts = parent_counts[:, None] - true_counts

        def entropy_columns(counts: Any, totals: Any) -> Any:
            with np.errstate(divide="ignore", invalid="ignore"):
                values = np.divide(
                    counts,
                    totals[None, :],
                    out=np.zeros_like(counts),
                    where=totals[None, :] > 0,
                )
                terms = np.where(
                    values > 0,
                    values * np.log2(values),
                    0.0,
                )
            return -terms.sum(axis=0)

        child_entropy = (
            true_totals
            / len(node_indices)
            * entropy_columns(true_counts, true_totals)
            + false_totals
            / len(node_indices)
            * entropy_columns(false_counts, false_totals)
        )
        gains = parent_entropy - child_entropy
        gains[~valid] = -np.inf
        balance = np.minimum(true_totals, false_totals)
        scores = gains + balance / max(len(node_indices), 1) * 1e-12
        best_column = int(np.argmax(scores))
        gain = float(gains[best_column])
        if (
            not math.isfinite(gain)
            or gain < float(self.config.get("minimum_gain", 1e-9))
        ):
            return None
        true_mask = matrix[:, best_column]
        return DistillerySplit(
            atom=self.candidate_coverages[best_column][0],
            gain=gain,
            true_indices=tuple(
                int(value) for value in node_indices[true_mask]
            ),
            false_indices=tuple(
                int(value) for value in node_indices[~true_mask]
            ),
        )

    def build_leaves(
        self,
        indices: tuple[int, ...],
        path: tuple[DistilleryAtom, ...],
        depth: int,
    ) -> list[DistilleryLeaf]:
        counts = Counter(self.rows[index].label for index in indices)
        predicted, count = counts.most_common(1)[0]
        confidence = count / len(indices)
        if (
            confidence == 1.0
            or depth >= int(self.config.get("maximum_depth", 16))
        ):
            return [DistilleryLeaf(path, indices, predicted, confidence)]
        split = self.best_split(indices)
        if split is None:
            return [DistilleryLeaf(path, indices, predicted, confidence)]
        return [
            *self.build_leaves(
                split.true_indices,
                (*path, split.atom),
                depth + 1,
            ),
            *self.build_leaves(
                split.false_indices,
                (*path, distillery_inverse_atom(split.atom)),
                depth + 1,
            ),
        ]

    @staticmethod
    def rule_id(
        prefix: str,
        path: Sequence[DistilleryAtom],
        label: tuple[tuple[str, Any], ...],
    ) -> str:
        payload = repr((tuple(path), label)).encode("utf-8")
        return (
            f"{prefix}-"
            f"{hashlib.sha256(payload).hexdigest()[:12].upper()}"
        )

    def general_rules(
        self,
        leaves: Sequence[DistilleryLeaf],
    ) -> list[DistilleryRule]:
        rules: list[DistilleryRule] = []
        ordered = sorted(
            leaves,
            key=lambda leaf: (
                -len(leaf.path),
                -leaf.confidence,
                -len(leaf.indices),
            ),
        )
        minimum_support = int(
            self.config.get("minimum_general_support", 3)
        )
        profile_id = clean_text(self.profile.get("profile_id")).upper()
        for index, leaf in enumerate(ordered):
            if (
                leaf.confidence < 1.0
                or len(leaf.indices) < minimum_support
            ):
                continue
            rules.append(
                DistilleryRule(
                    rule_id=self.rule_id(
                        f"DISTILLED-{profile_id}-GENERAL",
                        leaf.path,
                        leaf.predicted,
                    ),
                    priority=100_000 + index,
                    predicates=leaf.path,
                    outputs=dict(leaf.predicted),
                    support=len(leaf.indices),
                    confidence=1.0,
                    source_groups=tuple(
                        sorted(
                            {
                                self.rows[row_index].pair.source_group
                                for row_index in leaf.indices
                            }
                        )
                    ),
                    kind="general",
                    evidence_ids=tuple(
                        self.rows[row_index].pair.pair_id
                        for row_index in leaf.indices
                    ),
                )
            )
        return rules

    @staticmethod
    def predict(
        features: Mapping[str, Any],
        rules: Sequence[DistilleryRule],
    ) -> Mapping[str, Any] | None:
        for rule in sorted(rules, key=lambda item: item.priority):
            if all(
                distillery_evaluate_atom(atom, features)
                for atom in rule.predicates
            ):
                return rule.outputs
        return None

    def exception_rules(
        self,
        general_rules: Sequence[DistilleryRule],
    ) -> list[DistilleryRule]:
        residuals = [
            index
            for index, row in enumerate(self.rows)
            if self.predict(row.features, general_rules) != dict(row.label)
        ]
        grouped: dict[
            tuple[str, tuple[tuple[str, Any], ...]],
            dict[str, Any],
        ] = {}
        for index in residuals:
            row = self.rows[index]
            selector_field = "__evidence_hash"
            selector_value = row.features[selector_field]
            for identity_group, labels_by_key in self.identity_labels.items():
                if len(identity_group) != 1:
                    continue
                field = identity_group[0]
                value = row.features.get(field)
                if not clean_text(value):
                    continue
                key = (normalize_key(value),)
                if labels_by_key.get(key) == frozenset({row.label}):
                    selector_field = field
                    selector_value = value
                    break
            bucket = grouped.setdefault(
                (selector_field, row.label),
                {"values": [], "indices": []},
            )
            bucket["values"].append(selector_value)
            bucket["indices"].append(index)
        rules: list[DistilleryRule] = []
        profile_id = clean_text(self.profile.get("profile_id")).upper()
        for rule_index, ((field, label), bucket) in enumerate(
            grouped.items()
        ):
            values = list(dict.fromkeys(bucket["values"]))
            indices = list(bucket["indices"])
            predicate = DistilleryAtom(
                field,
                "eq" if len(values) == 1 else "in",
                values[0] if len(values) == 1 else values,
            )
            rules.append(
                DistilleryRule(
                    rule_id=self.rule_id(
                        f"DISTILLED-{profile_id}-EXCEPTION",
                        (predicate,),
                        label,
                    ),
                    priority=10_000 + rule_index,
                    predicates=(predicate,),
                    outputs=dict(label),
                    support=len(indices),
                    confidence=1.0,
                    source_groups=tuple(
                        sorted(
                            {
                                self.rows[index].pair.source_group
                                for index in indices
                            }
                        )
                    ),
                    kind="exception",
                    evidence_ids=tuple(
                        self.rows[index].pair.pair_id for index in indices
                    ),
                )
            )
        return rules

    def fit(
        self,
        rows: Sequence[DistilleryProjected],
        *,
        include_exceptions: bool = True,
    ) -> tuple[DistilleryRule, ...]:
        if not rows:
            return ()
        self.rows = rows
        self.prepare_indexes()
        leaves = self.build_leaves(tuple(range(len(rows))), (), 0)
        general = self.general_rules(leaves)
        exceptions = self.exception_rules(general) if include_exceptions else []
        return tuple(
            sorted([*exceptions, *general], key=lambda rule: rule.priority)
        )


def literal_atom_signature(atom: DistilleryAtom) -> tuple[str, str, str]:
    value = atom.value
    if isinstance(value, (list, tuple, set)):
        normalized = sorted(normalize_key(item) for item in value)
        value_signature = json.dumps(normalized, separators=(",", ":"))
    else:
        value_signature = normalize_key(value)
    return atom.field, atom.operator, value_signature


def literal_logic_signature(
    predicates: Sequence[DistilleryAtom],
    outputs: Mapping[str, Any] | Sequence[tuple[str, Any]],
) -> str:
    label = dict(outputs)
    payload = {
        "predicates": [
            {
                "field": atom.field,
                "op": atom.operator,
                "value": (
                    sorted(
                        [clean_text(value) for value in atom.value],
                        key=normalize_key,
                    )
                    if isinstance(atom.value, (list, tuple, set))
                    else _plain_data(atom.value)
                ),
            }
            for atom in sorted(predicates, key=literal_atom_signature)
        ],
        "outputs": {
            key: clean_text(value) for key, value in sorted(label.items())
        },
    }
    return hashlib.sha256(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def literal_filter_name(
    profile_id: str,
    predicates: Sequence[DistilleryAtom],
    outputs: Mapping[str, Any],
) -> str:
    profile_name = clean_text(profile_id).replace("_", " ").title()
    filter_parts: list[str] = []
    for atom in predicates[:4]:
        label = FIELD_LABELS.get(atom.field, atom.field.replace("_", " ").title())
        if atom.operator in {"blank", "not_blank", "is_true", "is_false"}:
            value = OPERATOR_LABELS.get(atom.operator, atom.operator)
        elif isinstance(atom.value, (list, tuple, set)):
            values = [clean_text(value) for value in atom.value]
            value = "/".join(values[:3]) + ("…" if len(values) > 3 else "")
        else:
            value = clean_text(atom.value)
        filter_parts.append(f"{label} {value}".strip())
    outcome_parts = [
        clean_text(outputs.get("action")) or "blank ACTION",
        (
            f"stock {clean_text(outputs.get('if_in_stock_action'))}"
            if clean_text(outputs.get("if_in_stock_action"))
            else ""
        ),
        (
            f"audit {clean_text(outputs.get('audit_action'))}"
            if clean_text(outputs.get("audit_action"))
            else ""
        ),
    ]
    outcome = " / ".join(part for part in outcome_parts if part)
    filters = " · ".join(filter_parts) or "All rows"
    return f"{profile_name} — {filters} — {outcome}"


class LiteralFilterMiner:
    """Discover readable pure conjunctions for literal AFTER permutations."""

    def __init__(self, profile: Mapping[str, Any]):
        self.profile = deepcopy(dict(profile))
        self.config = self.profile.get("induction") or {}
        self.rows: tuple[DistilleryProjected, ...] = ()
        self.atom_coverages: list[tuple[DistilleryAtom, int]] = []
        self.coverage_by_signature: dict[
            tuple[str, str, str],
            int,
        ] = {}
        self.filters_by_label: dict[
            tuple[tuple[str, Any], ...],
            list[tuple[tuple[DistilleryAtom, ...], int]],
        ] = defaultdict(list)
        self.labels: list[tuple[tuple[str, Any], ...]] = []
        self.all_mask = 0

    @staticmethod
    def mask_for_indexes(indexes: Iterable[int]) -> int:
        mask = 0
        for index in indexes:
            mask |= 1 << int(index)
        return mask

    @staticmethod
    def indexes_for_mask(mask: int) -> Iterator[int]:
        while mask:
            least = mask & -mask
            yield least.bit_length() - 1
            mask ^= least

    def prepare(self, rows: Sequence[DistilleryProjected]) -> None:
        self.rows = tuple(rows)
        self.labels = [row.label for row in self.rows]
        self.all_mask = (1 << len(self.rows)) - 1 if self.rows else 0
        literal_profile = deepcopy(self.profile)
        induction = literal_profile.setdefault("induction", {})
        governed = [
            clean_text(field)
            for field in induction.get("governed_fields") or []
            if clean_text(field)
        ]
        prohibited = {
            clean_text(field)
            for field in induction.get("prohibited_predicate_fields") or []
        }
        induction["feature_fields"] = [
            field for field in governed if field not in prohibited
        ]
        induction["maximum_category_splits"] = max(
            int(
                induction.get(
                    "literal_maximum_category_splits",
                    induction.get("maximum_category_splits", 16),
                )
            ),
            128,
        )
        induction["maximum_numeric_splits"] = max(
            int(induction.get("maximum_numeric_splits", 16)),
            32,
        )
        induction["maximum_token_splits"] = max(
            int(
                induction.get(
                    "literal_maximum_token_splits",
                    induction.get("maximum_token_splits", 24),
                )
            ),
            64,
        )
        indexer = SingleFileRuleInducer(literal_profile)
        indexer.rows = self.rows
        indexer.prepare_feature_caches()
        seen: set[tuple[str, str, str]] = set()
        atoms: list[tuple[DistilleryAtom, int]] = []
        for atom in indexer.candidate_atoms(tuple(range(len(self.rows)))):
            if atom.field in prohibited or atom.field not in governed:
                continue
            signature = literal_atom_signature(atom)
            if signature in seen:
                continue
            seen.add(signature)
            coverage = indexer.coverage_for_atom(atom)
            if not coverage or len(coverage) == len(self.rows):
                continue
            atoms.append((atom, self.mask_for_indexes(coverage)))
        self.atom_coverages = sorted(
            atoms,
            key=lambda item: literal_atom_signature(item[0]),
        )
        self.coverage_by_signature = {
            literal_atom_signature(atom): coverage
            for atom, coverage in self.atom_coverages
        }

    def path_mask(
        self,
        path: Sequence[DistilleryAtom],
        scope_mask: int | None = None,
    ) -> int:
        mask = self.all_mask if scope_mask is None else scope_mask
        for atom in path:
            coverage = self.coverage_by_signature.get(
                literal_atom_signature(atom)
            )
            if coverage is None:
                coverage = self.mask_for_indexes(
                    index
                    for index, row in enumerate(self.rows)
                    if distillery_evaluate_atom(atom, row.features)
                )
            mask &= coverage
            if not mask:
                break
        return mask

    def labels_for_mask(
        self,
        mask: int,
    ) -> set[tuple[tuple[str, Any], ...]]:
        return {self.labels[index] for index in self.indexes_for_mask(mask)}

    def minimize_path(
        self,
        path: Sequence[DistilleryAtom],
        expected: tuple[tuple[str, Any], ...],
    ) -> tuple[DistilleryAtom, ...]:
        minimized = list(sorted(path, key=literal_atom_signature))
        changed = True
        while changed and len(minimized) > 1:
            changed = False
            for index in range(len(minimized)):
                candidate = minimized[:index] + minimized[index + 1 :]
                coverage = self.path_mask(candidate)
                if coverage and self.labels_for_mask(coverage) == {expected}:
                    minimized = candidate
                    changed = True
                    break
        return tuple(minimized)

    def scope_candidates(
        self,
        scope_mask: int,
        target_mask: int,
    ) -> list[tuple[tuple[DistilleryAtom, ...], int]]:
        if not target_mask:
            return []
        seed_index = next(self.indexes_for_mask(target_mask))
        expected = self.labels[seed_index]
        global_target_mask = self.mask_for_indexes(
            index
            for index, label in enumerate(self.labels)
            if label == expected
        )
        global_negative_mask = self.all_mask & ~global_target_mask
        maximum_atoms = int(
            self.config.get("maximum_atoms_per_outcome", 64)
        )
        maximum_depth = int(self.config.get("maximum_filter_depth", 4))
        beam_width = int(self.config.get("filter_beam_width", 64))
        maximum_filters = int(
            self.config.get("maximum_filters_per_outcome", 16)
        )
        selected_filters: list[
            tuple[tuple[DistilleryAtom, ...], int]
        ] = [
            (path, coverage & target_mask)
            for path, coverage in self.filters_by_label.get(expected, [])
            if coverage & target_mask
        ]
        already_covered = 0
        for _, coverage in selected_filters:
            already_covered |= coverage
        remaining = target_mask & ~already_covered
        failed_seeds = 0
        while remaining and len(selected_filters) < maximum_filters:
            seed_index = next(self.indexes_for_mask(remaining))
            seed_bit = 1 << seed_index
            ranked_atoms: list[
                tuple[tuple[float, int, int, tuple[str, str, str]], DistilleryAtom, int]
            ] = []
            for atom, atom_mask in self.atom_coverages:
                if not (atom_mask & seed_bit):
                    continue
                positive = (atom_mask & global_target_mask).bit_count()
                negative = (atom_mask & global_negative_mask).bit_count()
                if not positive or negative == global_negative_mask.bit_count():
                    continue
                precision = positive / max(positive + negative, 1)
                ranked_atoms.append(
                    (
                        (
                            precision,
                            global_negative_mask.bit_count() - negative,
                            positive,
                            literal_atom_signature(atom),
                        ),
                        atom,
                        atom_mask,
                    )
                )
            ranked_atoms.sort(key=lambda item: item[0], reverse=True)
            greedy_mask = self.all_mask
            greedy_path: list[DistilleryAtom] = []
            greedy_pool_candidates = [
                *ranked_atoms[:256],
                *sorted(
                    ranked_atoms,
                    key=lambda item: (
                        global_negative_mask.bit_count()
                        - (item[2] & global_negative_mask).bit_count(),
                        (item[2] & global_target_mask).bit_count(),
                        item[0],
                    ),
                    reverse=True,
                )[:256],
            ]
            greedy_pool = []
            greedy_seen: set[tuple[str, str, str]] = set()
            for item in greedy_pool_candidates:
                signature = literal_atom_signature(item[1])
                if signature in greedy_seen:
                    continue
                greedy_seen.add(signature)
                greedy_pool.append(item)
            for _ in range(
                int(self.config.get("maximum_greedy_filter_depth", 8))
            ):
                current_negative = (
                    greedy_mask & global_negative_mask
                ).bit_count()
                if current_negative == 0:
                    break
                best_greedy = max(
                    greedy_pool,
                    key=lambda item: (
                        current_negative
                        - (
                            greedy_mask
                            & item[2]
                            & global_negative_mask
                        ).bit_count(),
                        (
                            greedy_mask
                            & item[2]
                            & global_target_mask
                        ).bit_count(),
                        item[0],
                    ),
                    default=None,
                )
                if best_greedy is None:
                    break
                narrowed = greedy_mask & best_greedy[2]
                if (
                    not (narrowed & seed_bit)
                    or (
                        narrowed & global_negative_mask
                    ).bit_count()
                    >= current_negative
                ):
                    break
                greedy_path.append(best_greedy[1])
                greedy_mask = narrowed
                greedy_pool.remove(best_greedy)
            greedy_filter = (
                (
                    tuple(
                        sorted(
                            greedy_path,
                            key=literal_atom_signature,
                        )
                    ),
                    greedy_mask,
                )
                if greedy_path
                and not (greedy_mask & global_negative_mask)
                else None
            )

            # Preserve several ranking views plus the greedy cover so the
            # minimizing beam is diverse without expanding every atom.
            candidate_items = [
                *ranked_atoms[: maximum_atoms // 3],
                *sorted(
                    ranked_atoms,
                    key=lambda item: (
                        global_negative_mask.bit_count()
                        - (item[2] & global_negative_mask).bit_count(),
                        (item[2] & global_target_mask).bit_count(),
                        item[0],
                    ),
                    reverse=True,
                )[: maximum_atoms // 3],
                *sorted(
                    ranked_atoms,
                    key=lambda item: (
                        (item[2] & global_target_mask).bit_count(),
                        item[0],
                    ),
                    reverse=True,
                )[: maximum_atoms // 3],
            ]
            if greedy_filter is not None:
                greedy_signatures = {
                    literal_atom_signature(atom)
                    for atom in greedy_filter[0]
                }
                candidate_items.extend(
                    item
                    for item in ranked_atoms
                    if literal_atom_signature(item[1])
                    in greedy_signatures
                )
            atoms: list[tuple[DistilleryAtom, int]] = []
            seen_atom_signatures: set[tuple[str, str, str]] = set()
            for _, atom, atom_mask in candidate_items:
                signature = literal_atom_signature(atom)
                if signature in seen_atom_signatures:
                    continue
                seen_atom_signatures.add(signature)
                atoms.append((atom, atom_mask))
                if len(atoms) >= maximum_atoms:
                    break
            states: list[tuple[int, tuple[int, ...], int]] = [
                (self.all_mask, (), -1)
            ]
            best_filter: tuple[tuple[DistilleryAtom, ...], int] | None = None
            for _depth in range(1, maximum_depth + 1):
                next_by_mask: dict[
                    int,
                    tuple[int, tuple[int, ...], int],
                ] = {}
                pure_at_depth: list[
                    tuple[tuple[DistilleryAtom, ...], int]
                ] = []
                for mask, path_indexes, last_index in states:
                    for atom_index in range(last_index + 1, len(atoms)):
                        atom, atom_mask = atoms[atom_index]
                        narrowed = mask & atom_mask
                        if not (narrowed & seed_bit) or narrowed == mask:
                            continue
                        new_indexes = (*path_indexes, atom_index)
                        if not (narrowed & global_negative_mask):
                            path = tuple(
                                sorted(
                                    (
                                        atoms[index][0]
                                        for index in new_indexes
                                    ),
                                    key=literal_atom_signature,
                                )
                            )
                            pure_at_depth.append((path, narrowed))
                            continue
                        existing = next_by_mask.get(narrowed)
                        if existing is None or new_indexes < existing[1]:
                            next_by_mask[narrowed] = (
                                narrowed,
                                new_indexes,
                                atom_index,
                            )
                if pure_at_depth:
                    # First pure depth is predicate-count minimal. Prefer the
                    # filter with widest same-outcome coverage at that depth.
                    best_filter = max(
                        pure_at_depth,
                        key=lambda item: (
                            (item[1] & global_target_mask).bit_count(),
                            tuple(
                                literal_atom_signature(atom)
                                for atom in item[0]
                            ),
                        ),
                    )
                    break
                if not next_by_mask:
                    break
                states = sorted(
                    next_by_mask.values(),
                    key=lambda state: (
                        (
                            state[0] & global_target_mask
                        ).bit_count()
                        / max(state[0].bit_count(), 1),
                        (state[0] & global_target_mask).bit_count(),
                        -(state[0] & global_negative_mask).bit_count(),
                    ),
                    reverse=True,
                )[:beam_width]
            if best_filter is None:
                best_filter = greedy_filter
            if best_filter is None:
                remaining &= ~seed_bit
                failed_seeds += 1
                if failed_seeds >= maximum_filters:
                    break
                continue
            path = self.minimize_path(best_filter[0], expected)
            coverage = self.path_mask(path)
            selected_filters.append((path, coverage & target_mask))
            signature = tuple(
                literal_atom_signature(atom) for atom in path
            )
            if all(
                tuple(
                    literal_atom_signature(atom)
                    for atom in cached_path
                )
                != signature
                for cached_path, _ in self.filters_by_label[expected]
            ):
                self.filters_by_label[expected].append((path, coverage))
            remaining &= ~coverage
        return selected_filters

    def scope_candidates_within_date(
        self,
        scope_mask: int,
        target_mask: int,
    ) -> list[tuple[tuple[DistilleryAtom, ...], int]]:
        """Return a compact pure set-cover basis for one date/outcome."""
        negative_mask = scope_mask & ~target_mask
        negative_count = negative_mask.bit_count()
        ranked: list[
            tuple[tuple[Any, ...], DistilleryAtom, int]
        ] = []
        for atom, atom_mask in self.atom_coverages:
            local_mask = atom_mask & scope_mask
            positive = (local_mask & target_mask).bit_count()
            negative = (local_mask & negative_mask).bit_count()
            if not positive or local_mask == scope_mask:
                continue
            precision = positive / max(positive + negative, 1)
            ranked.append(
                (
                    (
                        1 if negative == 0 else 0,
                        precision,
                        positive,
                        negative_count - negative,
                        literal_atom_signature(atom),
                    ),
                    atom,
                    atom_mask,
                )
            )
        ranked.sort(key=lambda item: item[0], reverse=True)
        maximum_atoms = int(
            self.config.get("maximum_atoms_per_outcome", 32)
        )
        pure = [
            item for item in ranked if not (item[2] & negative_mask)
        ]
        impure = [
            item for item in ranked if item[2] & negative_mask
        ]
        atoms = [
            (item[1], item[2])
            for item in [
                *pure[:maximum_atoms],
                *impure[:maximum_atoms],
            ][: maximum_atoms * 2]
        ]
        maximum_depth = int(self.config.get("maximum_filter_depth", 4))
        beam_width = int(self.config.get("filter_beam_width", 32))
        states: list[tuple[int, tuple[int, ...], int]] = [
            (scope_mask, (), -1)
        ]
        pure_candidates: dict[
            tuple[tuple[str, str, str], ...],
            tuple[tuple[DistilleryAtom, ...], int],
        ] = {}
        for _depth in range(1, maximum_depth + 1):
            next_by_mask: dict[
                int,
                tuple[int, tuple[int, ...], int],
            ] = {}
            for mask, path_indexes, last_index in states:
                for atom_index in range(last_index + 1, len(atoms)):
                    atom, atom_mask = atoms[atom_index]
                    narrowed = mask & atom_mask
                    if not narrowed or not (narrowed & target_mask):
                        continue
                    new_indexes = (*path_indexes, atom_index)
                    path = tuple(
                        sorted(
                            (
                                atoms[index][0]
                                for index in new_indexes
                            ),
                            key=literal_atom_signature,
                        )
                    )
                    if not (narrowed & negative_mask):
                        signature = tuple(
                            literal_atom_signature(item) for item in path
                        )
                        existing = pure_candidates.get(signature)
                        if (
                            existing is None
                            or narrowed.bit_count()
                            > existing[1].bit_count()
                        ):
                            pure_candidates[signature] = (
                                path,
                                narrowed & target_mask,
                            )
                        continue
                    existing_state = next_by_mask.get(narrowed)
                    if (
                        existing_state is None
                        or len(new_indexes) < len(existing_state[1])
                    ):
                        next_by_mask[narrowed] = (
                            narrowed,
                            new_indexes,
                            atom_index,
                        )
            if not next_by_mask:
                break
            states = sorted(
                next_by_mask.values(),
                key=lambda state: (
                    (
                        state[0] & target_mask
                    ).bit_count()
                    / max(state[0].bit_count(), 1),
                    (state[0] & target_mask).bit_count(),
                    -(state[0] & negative_mask).bit_count(),
                    -len(state[1]),
                ),
                reverse=True,
            )[:beam_width]
        ranked_candidates = sorted(
            pure_candidates.values(),
            key=lambda item: (
                item[1].bit_count(),
                -len(item[0]),
                tuple(
                    literal_atom_signature(atom) for atom in item[0]
                ),
            ),
            reverse=True,
        )
        remaining = target_mask
        selected: list[tuple[tuple[DistilleryAtom, ...], int]] = []
        maximum_filters = int(
            self.config.get("maximum_filters_per_outcome", 16)
        )
        while remaining and len(selected) < maximum_filters:
            best = max(
                ranked_candidates,
                key=lambda item: (
                    (item[1] & remaining).bit_count(),
                    item[1].bit_count(),
                    -len(item[0]),
                ),
                default=None,
            )
            if best is None or not (best[1] & remaining):
                break
            selected.append(best)
            remaining &= ~best[1]
            ranked_candidates.remove(best)
        return selected

    def merge_in_rules(
        self,
        candidates: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        output = list(candidates)
        sibling_groups: dict[
            tuple[Any, ...],
            list[tuple[dict[str, Any], DistilleryAtom]],
        ] = defaultdict(list)
        for candidate in candidates:
            path = tuple(candidate["path"])
            for atom_index, atom in enumerate(path):
                if atom.operator != "eq":
                    continue
                skeleton = tuple(
                    literal_atom_signature(other)
                    for index, other in enumerate(path)
                    if index != atom_index
                )
                sibling_groups[
                    (
                        candidate["label"],
                        atom.field,
                        skeleton,
                    )
                ].append((candidate, atom))
        for (label, field, _), siblings in sibling_groups.items():
            values = sorted(
                list(
                    {
                        clean_text(atom.value)
                        for _, atom in siblings
                    }
                ),
                key=normalize_key,
            )
            if len(values) < 2:
                continue
            first_path = tuple(siblings[0][0]["path"])
            merged_path = tuple(
                sorted(
                    [
                        atom for atom in first_path if atom.field != field
                    ]
                    + [DistilleryAtom(field, "in", values)],
                    key=literal_atom_signature,
                )
            )
            coverage = self.path_mask(merged_path)
            if not coverage or self.labels_for_mask(coverage) != {label}:
                continue
            output.append(
                {
                    "path": merged_path,
                    "label": label,
                    "coverage": coverage,
                    "discovered_groups": set().union(
                        *[
                            set(candidate["discovered_groups"])
                            for candidate, _ in siblings
                        ]
                    ),
                }
            )
        deduplicated: dict[tuple[Any, ...], dict[str, Any]] = {}
        for candidate in output:
            key = (
                candidate["label"],
                tuple(
                    literal_atom_signature(atom)
                    for atom in candidate["path"]
                ),
            )
            existing = deduplicated.get(key)
            if existing is None:
                deduplicated[key] = candidate
            else:
                existing["coverage"] |= candidate["coverage"]
                existing["discovered_groups"].update(
                    candidate["discovered_groups"]
                )
        return list(deduplicated.values())

    def fit(self, rows: Sequence[DistilleryProjected]) -> dict[str, Any]:
        if not rows:
            return {
                "rules": (),
                "gaps": (),
                "conflicts": (),
                "permutations": {},
            }
        self.prepare(rows)
        groups: dict[str, int] = defaultdict(int)
        label_masks: dict[
            tuple[str, tuple[tuple[str, Any], ...]],
            int,
        ] = defaultdict(int)
        permutations: dict[str, Counter[str]] = defaultdict(Counter)
        for index, row in enumerate(self.rows):
            bit = 1 << index
            group = row.pair.source_group
            groups[group] |= bit
            label_masks[(group, row.label)] |= bit
            permutations[group][
                json.dumps(
                    dict(row.label),
                    ensure_ascii=False,
                    sort_keys=True,
                )
            ] += 1
        discovered: list[dict[str, Any]] = []
        for (group, label), target_mask in sorted(
            label_masks.items(),
            key=lambda item: (item[0][0], repr(item[0][1])),
        ):
            for path, coverage in self.scope_candidates(
                groups[group],
                target_mask,
            ):
                discovered.append(
                    {
                        "path": path,
                        "label": label,
                        "coverage": coverage,
                        "discovered_groups": {group},
                    }
                )
        conflicts: list[dict[str, Any]] = []
        consolidated: dict[
            tuple[
                tuple[tuple[str, str, str], ...],
                tuple[tuple[str, Any], ...],
            ],
            dict[str, Any],
        ] = {}
        for candidate in discovered:
            expected = candidate["label"]
            path = self.minimize_path(candidate["path"], expected)
            coverage = self.path_mask(path)
            labels = self.labels_for_mask(coverage)
            if labels != {expected}:
                conflicts.append(
                    {
                        "filter": [
                            {
                                "field": atom.field,
                                "op": atom.operator,
                                "value": _plain_data(atom.value),
                            }
                            for atom in path
                        ],
                        "expected": dict(expected),
                        "observed_outcomes": [
                            dict(label) for label in sorted(labels, key=repr)
                        ],
                        "source_groups": sorted(
                            candidate["discovered_groups"]
                        ),
                    }
                )
                continue
            key = (
                tuple(literal_atom_signature(atom) for atom in path),
                expected,
            )
            existing = consolidated.get(key)
            if existing is None:
                consolidated[key] = {
                    "path": path,
                    "label": expected,
                    "coverage": coverage,
                    "discovered_groups": set(
                        candidate["discovered_groups"]
                    ),
                }
            else:
                existing["coverage"] |= coverage
                existing["discovered_groups"].update(
                    candidate["discovered_groups"]
                )
        candidates = self.merge_in_rules(list(consolidated.values()))
        candidates.sort(
            key=lambda item: (
                -item["coverage"].bit_count(),
                len(item["path"]),
                tuple(
                    literal_atom_signature(atom) for atom in item["path"]
                ),
            )
        )
        retained: list[dict[str, Any]] = []
        for candidate in candidates:
            if any(
                existing["label"] == candidate["label"]
                and candidate["coverage"] & ~existing["coverage"] == 0
                and len(existing["path"]) <= len(candidate["path"])
                for existing in retained
            ):
                continue
            retained.append(candidate)
        rules: list[DistilleryRule] = []
        covered_mask = 0
        for index, candidate in enumerate(retained):
            coverage = candidate["coverage"]
            support_groups = tuple(
                sorted(
                    {
                        self.rows[row_index].pair.source_group
                        for row_index in self.indexes_for_mask(coverage)
                    }
                )
            )
            evidence_ids = tuple(
                self.rows[row_index].pair.pair_id
                for row_index in self.indexes_for_mask(coverage)
            )
            path = tuple(candidate["path"])
            label = candidate["label"]
            signature = literal_logic_signature(path, label)
            rules.append(
                DistilleryRule(
                    rule_id=(
                        "LITERAL-"
                        f"{clean_text(self.profile.get('profile_id')).upper()}-"
                        f"{signature[:12].upper()}"
                    ),
                    priority=100_000 + index,
                    predicates=path,
                    outputs=dict(label),
                    support=coverage.bit_count(),
                    confidence=1.0,
                    source_groups=support_groups,
                    kind=(
                        "reusable" if len(support_groups) >= 2 else "one_date"
                    ),
                    evidence_ids=evidence_ids,
                )
            )
            covered_mask |= coverage
        gaps: list[dict[str, Any]] = []
        for row_index in self.indexes_for_mask(self.all_mask & ~covered_mask):
            row = self.rows[row_index]
            gaps.append(
                {
                    "gap_id": hashlib.sha256(
                        f"{row.pair.pair_id}|literal-gap".encode("utf-8")
                    ).hexdigest()[:24],
                    "pair_id": row.pair.pair_id,
                    "source_group": row.pair.source_group,
                    "before_index": row.pair.before_index,
                    "after_index": row.pair.after_index,
                    "expected_outcome": dict(row.label),
                    "reason": "No globally pure governed-field filter covered this evidence row.",
                    "evidence": {
                        "case": clean_text(row.pair.before.get("Case#")),
                        "business": clean_text(row.pair.before.get("Business")),
                        "type": clean_text(row.pair.before.get("Type")),
                        "description": clean_text(
                            row.pair.before.get("Description")
                        ),
                    },
                }
            )
        return {
            "rules": tuple(sorted(rules, key=lambda item: item.priority)),
            "gaps": tuple(gaps),
            "conflicts": tuple(conflicts),
            "permutations": {
                group: {
                    "unique": len(counts),
                    "counts": dict(counts.most_common()),
                }
                for group, counts in sorted(permutations.items())
            },
        }


def distillery_validate(
    rows: Sequence[DistilleryProjected],
    rules: Sequence[DistilleryRule],
    feature_fields: Sequence[str] | None = None,
) -> dict[str, Any]:
    exact = 0
    matched = 0
    uncovered: list[str] = []
    mismatched: list[str] = []
    by_group: dict[str, Counter[str]] = defaultdict(Counter)
    state_outputs: dict[
        tuple[tuple[str, str], ...],
        set[tuple[tuple[str, Any], ...]],
    ] = defaultdict(set)
    governed = {
        clean_text(field) for field in (feature_fields or []) if clean_text(field)
    }
    for row in rows:
        state = tuple(
            sorted(
                (key, repr(value))
                for key, value in row.features.items()
                if (
                    (not governed and not key.startswith("__"))
                    or key in governed
                )
            )
        )
        state_outputs[state].add(row.label)
        predicted = SingleFileRuleInducer.predict(row.features, rules)
        stats = by_group[row.pair.source_group]
        stats["rows"] += 1
        if predicted is None:
            uncovered.append(row.pair.pair_id)
            stats["uncovered"] += 1
            continue
        matched += 1
        stats["matched"] += 1
        if predicted == dict(row.label):
            exact += 1
            stats["exact"] += 1
        else:
            mismatched.append(row.pair.pair_id)
            stats["mismatched"] += 1
    row_count = len(rows)
    return {
        "row_count": row_count,
        "matched_count": matched,
        "exact_count": exact,
        "accuracy": exact / row_count if row_count else 0.0,
        "contradictions": sum(
            len(outputs) > 1 for outputs in state_outputs.values()
        ),
        "uncovered_pair_ids": uncovered,
        "mismatched_pair_ids": mismatched,
        "by_source_group": {
            group: {
                **dict(stats),
                "accuracy": (
                    stats["exact"] / stats["rows"] if stats["rows"] else 0.0
                ),
            }
            for group, stats in sorted(by_group.items())
        },
    }


def literal_distillery_holdouts(
    rows: Sequence[DistilleryProjected],
    profile: Mapping[str, Any],
) -> dict[str, Any]:
    groups = sorted({row.pair.source_group for row in rows})
    governed = (
        (profile.get("induction") or {}).get("governed_fields") or []
    )
    folds: dict[str, Any] = {}
    for holdout in groups:
        training = [
            row for row in rows if row.pair.source_group != holdout
        ]
        testing = [row for row in rows if row.pair.source_group == holdout]
        mined = LiteralFilterMiner(profile).fit(training)
        rules = mined["rules"]
        validation = distillery_validate(testing, rules, governed)
        folds[holdout] = {
            "training_rows": len(training),
            "testing_rows": len(testing),
            "rule_count": len(rules),
            "training_gaps": len(mined["gaps"]),
            "training_conflicts": len(mined["conflicts"]),
            "accuracy": validation["accuracy"],
            "exact": validation["exact_count"],
            "uncovered": len(validation["uncovered_pair_ids"]),
            "mismatched": len(validation["mismatched_pair_ids"]),
        }
    accuracies = [fold["accuracy"] for fold in folds.values()]
    return {
        "strategy": "leave-one-date-out-literal-filters",
        "folds": folds,
        "mean_accuracy": (
            sum(accuracies) / len(accuracies) if accuracies else 0.0
        ),
        "minimum_accuracy": min(accuracies) if accuracies else 0.0,
        "maximum_accuracy": max(accuracies) if accuracies else 0.0,
    }


def distillery_holdouts(
    rows: Sequence[DistilleryProjected],
    profile: Mapping[str, Any],
) -> dict[str, Any]:
    groups = sorted({row.pair.source_group for row in rows})
    folds: dict[str, Any] = {}
    for holdout in groups:
        training = [
            row for row in rows if row.pair.source_group != holdout
        ]
        testing = [row for row in rows if row.pair.source_group == holdout]
        rules = SingleFileRuleInducer(profile).fit(
            training,
            include_exceptions=False,
        )
        validation = distillery_validate(testing, rules)
        folds[holdout] = {
            "training_rows": len(training),
            "testing_rows": len(testing),
            "rule_count": len(rules),
            "accuracy": validation["accuracy"],
            "exact": validation["exact_count"],
            "uncovered": len(validation["uncovered_pair_ids"]),
            "mismatched": len(validation["mismatched_pair_ids"]),
        }
    accuracies = [fold["accuracy"] for fold in folds.values()]
    return {
        "strategy": "leave-one-source-group-out",
        "folds": folds,
        "mean_accuracy": (
            sum(accuracies) / len(accuracies) if accuracies else 0.0
        ),
        "minimum_accuracy": min(accuracies) if accuracies else 0.0,
        "maximum_accuracy": max(accuracies) if accuracies else 0.0,
    }


def distillery_predicate_json(
    predicates: Sequence[DistilleryAtom],
    profile_id: str,
) -> dict[str, Any]:
    values: list[dict[str, Any]] = [
        {
            "field": "__ruleset_id",
            "op": "eq",
            "value": profile_id,
        }
    ]
    for atom in predicates:
        predicate: dict[str, Any] = {
            "field": atom.field,
            "op": atom.operator,
        }
        if atom.operator not in {
            "blank",
            "not_blank",
            "is_true",
            "is_false",
        }:
            predicate["value"] = atom.value
        values.append(predicate)
    return {"all": values}


def distillery_catalog(
    rules: Sequence[DistilleryRule],
    profile: Mapping[str, Any],
    run_id: str,
    holdout_accuracy: float,
    deployment_eligible: bool,
) -> list[dict[str, Any]]:
    profile_id = clean_text(profile.get("profile_id"))
    output_contract = {
        clean_text(item.get("target")): item
        for item in profile.get("output_fields") or []
    }
    catalog: list[dict[str, Any]] = []
    for rule in sorted(rules, key=lambda item: item.priority):
        evidence_digest = hashlib.sha256(
            "|".join(sorted(rule.evidence_ids)).encode("utf-8")
        ).hexdigest()
        rule_uuid = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"one-engine:{profile_id}:{rule.rule_id}",
            )
        )
        variant_uuid = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"one-engine:{profile_id}:{rule.rule_id}:variant",
            )
        )
        status = "approved" if deployment_eligible else "draft"
        automation = "alpha" if rule.kind == "general" else "evidence"
        source = {
            "kind": "rules_distillery",
            "ruleset_id": profile_id,
            "ruleset_version": clean_text(profile.get("version")),
            "distillation_run_id": run_id,
            "distillery_version": DISTILLERY_VERSION,
            "distilled_rule_kind": rule.kind,
            "support": rule.support,
            "confidence": rule.confidence,
            "holdout_accuracy": (
                holdout_accuracy if rule.kind == "general" else 0.0
            ),
            "source_groups": list(rule.source_groups),
            "evidence_count": len(rule.evidence_ids),
            "evidence_sha256": evidence_digest,
        }
        actions = [
            {
                "type": clean_text(output_contract[target].get("action_type")),
                "value": value,
            }
            for target, value in rule.outputs.items()
            if target in output_contract
        ]
        catalog.append(
            {
                "id": rule_uuid,
                "rule_id": rule.rule_id,
                "name": (
                    f"{profile_id.replace('_', ' ').title()} "
                    f"{rule.kind.title()} Decision"
                ),
                "rule_group": f"Rules Distillery · {profile_id}",
                "business_scope": "Profile-defined",
                "request_types": [],
                "discovery_reference": (
                    "Mechanized BEFORE/AFTER distillation in ONE ENGINE"
                ),
                "notes": (
                    "Generated inside Streamlit. General rules are pure, "
                    "supported filters; evidence rules close deterministic "
                    "historical residuals."
                ),
                "owner_team": "ONE ENGINE",
                "status": status,
                "automation_level": automation,
                "is_bundled": False,
                "ruleset_id": profile_id,
                "distillation_run_id": run_id,
                "updated_at": iso_now(),
                "variants": [
                    {
                        "id": variant_uuid,
                        "rule_id": rule.rule_id,
                        "runtime_rule_id": f"{rule.rule_id}.01",
                        "runtime_kind": "row_rule",
                        "execution_priority": rule.priority,
                        "enabled": deployment_eligible,
                        "is_executable": True,
                        "stop_processing": True,
                        "predicate_json": distillery_predicate_json(
                            rule.predicates,
                            profile_id,
                        ),
                        "action_json": actions,
                        "description": (
                            f"Distilled {rule.kind} rule with support "
                            f"{rule.support:,} and confidence "
                            f"{rule.confidence:.3f}"
                        ),
                        "automation_level": automation,
                        "status": status,
                        "source": source,
                    }
                ],
                "source": source,
            }
        )
    return catalog


def literal_distillery_catalog(
    rules: Sequence[DistilleryRule],
    profile: Mapping[str, Any],
    run_id: str,
    holdout_accuracy: float,
    approved_rule_signatures: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    profile_id = clean_text(profile.get("profile_id"))
    output_contract = {
        clean_text(item.get("target")): item
        for item in profile.get("output_fields") or []
    }
    approvals = {
        clean_text(value).lower()
        for value in (approved_rule_signatures or [])
        if clean_text(value)
    }
    approve_all = "*" in approvals
    catalog: list[dict[str, Any]] = []
    for priority, rule in enumerate(
        sorted(
            rules,
            key=lambda item: (
                0 if item.kind == "reusable" else 1,
                -len(item.source_groups),
                -item.support,
                len(item.predicates),
                item.rule_id,
            ),
        ),
        start=1,
    ):
        signature = literal_logic_signature(rule.predicates, rule.outputs)
        approved = (
            rule.kind == "reusable"
            or approve_all
            or signature.lower() in approvals
        )
        status = "approved" if approved else "ready"
        actions = [
            {
                "type": clean_text(output_contract[target].get("action_type")),
                "value": clean_text(value),
                "explicit_final_state": True,
            }
            for target, value in rule.outputs.items()
            if target in output_contract
        ]
        predicate = distillery_predicate_json(
            rule.predicates,
            profile_id,
        )
        name = literal_filter_name(
            profile_id,
            rule.predicates,
            rule.outputs,
        )
        source = {
            "kind": "rules_distillery",
            "method": "literal_filter_reconstruction",
            "ruleset_id": profile_id,
            "ruleset_version": clean_text(profile.get("version")),
            "distillation_run_id": run_id,
            "distillery_version": DISTILLERY_VERSION,
            "distilled_rule_kind": rule.kind,
            "logic_signature": signature,
            "support": rule.support,
            "confidence": 1.0,
            "support_date_count": len(rule.source_groups),
            "source_groups": list(rule.source_groups),
            "evidence_count": len(rule.evidence_ids),
            "holdout_accuracy": holdout_accuracy,
            "approval_required": rule.kind == "one_date",
            "approved": approved,
            "filter_logic": filter_logic_text(predicate),
            "outcome": _plain_data(rule.outputs),
        }
        rule_uuid = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"one-engine:literal:{profile_id}:{signature}",
            )
        )
        catalog.append(
            {
                "id": rule_uuid,
                "rule_id": rule.rule_id,
                "name": name,
                "rule_group": f"Literal Filters · {profile_id}",
                "business_scope": "Profile-defined",
                "request_types": [],
                "discovery_reference": (
                    "Per-date BEFORE/AFTER literal filter reconstruction"
                ),
                "notes": (
                    "Minimal globally pure governed-field filter. "
                    "No evidence fingerprint or row identity predicate."
                ),
                "owner_team": "ONE ENGINE",
                "status": status,
                "automation_level": (
                    "alpha" if rule.kind == "reusable" else "reviewed"
                ),
                "is_bundled": False,
                "ruleset_id": profile_id,
                "distillation_run_id": run_id,
                "updated_at": iso_now(),
                "variants": [
                    {
                        "id": str(
                            uuid.uuid5(
                                uuid.NAMESPACE_URL,
                                f"one-engine:literal:{profile_id}:{signature}:variant",
                            )
                        ),
                        "rule_id": rule.rule_id,
                        "runtime_rule_id": f"{rule.rule_id}.01",
                        "runtime_kind": "row_rule",
                        "execution_priority": 100_000 + priority,
                        "enabled": approved,
                        "is_executable": True,
                        "stop_processing": True,
                        "predicate_json": predicate,
                        "action_json": actions,
                        "description": name,
                        "automation_level": (
                            "alpha" if rule.kind == "reusable" else "reviewed"
                        ),
                        "status": status,
                        "source": source,
                    }
                ],
                "source": source,
            }
        )
    return catalog


def candidate_catalog_for_test(
    catalog: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Enable an immutable candidate in memory without changing active rules."""
    output = deepcopy([dict(rule) for rule in catalog])
    for rule in output:
        rule["status"] = "approved"
        for variant in rule.get("variants") or []:
            if isinstance(variant, MutableMapping):
                variant["enabled"] = True
                variant["status"] = "approved"
    return output


def run_rules_distillery(
    *,
    profile_id: str,
    before_file_name: str,
    before_bytes: bytes,
    after_file_name: str,
    after_bytes: bytes,
    run_holdouts: bool = True,
    outcome_aliases: Mapping[str, Mapping[str, Any]] | None = None,
    approved_alias_keys: Sequence[str] | None = None,
    approved_rule_signatures: Sequence[str] | None = None,
) -> dict[str, Any]:
    profile = distillery_profile(profile_id)
    before_documents = distillery_documents_from_upload(
        before_file_name,
        before_bytes,
        profile,
    )
    after_documents = distillery_documents_from_upload(
        after_file_name,
        after_bytes,
        profile,
    )
    alias_registry = distillery_outcome_alias_registry(
        after_documents,
        profile,
        outcome_aliases,
        approved_alias_keys,
    )
    pairs, unmatched = distillery_match_documents(
        before_documents,
        after_documents,
        profile,
        outcome_aliases,
    )
    projected = distillery_project_pairs(pairs, profile)
    mined = LiteralFilterMiner(profile).fit(projected)
    rules = mined["rules"]
    governed = (
        (profile.get("induction") or {}).get("governed_fields") or []
    )
    validation = distillery_validate(projected, rules, governed)
    holdout = (
        literal_distillery_holdouts(projected, profile)
        if run_holdouts
        else {
            "strategy": "not-run",
            "folds": {},
            "mean_accuracy": 0.0,
            "minimum_accuracy": 0.0,
            "maximum_accuracy": 0.0,
        }
    )
    approvals = {
        clean_text(value).lower()
        for value in (approved_rule_signatures or [])
        if clean_text(value)
    }
    approve_all_rules = "*" in approvals
    pending_one_date = [
        {
            "logic_signature": literal_logic_signature(
                rule.predicates,
                rule.outputs,
            ),
            "rule_id": rule.rule_id,
            "name": literal_filter_name(
                profile_id,
                rule.predicates,
                rule.outputs,
            ),
            "support_rows": rule.support,
            "source_groups": list(rule.source_groups),
        }
        for rule in rules
        if rule.kind == "one_date"
        and not approve_all_rules
        and literal_logic_signature(
            rule.predicates,
            rule.outputs,
        ).lower()
        not in approvals
    ]
    gaps = list(mined["gaps"])
    conflicts = list(mined["conflicts"])
    deployment_eligible = (
        validation["accuracy"] == 1.0
        and validation["contradictions"] == 0
        and not unmatched
        and not gaps
        and not conflicts
        and not pending_one_date
        and int(alias_registry.get("review_required") or 0) == 0
    )
    before_hash = hashlib.sha256(before_bytes).hexdigest()
    after_hash = hashlib.sha256(after_bytes).hexdigest()
    alias_hash = hashlib.sha256(
        json_dumps(_plain_data(outcome_aliases or {})).encode("utf-8")
    ).hexdigest()
    approval_hash = hashlib.sha256(
        "|".join(sorted(approvals)).encode("utf-8")
    ).hexdigest()
    run_id = hashlib.sha256(
        "|".join(
            [
                DISTILLERY_VERSION,
                profile_id,
                clean_text(profile.get("version")),
                before_hash,
                after_hash,
                alias_hash,
                approval_hash,
                "holdouts" if run_holdouts else "draft",
            ]
        ).encode("utf-8")
    ).hexdigest()[:20]
    methods = Counter(pair.method for pair in pairs)
    changed_fields = Counter(
        field for pair in pairs for field in pair.changed_input_fields
    )
    labels = Counter(
        json.dumps(dict(row.label), sort_keys=True, ensure_ascii=False)
        for row in projected
    )
    raw_permutations_by_date: dict[str, Any] = {}
    canonical_permutations_by_date: dict[str, Any] = {}
    contracts = profile.get("output_fields") or []
    for document in after_documents:
        raw_counts: Counter[str] = Counter()
        canonical_counts: Counter[str] = Counter()
        for row in document.get("rows") or []:
            raw_outcome = {
                clean_text(contract.get("target")): clean_text(
                    row.get(clean_text(contract.get("source")))
                )
                for contract in contracts
            }
            canonical_outcome = {
                target: distillery_outcome_value(
                    target,
                    value,
                    outcome_aliases,
                )
                for target, value in raw_outcome.items()
            }
            raw_counts[
                json.dumps(
                    raw_outcome,
                    sort_keys=True,
                    ensure_ascii=False,
                )
            ] += 1
            canonical_counts[
                json.dumps(
                    canonical_outcome,
                    sort_keys=True,
                    ensure_ascii=False,
                )
            ] += 1
        source_group = clean_text(document.get("source_group"))
        raw_permutations_by_date[source_group] = {
            "unique": len(raw_counts),
            "counts": dict(raw_counts.most_common()),
        }
        canonical_permutations_by_date[source_group] = {
            "unique": len(canonical_counts),
            "counts": dict(canonical_counts.most_common()),
        }
    report = {
        "run_id": run_id,
        "profile_id": profile_id,
        "profile_version": clean_text(profile.get("version")),
        "distillery_version": DISTILLERY_VERSION,
        "created_at": iso_now(),
        "source": {
            "before": {
                "file_name": before_file_name,
                "sha256": before_hash,
                "documents": len(before_documents),
                "rows": sum(
                    int(document["row_count"])
                    for document in before_documents
                ),
            },
            "after": {
                "file_name": after_file_name,
                "sha256": after_hash,
                "documents": len(after_documents),
                "rows": sum(
                    int(document["row_count"])
                    for document in after_documents
                ),
            },
        },
        "matching": {
            "pairs": len(pairs),
            "unmatched": len(unmatched),
            "ambiguous": sum(pair.ambiguous for pair in pairs),
            "methods": dict(methods),
            "changed_input_fields": dict(changed_fields.most_common()),
        },
        "labels": {
            "unique": len(labels),
            "counts": dict(labels.most_common()),
            "fields": [
                "ACTION",
                "If In Stock: Action",
                "Audit Action",
            ],
            "alias_registry": alias_registry,
            "raw_permutations_by_date": raw_permutations_by_date,
            "canonical_permutations_by_date": (
                canonical_permutations_by_date
            ),
            "permutations_by_date": mined["permutations"],
        },
        "rules": {
            "total": len(rules),
            "reusable": sum(rule.kind == "reusable" for rule in rules),
            "one_date": sum(rule.kind == "one_date" for rule in rules),
            "pending_approval": len(pending_one_date),
            "reusable_support": sum(
                rule.support for rule in rules if rule.kind == "reusable"
            ),
            "one_date_support": sum(
                rule.support for rule in rules if rule.kind == "one_date"
            ),
            "forbidden_runtime_predicates": 0,
        },
        "pending_rule_approvals": pending_one_date,
        "gaps": {
            "count": len(gaps),
            "records": gaps,
        },
        "conflicts": {
            "count": len(conflicts),
            "records": conflicts,
        },
        "validation": validation,
        "holdout": holdout,
        "deployment_gate": {
            "eligible": deployment_eligible,
            "requirements": {
                "corpus_accuracy": 1.0,
                "unmatched_rows": 0,
                "contradictions": 0,
                "gaps": 0,
                "filter_conflicts": 0,
                "pending_rule_approvals": 0,
                "pending_alias_reviews": 0,
                "forbidden_runtime_predicates": 0,
            },
            "observed": {
                "corpus_accuracy": validation["accuracy"],
                "unmatched_rows": len(unmatched),
                "contradictions": validation["contradictions"],
                "gaps": len(gaps),
                "filter_conflicts": len(conflicts),
                "pending_rule_approvals": len(pending_one_date),
                "pending_alias_reviews": int(
                    alias_registry.get("review_required") or 0
                ),
                "forbidden_runtime_predicates": 0,
            },
        },
    }
    catalog = literal_distillery_catalog(
        rules,
        profile,
        run_id,
        float(holdout.get("mean_accuracy") or 0.0),
        approved_rule_signatures,
    )
    return {
        "run_id": run_id,
        "profile_id": profile_id,
        "catalog": catalog,
        "report": report,
        "gaps": gaps,
        "conflicts": conflicts,
        "alias_registry": alias_registry,
        "deployment_eligible": deployment_eligible,
    }



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
        "Audit Action": clean_text(row.get("audit_action")),
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
        "Final Audit Action": clean_text(row.get("audit_action")),
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
            "LIVE_PRODUCT_REQUEST_VIEW",
            "load_live_product_request_data",
            "Use Live Product Request Data",
            "run_rules_distillery",
            "promote_distilled_catalog",
            "oneengine_brand.png",
            "render_actionable_exception",
        ]
        missing = [marker for marker in required_markers if marker not in source_text]
        quote = chr(34)
        forbidden = [
            "st.session_state[" + quote + "_parsed_upload" + quote + "] = " + "parse_source_workbook",
            "isinstance(" + "parsed, " + "ParsedWorkbook)",
            "st.session_state[" + quote + "_last_execution_result" + quote + "] = " + "result",
            "st.session_state[" + quote + "_workbench_run_result" + quote + "] = " + "result",
            "from " + "one_engine",
            "import " + "one_engine",
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


def snowflake_source_row_dict(row: Any) -> dict[str, Any]:
    """Preserve source column names while converting a Snowpark result row."""
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
    return {
        clean_text(key): _plain_data(value)
        for key, value in raw.items()
        if clean_text(key)
    }


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

    def live_product_request_view(self) -> str:
        view = self._identifier(
            LIVE_PRODUCT_REQUEST_VIEW,
            "LIVE_PRODUCT_REQUEST_VIEW",
        )
        return f"{self.schema_name}.{self._quoted(view)}"

    def load_live_product_request_data(
        self,
    ) -> tuple[ParsedWorkbook, str, dict[str, Any]]:
        """Snapshot the live Product Request view into the workbook contract."""
        qualified_view = self.live_product_request_view()
        source_rows = self.collect(f"SELECT * FROM {qualified_view}")
        records: list[dict[str, Any]] = []
        discovered_columns: list[str] = []
        for source_row in source_rows:
            raw = snowflake_source_row_dict(source_row)
            canonical = collapse_raw_row(raw)
            if not any(clean_text(value) for value in canonical.values()):
                continue
            records.append(canonical)
            for column in canonical:
                if column not in discovered_columns:
                    discovered_columns.append(column)
        if not records:
            raise ValueError(
                f"{qualified_view} returned no non-empty Product Request rows."
            )
        serialized_rows = [
            json.dumps(
                _plain_data(record),
                default=json_default,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            for record in records
        ]
        ordered_records = [
            record
            for _, record in sorted(
                zip(serialized_rows, records),
                key=lambda item: item[0],
            )
        ]
        ordered_columns = [
            column for column in EXPECTED_HEADERS if column in discovered_columns
        ] + [
            column for column in discovered_columns if column not in EXPECTED_HEADERS
        ]
        display_view = (
            f"{self.database}.{self.schema}.{LIVE_PRODUCT_REQUEST_VIEW}"
        )
        content = "\n".join(
            [
                display_view,
                *sorted(serialized_rows),
            ]
        ).encode("utf-8")
        source_hash = hashlib.sha256(content).hexdigest()
        parsed = ParsedWorkbook(
            file_name=LIVE_PRODUCT_REQUEST_VIEW,
            sheet_name="Snowflake live view",
            columns=ordered_columns,
            rows=ordered_records,
            warnings=[],
            source_row_numbers=list(range(1, len(ordered_records) + 1)),
        )
        metadata = {
            "source_type": "snowflake_view",
            "source_view": display_view,
            "qualified_source_view": qualified_view,
            "snapshot_sha256": source_hash,
            "snapshot_row_count": len(ordered_records),
            "snapshot_at": iso_now(),
        }
        return parsed, source_hash, metadata

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

    def promote_distilled_catalog(
        self,
        catalog: Sequence[Mapping[str, Any]],
        report: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Deprecated direct promotion path retained only for API compatibility."""
        raise RuntimeError(
            "Direct Distillery promotion is disabled. Save an immutable "
            "catalog candidate and activate its catalog_version_id instead."
        )
        # Unreachable compatibility implementation below is intentionally
        # retained for old serialized sessions and will be removed after the
        # versioned-catalog migration window.
        if not catalog:
            raise ValueError("The distilled catalog is empty.")
        deployment_gate = report.get("deployment_gate") or {}
        if not bool_value(deployment_gate.get("eligible")):
            raise ValueError(
                "The Distillery deployment gate did not pass. "
                "Resolve unmatched rows, contradictions, or parity failures."
            )
        profile_id = clean_text(report.get("profile_id"))
        run_id = clean_text(report.get("run_id"))
        if not profile_id or not run_id:
            raise ValueError("Distillery profile and run identifiers are required.")
        current_ids = {
            clean_text(rule.get("rule_id")).upper()
            for rule in catalog
            if clean_text(rule.get("rule_id"))
        }
        existing = self.load_rules()
        retired: list[dict[str, Any]] = []
        for existing_rule in existing:
            source = existing_rule.get("source") or {}
            if (
                clean_text(source.get("kind")) != "rules_distillery"
                or clean_text(source.get("ruleset_id")) != profile_id
                or clean_text(existing_rule.get("rule_id")).upper() in current_ids
            ):
                continue
            retired_rule = deepcopy(existing_rule)
            retired_rule["status"] = "retired"
            retired_rule["updated_at"] = iso_now()
            for variant in retired_rule.get("variants") or []:
                if isinstance(variant, MutableMapping):
                    variant["enabled"] = False
                    variant["status"] = "retired"
            retired.append(retired_rule)
        with self.transaction():
            if retired:
                self.upsert_rules(retired)
            self.upsert_rules(catalog)
            self.log_event(
                entity_type="rules_distillery",
                entity_id=run_id,
                action="promote_distilled_catalog",
                after={
                    "run_id": run_id,
                    "profile_id": profile_id,
                    "rule_count": len(catalog),
                    "retired_rule_count": len(retired),
                    "deployment_eligible": True,
                },
                details=report,
            )
        return {
            "run_id": run_id,
            "profile_id": profile_id,
            "promoted_rule_count": len(catalog),
            "retired_rule_count": len(retired),
        }

    def load_outcome_aliases(
        self,
        workflow_id: str,
    ) -> dict[str, dict[str, str]]:
        query = f"""
            SELECT
                FIELD_NAME,
                RAW_VALUE_KEY,
                CANONICAL_VALUE
            FROM {self.table('outcome_aliases')}
            WHERE WORKFLOW_ID = ?
              AND STATUS = 'APPROVED'
            ORDER BY FIELD_NAME, RAW_VALUE_KEY
        """
        output: dict[str, dict[str, str]] = defaultdict(dict)
        for row in self.collect(query, [clean_text(workflow_id)]):
            data = snowflake_row_dict(row)
            output[clean_text(data.get("field_name"))][
                clean_text(data.get("raw_value_key"))
            ] = clean_text(data.get("canonical_value"))
        return dict(output)

    def save_outcome_aliases(
        self,
        workflow_id: str,
        entries: Sequence[Mapping[str, Any]],
    ) -> int:
        values = [
            {
                "workflow_id": clean_text(workflow_id),
                "field_name": clean_text(entry.get("field_name")),
                "raw_value_key": normalize_key(entry.get("raw_value")),
                "raw_value": clean_text(entry.get("raw_value")),
                "canonical_value": clean_text(entry.get("canonical_value")),
                "status": "APPROVED",
                "updated_by": self.current_user(),
                "updated_at": iso_now(),
            }
            for entry in entries
            if clean_text(entry.get("field_name"))
        ]
        if not values:
            return 0
        query = f"""
            MERGE INTO {self.table('outcome_aliases')} AS target
            USING (
                SELECT
                    value:"workflow_id"::VARCHAR AS WORKFLOW_ID,
                    value:"field_name"::VARCHAR AS FIELD_NAME,
                    value:"raw_value_key"::VARCHAR AS RAW_VALUE_KEY,
                    value:"raw_value"::VARCHAR AS RAW_VALUE,
                    value:"canonical_value"::VARCHAR AS CANONICAL_VALUE,
                    value:"status"::VARCHAR AS STATUS,
                    value:"updated_by"::VARCHAR AS UPDATED_BY,
                    TRY_TO_TIMESTAMP_TZ(value:"updated_at"::VARCHAR) AS UPDATED_AT
                FROM TABLE(FLATTEN(INPUT => PARSE_JSON(?)))
            ) AS source
            ON target.WORKFLOW_ID = source.WORKFLOW_ID
               AND target.FIELD_NAME = source.FIELD_NAME
               AND target.RAW_VALUE_KEY = source.RAW_VALUE_KEY
            WHEN MATCHED THEN UPDATE SET
                RAW_VALUE = source.RAW_VALUE,
                CANONICAL_VALUE = source.CANONICAL_VALUE,
                STATUS = source.STATUS,
                UPDATED_BY = source.UPDATED_BY,
                UPDATED_AT = source.UPDATED_AT
            WHEN NOT MATCHED THEN INSERT (
                WORKFLOW_ID, FIELD_NAME, RAW_VALUE_KEY, RAW_VALUE,
                CANONICAL_VALUE, STATUS, UPDATED_BY, UPDATED_AT
            ) VALUES (
                source.WORKFLOW_ID, source.FIELD_NAME, source.RAW_VALUE_KEY,
                source.RAW_VALUE, source.CANONICAL_VALUE, source.STATUS,
                source.UPDATED_BY, source.UPDATED_AT
            )
        """
        with self.transaction():
            self.execute(query, [json_dumps(values)])
            self.log_event(
                entity_type="outcome_aliases",
                entity_id=clean_text(workflow_id),
                action="save_outcome_aliases",
                details={"workflow_id": workflow_id, "entry_count": len(values)},
            )
        return len(values)

    def _catalog_version_from_row(self, row: Any) -> dict[str, Any]:
        data = snowflake_row_dict(row)
        payload = normalize_persisted_json(data.get("version_json"), {})
        output = dict(payload) if isinstance(payload, Mapping) else {}
        output.update(
            {
                "id": clean_text(data.get("id") or output.get("id")),
                "workflow_id": clean_text(
                    data.get("workflow_id") or output.get("workflow_id")
                ),
                "version_number": int(
                    data.get("version_number")
                    or output.get("version_number")
                    or 0
                ),
                "status": clean_text(data.get("status") or output.get("status")),
                "distillery_run_id": clean_text(
                    data.get("distillery_run_id")
                    or output.get("distillery_run_id")
                ),
                "parent_version_id": clean_text(
                    data.get("parent_version_id")
                    or output.get("parent_version_id")
                ),
                "created_by": clean_text(
                    data.get("created_by") or output.get("created_by")
                ),
                "created_at": timestamp_text(
                    data.get("created_at") or output.get("created_at")
                ),
                "activated_by": clean_text(
                    data.get("activated_by") or output.get("activated_by")
                ),
                "activated_at": timestamp_text(
                    data.get("activated_at") or output.get("activated_at")
                ),
            }
        )
        return output

    def list_catalog_versions(
        self,
        workflow_id: str,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        safe_limit = max(1, min(int(limit), 1000))
        query = f"""
            SELECT
                ID, WORKFLOW_ID, VERSION_NUMBER, STATUS,
                DISTILLERY_RUN_ID, PARENT_VERSION_ID,
                CREATED_BY, CREATED_AT, ACTIVATED_BY, ACTIVATED_AT,
                TO_JSON(VERSION_JSON) AS VERSION_JSON
            FROM {self.table('catalog_versions')}
            WHERE WORKFLOW_ID = ?
            ORDER BY VERSION_NUMBER DESC
            LIMIT {safe_limit}
        """
        return [
            self._catalog_version_from_row(row)
            for row in self.collect(query, [clean_text(workflow_id)])
        ]

    def get_catalog_version(
        self,
        version_id: str,
    ) -> dict[str, Any] | None:
        query = f"""
            SELECT
                ID, WORKFLOW_ID, VERSION_NUMBER, STATUS,
                DISTILLERY_RUN_ID, PARENT_VERSION_ID,
                CREATED_BY, CREATED_AT, ACTIVATED_BY, ACTIVATED_AT,
                TO_JSON(VERSION_JSON) AS VERSION_JSON
            FROM {self.table('catalog_versions')}
            WHERE ID = ?
            LIMIT 1
        """
        rows = self.collect(query, [clean_text(version_id)])
        return self._catalog_version_from_row(rows[0]) if rows else None

    def upsert_catalog_version_rules(
        self,
        version_id: str,
        workflow_id: str,
        rules: Sequence[Mapping[str, Any]],
    ) -> None:
        if not rules:
            return
        values = [
            {
                "id": f"{version_id}:{clean_text(rule.get('rule_id'))}",
                "catalog_version_id": version_id,
                "workflow_id": workflow_id,
                "rule_id": clean_text(rule.get("rule_id")),
                "rule_json": _plain_data(rule),
                "created_at": iso_now(),
            }
            for rule in rules
        ]
        for batch in chunked(values):
            query = f"""
                INSERT INTO {self.table('catalog_rules')} (
                    ID, CATALOG_VERSION_ID, WORKFLOW_ID, RULE_ID,
                    RULE_JSON, CREATED_AT
                )
                SELECT
                    value:"id"::VARCHAR,
                    value:"catalog_version_id"::VARCHAR,
                    value:"workflow_id"::VARCHAR,
                    value:"rule_id"::VARCHAR,
                    value:"rule_json",
                    TRY_TO_TIMESTAMP_TZ(value:"created_at"::VARCHAR)
                FROM TABLE(FLATTEN(INPUT => PARSE_JSON(?)))
            """
            self.execute(query, [json_dumps(batch)])

    def load_catalog_version_rules(
        self,
        version_id: str,
    ) -> list[dict[str, Any]]:
        query = f"""
            SELECT TO_JSON(RULE_JSON) AS RULE_JSON
            FROM {self.table('catalog_rules')}
            WHERE CATALOG_VERSION_ID = ?
            ORDER BY RULE_ID
        """
        output: list[dict[str, Any]] = []
        for row in self.collect(query, [clean_text(version_id)]):
            rule = normalize_persisted_json(
                snowflake_row_dict(row).get("rule_json"),
                {},
            )
            if isinstance(rule, dict) and rule:
                output.append(rule)
        return output

    def save_catalog_candidate(
        self,
        catalog: Sequence[Mapping[str, Any]],
        report: Mapping[str, Any],
        gaps: Sequence[Mapping[str, Any]] | None = None,
    ) -> dict[str, Any]:
        workflow_id = clean_text(report.get("profile_id"))
        run_id = clean_text(report.get("run_id"))
        if not workflow_id or not run_id:
            raise ValueError("Candidate workflow and Distillery run ID are required.")
        # Preserve the pre-versioning workflow catalog as the first rollback
        # point before attaching a candidate to its active parent.
        self.bootstrap_legacy_catalog(workflow_id)
        existing = self.scalar(
            f"""
                SELECT ID AS VALUE
                FROM {self.table('catalog_versions')}
                WHERE WORKFLOW_ID = ? AND DISTILLERY_RUN_ID = ?
                LIMIT 1
            """,
            [workflow_id, run_id],
            "",
        )
        if clean_text(existing):
            version = self.get_catalog_version(clean_text(existing))
            if version is None:
                raise RuntimeError("The existing candidate version could not be loaded.")
            return version
        version_number = int(
            self.scalar(
                f"""
                    SELECT COALESCE(MAX(VERSION_NUMBER), 0) + 1 AS VALUE
                    FROM {self.table('catalog_versions')}
                    WHERE WORKFLOW_ID = ?
                """,
                [workflow_id],
                1,
            )
            or 1
        )
        active = next(
            (
                item
                for item in self.list_catalog_versions(workflow_id)
                if clean_text(item.get("status")).upper() == "ACTIVE"
            ),
            None,
        )
        version_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"one-engine:catalog-version:{workflow_id}:{run_id}",
            )
        )
        timestamp = iso_now()
        version = {
            "id": version_id,
            "workflow_id": workflow_id,
            "version_number": version_number,
            "status": "CANDIDATE",
            "distillery_run_id": run_id,
            "parent_version_id": clean_text(
                (active or {}).get("id")
            ),
            "rule_count": len(catalog),
            "gap_count": len(gaps or []),
            "deployment_eligible": bool_value(
                (report.get("deployment_gate") or {}).get("eligible")
            ),
            "report": _plain_data(report),
            "created_by": self.current_user(),
            "created_at": timestamp,
            "activated_by": "",
            "activated_at": "",
        }
        insert = f"""
            INSERT INTO {self.table('catalog_versions')} (
                ID, WORKFLOW_ID, VERSION_NUMBER, STATUS,
                DISTILLERY_RUN_ID, PARENT_VERSION_ID,
                CREATED_BY, CREATED_AT, ACTIVATED_BY, ACTIVATED_AT,
                VERSION_JSON
            )
            SELECT
                ?, ?, ?, 'CANDIDATE', ?, NULLIF(?, ''),
                ?, TRY_TO_TIMESTAMP_TZ(?), NULL, NULL, PARSE_JSON(?)
        """
        gap_values = [
            {
                "id": clean_text(gap.get("gap_id")) or new_id(),
                "catalog_version_id": version_id,
                "workflow_id": workflow_id,
                "source_group": clean_text(gap.get("source_group")),
                "pair_id": clean_text(gap.get("pair_id")),
                "status": "OPEN",
                "resolution": "",
                "gap_json": _plain_data(gap),
                "created_at": timestamp,
                "updated_at": timestamp,
            }
            for gap in (gaps or [])
        ]
        with self.transaction():
            self.execute(
                insert,
                [
                    version_id,
                    workflow_id,
                    version_number,
                    run_id,
                    version["parent_version_id"],
                    version["created_by"],
                    timestamp,
                    json_dumps(version),
                ],
            )
            self.upsert_catalog_version_rules(
                version_id,
                workflow_id,
                catalog,
            )
            if gap_values:
                self.execute(
                    f"""
                        INSERT INTO {self.table('distillery_gaps')} (
                            ID, CATALOG_VERSION_ID, WORKFLOW_ID, SOURCE_GROUP,
                            PAIR_ID, STATUS, RESOLUTION, GAP_JSON,
                            CREATED_AT, UPDATED_AT
                        )
                        SELECT
                            value:"id"::VARCHAR,
                            value:"catalog_version_id"::VARCHAR,
                            value:"workflow_id"::VARCHAR,
                            value:"source_group"::VARCHAR,
                            value:"pair_id"::VARCHAR,
                            value:"status"::VARCHAR,
                            value:"resolution"::VARCHAR,
                            value:"gap_json",
                            TRY_TO_TIMESTAMP_TZ(value:"created_at"::VARCHAR),
                            TRY_TO_TIMESTAMP_TZ(value:"updated_at"::VARCHAR)
                        FROM TABLE(FLATTEN(INPUT => PARSE_JSON(?)))
                    """,
                    [json_dumps(gap_values)],
                )
            self.log_event(
                entity_type="catalog_version",
                entity_id=version_id,
                action="save_catalog_candidate",
                after=version,
                details={
                    "workflow_id": workflow_id,
                    "rule_count": len(catalog),
                    "gap_count": len(gap_values),
                },
            )
        return version

    def bootstrap_legacy_catalog(
        self,
        workflow_id: str,
    ) -> dict[str, Any] | None:
        versions = self.list_catalog_versions(workflow_id)
        active = next(
            (
                item
                for item in versions
                if clean_text(item.get("status")).upper() == "ACTIVE"
            ),
            None,
        )
        if active is not None:
            return active
        rules = [
            rule
            for rule in self.load_rules()
            if rule_workflow_id(rule) == workflow_id
        ]
        if not rules:
            return None
        version_number = max(
            [int(item.get("version_number") or 0) for item in versions] or [0]
        ) + 1
        version_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"one-engine:legacy-catalog:{workflow_id}:{catalog_snapshot(rules).get('sha256')}",
            )
        )
        timestamp = iso_now()
        version = {
            "id": version_id,
            "workflow_id": workflow_id,
            "version_number": version_number,
            "status": "ACTIVE",
            "distillery_run_id": "legacy-bootstrap",
            "parent_version_id": "",
            "rule_count": len(rules),
            "gap_count": 0,
            "deployment_eligible": True,
            "report": {
                "method": "legacy_catalog_bootstrap",
                "snapshot": catalog_snapshot(rules),
            },
            "created_by": self.current_user(),
            "created_at": timestamp,
            "activated_by": self.current_user(),
            "activated_at": timestamp,
        }
        with self.transaction():
            self.execute(
                f"""
                    INSERT INTO {self.table('catalog_versions')} (
                        ID, WORKFLOW_ID, VERSION_NUMBER, STATUS,
                        DISTILLERY_RUN_ID, PARENT_VERSION_ID,
                        CREATED_BY, CREATED_AT, ACTIVATED_BY, ACTIVATED_AT,
                        VERSION_JSON
                    )
                    SELECT
                        ?, ?, ?, 'ACTIVE', 'legacy-bootstrap', NULL,
                        ?, TRY_TO_TIMESTAMP_TZ(?), ?,
                        TRY_TO_TIMESTAMP_TZ(?), PARSE_JSON(?)
                """,
                [
                    version_id,
                    workflow_id,
                    version_number,
                    version["created_by"],
                    timestamp,
                    version["activated_by"],
                    timestamp,
                    json_dumps(version),
                ],
            )
            self.upsert_catalog_version_rules(
                version_id,
                workflow_id,
                rules,
            )
            self.log_event(
                entity_type="catalog_version",
                entity_id=version_id,
                action="bootstrap_legacy_catalog",
                after=version,
                details={"workflow_id": workflow_id, "rule_count": len(rules)},
            )
        return version

    def activate_catalog_version(
        self,
        version_id: str,
    ) -> dict[str, Any]:
        target = self.get_catalog_version(version_id)
        if target is None:
            raise ValueError("The selected catalog version does not exist.")
        workflow_id = clean_text(target.get("workflow_id"))
        status = clean_text(target.get("status")).upper()
        if status == "CANDIDATE" and not bool_value(
            target.get("deployment_eligible")
        ):
            raise ValueError(
                "The candidate has not passed the literal-filter deployment gate."
            )
        if status not in {"CANDIDATE", "RETIRED", "ACTIVE"}:
            raise ValueError(f"Catalog version status {status!r} cannot be activated.")
        open_gaps = int(
            self.scalar(
                f"""
                    SELECT COUNT(*) AS VALUE
                    FROM {self.table('distillery_gaps')}
                    WHERE CATALOG_VERSION_ID = ?
                      AND STATUS <> 'RESOLVED'
                """,
                [version_id],
                0,
            )
            or 0
        )
        if open_gaps:
            raise ValueError(
                f"The selected catalog version has {open_gaps:,} unresolved "
                "Distillery gap(s). Rebuild a complete candidate before activation."
            )
        self.bootstrap_legacy_catalog(workflow_id)
        rules = self.load_catalog_version_rules(version_id)
        if not rules:
            raise ValueError("The selected catalog version contains no rules.")
        timestamp = iso_now()
        user = self.current_user()
        with self.transaction():
            self.execute(
                f"""
                    DELETE FROM {self.table('rules')}
                    WHERE COALESCE(
                        NULLIF(RULE_JSON:"ruleset_id"::VARCHAR, ''),
                        NULLIF(RULE_JSON:"source":"ruleset_id"::VARCHAR, ''),
                        'product_request'
                    ) = ?
                """,
                [workflow_id],
            )
            self.upsert_rules(rules)
            self.execute(
                f"""
                    UPDATE {self.table('catalog_versions')}
                    SET STATUS = 'RETIRED'
                    WHERE WORKFLOW_ID = ?
                      AND STATUS = 'ACTIVE'
                      AND ID <> ?
                """,
                [workflow_id, version_id],
            )
            self.execute(
                f"""
                    UPDATE {self.table('catalog_versions')}
                    SET
                        STATUS = 'ACTIVE',
                        ACTIVATED_BY = ?,
                        ACTIVATED_AT = TRY_TO_TIMESTAMP_TZ(?),
                        VERSION_JSON = OBJECT_INSERT(
                            OBJECT_INSERT(VERSION_JSON, 'status', 'ACTIVE', TRUE),
                            'activated_at', ?, TRUE
                        )
                    WHERE ID = ?
                """,
                [user, timestamp, timestamp, version_id],
            )
            self.log_event(
                entity_type="catalog_version",
                entity_id=version_id,
                action=(
                    "rollback_catalog_version"
                    if status == "RETIRED"
                    else "activate_catalog_version"
                ),
                after={
                    **target,
                    "status": "ACTIVE",
                    "activated_by": user,
                    "activated_at": timestamp,
                },
                details={
                    "workflow_id": workflow_id,
                    "rule_count": len(rules),
                    "previous_status": status,
                },
            )
        activated = self.get_catalog_version(version_id)
        if activated is None:
            raise RuntimeError("Activated catalog version could not be reloaded.")
        return activated

    def list_distillery_gaps(
        self,
        version_id: str,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        safe_limit = max(1, min(int(limit), 10000))
        query = f"""
            SELECT
                ID, CATALOG_VERSION_ID, WORKFLOW_ID, SOURCE_GROUP,
                PAIR_ID, STATUS, RESOLUTION,
                TO_JSON(GAP_JSON) AS GAP_JSON,
                CREATED_AT, UPDATED_AT
            FROM {self.table('distillery_gaps')}
            WHERE CATALOG_VERSION_ID = ?
            ORDER BY SOURCE_GROUP, PAIR_ID
            LIMIT {safe_limit}
        """
        output: list[dict[str, Any]] = []
        for row in self.collect(query, [clean_text(version_id)]):
            data = snowflake_row_dict(row)
            gap = normalize_persisted_json(data.get("gap_json"), {})
            item = dict(gap) if isinstance(gap, Mapping) else {}
            item.update(
                {
                    "id": clean_text(data.get("id")),
                    "catalog_version_id": clean_text(
                        data.get("catalog_version_id")
                    ),
                    "workflow_id": clean_text(data.get("workflow_id")),
                    "source_group": clean_text(data.get("source_group")),
                    "pair_id": clean_text(data.get("pair_id")),
                    "status": clean_text(data.get("status")),
                    "resolution": clean_text(data.get("resolution")),
                    "created_at": timestamp_text(data.get("created_at")),
                    "updated_at": timestamp_text(data.get("updated_at")),
                }
            )
            output.append(item)
        return output

    def resolve_distillery_gap(
        self,
        gap_id: str,
        resolution: str,
    ) -> dict[str, Any]:
        gap_key = clean_text(gap_id)
        resolution_text = clean_text(resolution)
        if not gap_key:
            raise ValueError("A Distillery gap ID is required.")
        if not resolution_text:
            raise ValueError("A gap resolution note is required.")
        rows = self.collect(
            f"""
                SELECT
                    ID, CATALOG_VERSION_ID, WORKFLOW_ID, SOURCE_GROUP,
                    PAIR_ID, STATUS, RESOLUTION,
                    TO_JSON(GAP_JSON) AS GAP_JSON,
                    CREATED_AT, UPDATED_AT
                FROM {self.table('distillery_gaps')}
                WHERE ID = ?
                LIMIT 1
            """,
            [gap_key],
        )
        if not rows:
            raise ValueError("The selected Distillery gap no longer exists.")
        before_data = snowflake_row_dict(rows[0])
        with self.transaction():
            self.execute(
                f"""
                    UPDATE {self.table('distillery_gaps')}
                    SET
                        STATUS = 'RESOLVED',
                        RESOLUTION = ?,
                        UPDATED_AT = CURRENT_TIMESTAMP()
                    WHERE ID = ?
                """,
                [resolution_text, gap_key],
            )
            self.log_event(
                entity_type="distillery_gap",
                entity_id=gap_key,
                action="resolve_distillery_gap",
                before=before_data,
                after={
                    **before_data,
                    "status": "RESOLVED",
                    "resolution": resolution_text,
                },
                details={
                    "catalog_version_id": clean_text(
                        before_data.get("catalog_version_id")
                    ),
                    "workflow_id": clean_text(
                        before_data.get("workflow_id")
                    ),
                },
            )
        return {
            "id": gap_key,
            "status": "RESOLVED",
            "resolution": resolution_text,
        }

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
        source_bytes: bytes | None,
        batch_name: str = "",
        reporting_date: date | str | None = None,
        *,
        source_kind: str = "",
        source_sha256: str = "",
        source_metadata: Mapping[str, Any] | None = None,
        initial_status: str = "",
        audit_action: str = "",
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        if not parsed.rows:
            raise ValueError("The Product Request source contains no data rows.")
        batch_id = new_id()
        timestamp = iso_now()
        extension = parsed.file_name.rsplit(".", 1)[-1].upper() if "." in parsed.file_name else "FILE"
        supplied_hash = clean_text(source_sha256).lower()
        if supplied_hash and not re.fullmatch(r"[a-f0-9]{64}", supplied_hash):
            raise ValueError("The supplied source SHA-256 is invalid.")
        if not supplied_hash and source_bytes is None:
            raise ValueError(
                "Source bytes or a validated source SHA-256 are required."
            )
        content_hash = supplied_hash or hashlib.sha256(source_bytes or b"").hexdigest()
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
            "source_kind": clean_text(source_kind).upper() or extension,
            "reporting_date": reporting_value,
            "status": clean_text(initial_status) or "Uploaded",
            "row_count": len(parsed.rows),
            "source_file_name": parsed.file_name,
            "source_sheet_name": parsed.sheet_name,
            "file_sha256": content_hash,
            "warnings": list(parsed.warnings),
            "metadata": {
                "columns": list(parsed.columns),
                "ingested_by": self.current_user(),
                "app_version": APP_VERSION,
                "source": _plain_data(source_metadata or {}),
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
                action=clean_text(audit_action) or "ingest_workbook",
                after=batch,
                details={
                    "source_file_name": parsed.file_name,
                    "source_sheet_name": parsed.sheet_name,
                    "source_kind": batch["source_kind"],
                    "row_count": len(workflow_rows),
                    "file_sha256": batch["file_sha256"],
                    "source": _plain_data(source_metadata or {}),
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
                        value:"audit_action"::VARCHAR AS AUDIT_ACTION,
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
                    AUDIT_ACTION = source.AUDIT_ACTION,
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
                    IF_IN_STOCK_ACTION, AUDIT_ACTION, BUYSMART_ACTION, RULE_APPLIED,
                    NEEDS_REVIEW, VALIDATION_STATUS, EXCLUDED, QUEUE_BUCKET,
                    OUTCOME_REPORTING, STATUS, UPDATED_AT, ROW_JSON
                ) VALUES (
                    source.ID, source.BATCH_ID, source.SOURCE_ROW_NUMBER,
                    source.BUSINESS, source.REQUEST_TYPE, source.CASE_NUMBER,
                    source.VENDOR, source.DIN, source.MIN, source.DESCRIPTION,
                    source.ACTION, source.IF_IN_STOCK_ACTION,
                    source.AUDIT_ACTION,
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


def workflow_rows_from_parsed(
    parsed: ParsedWorkbook,
    batch_id: str = "candidate-test",
) -> list[dict[str, Any]]:
    """Normalize an uploaded or live source without persisting a batch."""
    timestamp = iso_now()
    source_numbers = parsed.source_row_numbers or list(
        range(2, len(parsed.rows) + 2)
    )
    return [
        create_workflow_row(
            batch_id,
            raw_row,
            source_row_number=(
                source_numbers[index]
                if index < len(source_numbers)
                else index + 2
            ),
            now=timestamp,
        )
        for index, raw_row in enumerate(parsed.rows)
    ]


def compare_catalog_version(
    store: SnowflakeRulesStore,
    version_id: str,
    source_rows: Sequence[Mapping[str, Any]],
    *,
    source_label: str,
) -> dict[str, Any]:
    """Run active and candidate catalogs in memory with zero persistence."""
    version = store.get_catalog_version(version_id)
    if version is None:
        raise ValueError("The selected candidate catalog version no longer exists.")
    workflow_id = clean_text(version.get("workflow_id"))
    candidate_rules = store.load_catalog_version_rules(version_id)
    if not candidate_rules:
        raise ValueError("The selected candidate contains no versioned rules.")
    active_rules = [
        rule
        for rule in store.load_rules()
        if rule_workflow_id(rule) == workflow_id
    ]
    references = store.load_reference_lists()
    active_rows, _, _ = execute_rows(
        source_rows,
        active_rules,
        reference_lists=references,
    )
    candidate_rows, _, _ = execute_rows(
        source_rows,
        candidate_catalog_for_test(candidate_rules),
        reference_lists=references,
    )
    records: list[dict[str, Any]] = []
    same_count = 0
    for source, active, candidate in zip(
        source_rows,
        active_rows,
        candidate_rows,
    ):
        active_outcome = (
            clean_text(active.get("action")),
            clean_text(active.get("if_in_stock_action")),
            clean_text(active.get("audit_action")),
        )
        candidate_outcome = (
            clean_text(candidate.get("action")),
            clean_text(candidate.get("if_in_stock_action")),
            clean_text(candidate.get("audit_action")),
        )
        same = active_outcome == candidate_outcome
        same_count += int(same)
        records.append(
            {
                "Row": int(source.get("source_row_number") or 0),
                "Case": clean_text(source.get("case_number")),
                "Active ACTION": active_outcome[0],
                "Candidate ACTION": candidate_outcome[0],
                "Active If In Stock": active_outcome[1],
                "Candidate If In Stock": candidate_outcome[1],
                "Active Audit Action": active_outcome[2],
                "Candidate Audit Action": candidate_outcome[2],
                "Active Queue": clean_text(active.get("queue_bucket")),
                "Candidate Queue": clean_text(candidate.get("queue_bucket")),
                "Same atomic result": same,
            }
        )
    return {
        "catalog_version_id": version_id,
        "workflow_id": workflow_id,
        "source_label": source_label,
        "row_count": len(records),
        "same_count": same_count,
        "different_count": len(records) - same_count,
        "records": records,
    }


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
        "audit_action",
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
    "Rules Distillery",
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
            "recommended_action": f"Verify the eleven {TARGET_DATABASE}.{TARGET_SCHEMA}.{TABLE_PREFIX}_* tables and grant DML access to {TARGET_ROLE}. Use Settings → Verify backend tables for the exact failing object.",
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
        :root {
            /* Foodbuy foundations: exact active Storybook tokens. */
            --fb-primary-50: #F9F5FC;
            --fb-primary-100: #E5D7F4;
            --fb-primary-500: #7D36C9;
            --fb-primary-600: #642BA1;
            --fb-primary-800: #321650;
            --fb-secondary-50: #F3F8FE;
            --fb-secondary-500: #0E78F2;
            --fb-secondary-600: #0B60C2;
            --fb-secondary-800: #063061;
            --fb-success-50: #F3FCFA;
            --fb-success-500: #0FC4A3;
            --fb-success-600: #0C9D82;
            --fb-success-800: #064E41;
            --fb-warning-50: #FFFCF2;
            --fb-warning-500: #FFBB00;
            --fb-warning-600: #CC9600;
            --fb-warning-800: #664B00;
            --fb-danger-50: #FEF3F8;
            --fb-danger-500: #E6095A;
            --fb-danger-600: #B80846;
            --fb-danger-800: #5C0427;
            --fb-neutral-white: #FFFFFF;
            --fb-neutral-50: #F7F8F9;
            --fb-neutral-100: #DEE1E6;
            --fb-neutral-200: #BCC4CC;
            --fb-neutral-300: #9BA6B3;
            --fb-neutral-400: #798999;
            --fb-neutral-500: #586B80;
            --fb-neutral-600: #465666;
            --fb-neutral-700: #35404D;
            --fb-neutral-800: #232B33;
            --fb-neutral-900: #12151A;
            --fb-radius-xs: 4px;
            --fb-radius-sm: 8px;
            --fb-radius-md: 12px;
            --fb-radius-pill: 1000px;
            --fb-shadow-100: 0 2px 4px rgba(21, 33, 54, .08);
            --fb-shadow-200: 0 2px 8px rgba(21, 33, 54, .10);
            --fb-shadow-300: 0 8px 24px rgba(21, 33, 54, .12);
            --fb-shadow-900: 0 1px 32px rgba(21, 33, 54, .12);
            --fb-space-1: 4px;
            --fb-space-2: 8px;
            --fb-space-3: 12px;
            --fb-space-4: 16px;
            --fb-space-5: 20px;
            --fb-space-6: 24px;
            --fb-space-8: 32px;
            --fb-space-10: 40px;
            --fb-space-12: 48px;
            --fb-font: "DM Sans", "Inter", -apple-system, BlinkMacSystemFont,
                "Segoe UI", sans-serif;
        }

        html, body, .stApp, .stApp button, .stApp input,
        .stApp textarea, .stApp select {
            font-family: var(--fb-font);
        }

        .stApp {
            background: var(--fb-neutral-50);
            color: var(--fb-neutral-700);
        }

        .block-container {
            max-width: 1200px;
            padding: var(--fb-space-6) var(--fb-space-8) var(--fb-space-12);
        }

        h1, h2, h3, h4, h5, h6 {
            color: var(--fb-neutral-900);
            font-family: var(--fb-font);
            font-weight: 600;
            letter-spacing: -.015em;
        }

        h1 {font-size: 36px; line-height: 1.2;}
        h2 {font-size: 28px; line-height: 32px;}
        h3 {font-size: 24px; line-height: 28px;}
        p, li, label {font-size: 14px; line-height: 20px;}
        a {color: var(--fb-secondary-600);}
        a:hover {color: var(--fb-secondary-800);}

        [data-testid="stSidebar"] {
            background: var(--fb-neutral-white);
            border-right: 1px solid var(--fb-neutral-100);
        }

        header[data-testid="stHeader"],
        [data-testid="stHeader"],
        .stAppHeader {
            color: var(--fb-neutral-700) !important;
            background: rgba(247, 248, 249, .98) !important;
            border-bottom: 1px solid var(--fb-neutral-100);
            box-shadow: none !important;
        }

        [data-testid="stSidebarCollapsedControl"] {
            color: var(--fb-neutral-700) !important;
        }

        [data-testid="stSidebarCollapsedControl"] button {
            color: var(--fb-neutral-700) !important;
            background: var(--fb-neutral-white) !important;
            border: 1px solid var(--fb-neutral-100) !important;
            border-radius: var(--fb-radius-sm) !important;
            box-shadow: var(--fb-shadow-100) !important;
        }

        [data-testid="stSidebarCollapsedControl"] button:hover {
            color: var(--fb-primary-600) !important;
            background: var(--fb-primary-50) !important;
            border-color: var(--fb-primary-100) !important;
        }

        [data-testid="stLogo"] img,
        [data-testid="stSidebar"] img {
            border-radius: var(--fb-radius-md);
        }

        [data-testid="stSidebar"] > div:first-child {
            padding-top: var(--fb-space-4);
        }

        [data-testid="stSidebar"] img {
            box-shadow: var(--fb-shadow-100);
        }

        [data-testid="stSidebar"] [role="radiogroup"] label {
            min-height: 44px;
            border-radius: var(--fb-radius-sm);
            padding: 8px 12px;
            transition: background-color .15s ease, color .15s ease;
        }

        [data-testid="stSidebar"] [role="radiogroup"] label:hover {
            background: var(--fb-neutral-50);
        }

        [data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) {
            background: var(--fb-primary-50);
            color: var(--fb-primary-600);
            font-weight: 600;
        }

        .stButton > button,
        .stDownloadButton > button,
        [data-testid="stFormSubmitButton"] > button {
            min-height: 44px;
            border: 1px solid var(--fb-primary-500);
            border-radius: var(--fb-radius-sm);
            padding: 0 var(--fb-space-4);
            font-family: var(--fb-font);
            font-size: 14px;
            font-weight: 600;
            box-shadow: none;
            transition: background-color .15s ease, border-color .15s ease,
                color .15s ease, box-shadow .15s ease, transform .15s ease;
        }

        .stButton > button[kind="primary"],
        [data-testid="stFormSubmitButton"] > button[kind="primary"] {
            color: var(--fb-neutral-white);
            background: var(--fb-primary-500);
        }

        .stButton > button[kind="primary"]:hover,
        [data-testid="stFormSubmitButton"] > button[kind="primary"]:hover {
            background: var(--fb-primary-600);
            border-color: var(--fb-primary-600);
            box-shadow: var(--fb-shadow-100);
        }

        .stButton > button[kind="secondary"],
        .stDownloadButton > button {
            color: var(--fb-primary-600);
            background: var(--fb-neutral-white);
        }

        .stButton > button[kind="secondary"]:hover,
        .stDownloadButton > button:hover {
            color: var(--fb-primary-800);
            background: var(--fb-primary-50);
            border-color: var(--fb-primary-600);
        }

        .stButton > button:disabled,
        .stDownloadButton > button:disabled {
            color: var(--fb-neutral-500);
            background: var(--fb-neutral-50);
            border-color: var(--fb-neutral-200);
            opacity: .72;
        }

        button:focus-visible,
        input:focus-visible,
        textarea:focus-visible,
        [role="button"]:focus-visible,
        [role="tab"]:focus-visible,
        [role="radio"]:focus-visible,
        [role="checkbox"]:focus-visible {
            outline: 2px solid var(--fb-primary-500) !important;
            outline-offset: 2px;
        }

        [data-baseweb="input"],
        [data-baseweb="select"] > div,
        [data-baseweb="textarea"],
        .stTextInput input,
        .stNumberInput input,
        .stTextArea textarea,
        .stDateInput input {
            color: var(--fb-neutral-700);
            background: var(--fb-neutral-white);
            border-color: var(--fb-neutral-400) !important;
            border-radius: var(--fb-radius-sm) !important;
            min-height: 44px;
        }

        [data-baseweb="input"]:focus-within,
        [data-baseweb="select"] > div:focus-within,
        [data-baseweb="textarea"]:focus-within {
            border-color: var(--fb-primary-500) !important;
            box-shadow: 0 0 0 2px rgba(125, 54, 201, .16) !important;
        }

        [data-baseweb="popover"],
        [data-baseweb="menu"],
        [data-baseweb="calendar"] {
            border: 1px solid var(--fb-neutral-100);
            border-radius: var(--fb-radius-md);
            box-shadow: var(--fb-shadow-300);
        }

        [data-testid="stFileUploaderDropzone"] {
            min-height: 120px;
            background: var(--fb-neutral-white);
            border: 1px dashed var(--fb-neutral-400);
            border-radius: var(--fb-radius-md);
        }

        [data-testid="stFileUploaderDropzone"]:hover {
            background: var(--fb-primary-50);
            border-color: var(--fb-primary-500);
        }

        [data-testid="stMetric"] {
            min-height: 112px;
            padding: var(--fb-space-4);
            background: var(--fb-neutral-white);
            border: 1px solid var(--fb-neutral-100);
            border-radius: var(--fb-radius-md);
            box-shadow: var(--fb-shadow-100);
        }

        [data-testid="stMetricLabel"] {
            color: var(--fb-neutral-500);
            font-size: 12px;
            line-height: 16px;
        }

        [data-testid="stMetricValue"] {
            color: var(--fb-neutral-900);
            font-size: 28px;
            line-height: 32px;
            font-weight: 600;
        }

        [data-testid="stAlert"] {
            border: 1px solid var(--fb-neutral-100);
            border-radius: var(--fb-radius-sm);
            box-shadow: none;
        }

        [data-testid="stExpander"] {
            overflow: hidden;
            background: var(--fb-neutral-white);
            border: 1px solid var(--fb-neutral-100);
            border-radius: var(--fb-radius-md);
        }

        [data-testid="stForm"] {
            background: var(--fb-neutral-white);
            border: 1px solid var(--fb-neutral-100);
            border-radius: var(--fb-radius-md);
            padding: var(--fb-space-6);
            box-shadow: var(--fb-shadow-100);
        }

        .stTabs [data-baseweb="tab-list"] {
            gap: var(--fb-space-1);
            border-bottom: 1px solid var(--fb-neutral-100);
        }

        .stTabs [role="tab"] {
            min-height: 44px;
            color: var(--fb-neutral-500);
            border-radius: var(--fb-radius-sm) var(--fb-radius-sm) 0 0;
            font-weight: 600;
        }

        .stTabs [role="tab"]:hover {
            color: var(--fb-primary-600);
            background: var(--fb-neutral-50);
        }

        .stTabs [role="tab"][aria-selected="true"] {
            color: var(--fb-primary-600);
            background: var(--fb-primary-50);
        }

        [data-testid="stDataFrame"],
        [data-testid="stTable"] {
            overflow: hidden;
            background: var(--fb-neutral-white);
            border: 1px solid var(--fb-neutral-100);
            border-radius: var(--fb-radius-sm);
        }

        hr {
            border-color: var(--fb-neutral-100);
            margin: var(--fb-space-6) 0;
        }

        .rules-kicker {
            display: inline-flex;
            align-items: center;
            min-height: 24px;
            color: var(--fb-primary-600);
            background: var(--fb-primary-50);
            border: 1px solid var(--fb-primary-100);
            border-radius: var(--fb-radius-pill);
            font-size: 12px;
            line-height: 16px;
            letter-spacing: .08em;
            text-transform: uppercase;
            font-weight: 700;
            padding: var(--fb-space-1) var(--fb-space-2);
            margin-bottom: var(--fb-space-2);
        }

        .rules-live-badge {
            display: inline-flex;
            align-items: center;
            gap: var(--fb-space-2);
            color: var(--fb-success-800);
            background: var(--fb-success-50);
            border: 1px solid var(--fb-success-500);
            border-radius: var(--fb-radius-pill);
            padding: var(--fb-space-1) var(--fb-space-3);
            font-size: 12px;
            line-height: 16px;
            font-weight: 700;
        }

        .rules-live-badge::before {
            content: "";
            width: 8px;
            height: 8px;
            background: var(--fb-success-500);
            border-radius: var(--fb-radius-pill);
        }

        .rules-subtitle {
            max-width: 76ch;
            color: var(--fb-neutral-500);
            font-size: 16px;
            line-height: 24px;
            margin-top: -8px;
            margin-bottom: var(--fb-space-6);
        }

        .rules-card {
            color: var(--fb-neutral-700);
            background: var(--fb-neutral-white);
            border: 1px solid var(--fb-neutral-100);
            border-radius: var(--fb-radius-md);
            padding: var(--fb-space-4);
            margin: var(--fb-space-2) 0;
            box-shadow: var(--fb-shadow-100);
        }

        .rules-muted {
            color: var(--fb-neutral-500);
        }

        @media (min-width: 1280px) {
            .block-container {
                max-width: 1200px;
                padding-left: var(--fb-space-12);
                padding-right: var(--fb-space-12);
            }
        }

        @media (min-width: 1920px) {
            .block-container {max-width: 1680px;}
        }

        @media (max-width: 767px) {
            .block-container {
                padding: var(--fb-space-4);
            }
            h1 {font-size: 28px; line-height: 32px;}
            h2 {font-size: 24px; line-height: 28px;}
            [data-testid="stMetric"] {min-height: 96px;}
        }

        @media (prefers-reduced-motion: reduce) {
            *, *::before, *::after {
                scroll-behavior: auto !important;
                animation-duration: .01ms !important;
                animation-iteration-count: 1 !important;
                transition-duration: .01ms !important;
            }
        }
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
            "Audit Action": clean_text(row.get("audit_action")),
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
                "Audit Action": fields.get("upstreamAuditAction"),
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
        "Choose uploaded or live Product Request data, inspect normalization, create a persistent batch, and optionally execute immediately.",
        kicker="Ingestion",
    )
    source_option = st.radio(
        "Product Request data source",
        ["Upload a file", "Use Live Product Request Data"],
        horizontal=True,
        key="process_workbook_source",
    )
    parsed: ParsedWorkbook | None = None
    source_bytes: bytes | None = None
    source_hash = ""
    source_name = ""
    source_kind = ""
    source_metadata: dict[str, Any] = {}
    initial_status = ""
    audit_action = ""
    diagnostics: Mapping[str, Any] | None = None
    is_live_source = source_option == "Use Live Product Request Data"

    if not is_live_source:
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
            outcome = parse_source_workbook_for_ui(
                uploaded.name,
                source_hash,
                source_bytes,
                retry_nonce,
            )
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
                "file": {
                    "name": uploaded.name,
                    "size_bytes": len(source_bytes),
                    "sha256": source_hash,
                },
                "environment": runtime_environment_snapshot(),
                "stages": [],
            }
        if not bool_value(outcome.get("ok") if isinstance(outcome, Mapping) else False):
            render_workbook_parse_failure(diagnostics, source_hash)
            return
        parsed = parsed_workbook_from_payload(
            outcome.get("workbook") if isinstance(outcome, Mapping) else None
        )
        if parsed is None:
            render_actionable_exception(
                "Workbook payload reconstruction failed.",
                RuntimeError(
                    "Successful parser outcome could not be reconstructed from its plain-data payload."
                ),
                component="Workbook payload reconstruction",
                context={
                    "file_name": uploaded.name,
                    "source_hash": source_hash,
                    "diagnostics": diagnostics,
                },
            )
            return
        source_name = uploaded.name
        source_kind = (
            uploaded.name.rsplit(".", 1)[-1].upper()
            if "." in uploaded.name
            else "FILE"
        )
        source_metadata = {
            "source_type": "file_upload",
            "file_name": uploaded.name,
            "mime_type": clean_text(getattr(uploaded, "type", "")),
            "size_bytes": len(source_bytes),
        }
        audit_action = "ingest_workbook"
    else:
        display_view = (
            f"{TARGET_DATABASE}.{TARGET_SCHEMA}.{LIVE_PRODUCT_REQUEST_VIEW}"
        )
        st.info(
            f"OneEngine will snapshot **{display_view}** now. The snapshot enters the same normalization, validation, batch, and rule-execution path as an uploaded file."
        )
        refresh_live = st.button(
            "Refresh live Product Request snapshot",
            help="Discard the current preview snapshot and read the view again.",
        )
        snapshot_key = "_live_product_request_snapshot"
        snapshot = st.session_state.get(snapshot_key)
        if refresh_live:
            snapshot = None
            st.session_state.pop(snapshot_key, None)
        valid_snapshot = (
            isinstance(snapshot, Mapping)
            and snapshot.get("payload_type") == "LiveProductRequestSnapshot"
            and int(snapshot.get("payload_version") or 0) == 1
            and clean_text(snapshot.get("source_view")) == display_view
        )
        if not valid_snapshot:
            try:
                with st.spinner("Reading live Product Request data from Snowflake…"):
                    live_parsed, live_hash, live_metadata = (
                        store.load_live_product_request_data()
                    )
                snapshot = {
                    "payload_type": "LiveProductRequestSnapshot",
                    "payload_version": 1,
                    "source_view": display_view,
                    "source_hash": live_hash,
                    "workbook": parsed_workbook_to_payload(live_parsed),
                    "metadata": _plain_data(live_metadata),
                }
                st.session_state[snapshot_key] = snapshot
            except Exception as exc:
                render_actionable_exception(
                    "Live Product Request data could not be loaded.",
                    exc,
                    component="Snowflake live Product Request source",
                    context={
                        "source_view": display_view,
                        "required_access": f"SELECT on {display_view}",
                    },
                )
                return
        parsed = parsed_workbook_from_payload(snapshot.get("workbook"))
        source_hash = clean_text(snapshot.get("source_hash")).lower()
        raw_metadata = snapshot.get("metadata")
        if (
            parsed is None
            or not re.fullmatch(r"[a-f0-9]{64}", source_hash)
            or not isinstance(raw_metadata, Mapping)
        ):
            st.session_state.pop(snapshot_key, None)
            render_actionable_exception(
                "The live Product Request snapshot could not be reconstructed.",
                RuntimeError(
                    "The session snapshot contract is incomplete; refresh the live snapshot."
                ),
                component="Snowflake live Product Request snapshot",
                context={"source_view": display_view},
            )
            return
        source_name = display_view
        source_kind = "SNOWFLAKE_VIEW"
        source_metadata = dict(raw_metadata)
        initial_status = "Loaded"
        audit_action = "ingest_live_product_request_view"

    known_columns = [column for column in parsed.columns if column in EXPECTED_HEADERS]
    required_missing = [column for column in ("Business", "Type") if column not in parsed.columns]
    metrics = st.columns(4)
    metrics[0].metric("Rows", f"{len(parsed.rows):,}")
    metrics[1].metric("Columns", f"{len(parsed.columns):,}")
    metrics[2].metric("Recognized columns", f"{len(known_columns):,}")
    metrics[3].metric(
        "Source",
        LIVE_PRODUCT_REQUEST_VIEW if is_live_source else clean_text(parsed.sheet_name),
    )
    if is_live_source:
        st.caption(
            f"Live snapshot PASS · {source_name} · Snapshot SHA-256 {source_hash} · "
            f"Captured {timestamp_text(source_metadata.get('snapshot_at'))[:19].replace('T', ' ')}"
        )
    else:
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
            f"This exact source snapshot was already ingested as **{clean_text(duplicate.get('name'))}** on "
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
        if is_live_source:
            st.json(_plain_data(source_metadata))
    if diagnostics is not None:
        render_workbook_diagnostics(diagnostics, expanded=False)

    stem = (
        f"Live Product Requests {date.today().isoformat()}"
        if is_live_source
        else source_name.rsplit(".", 1)[0]
    )
    form_key = source_hash[:12]
    with st.form(f"ingest_form_{form_key}"):
        batch_name = st.text_input("Batch name", value=stem)
        include_reporting_date = st.checkbox("Set a reporting date", value=False)
        reporting_date = st.date_input("Reporting date", value=date.today(), disabled=not include_reporting_date)
        execute_immediately = st.checkbox("Execute all approved rules immediately after ingestion", value=False)
        submitted = st.form_submit_button(
            (
                "Create batch from live Product Request data"
                if is_live_source
                else "Ingest workbook"
            ),
            type="primary",
            disabled=not allow_duplicate,
        )
    if submitted:
        try:
            batch, _ = store.create_batch(
                parsed,
                source_bytes,
                batch_name=batch_name,
                reporting_date=reporting_date if include_reporting_date else None,
                source_kind=source_kind,
                source_sha256=source_hash if is_live_source else "",
                source_metadata=source_metadata,
                initial_status=initial_status,
                audit_action=audit_action,
            )
            execution_message = ""
            if execute_immediately:
                result = run_batch(store, batch["id"], dry_run=False)
                execution_message = f" Rules changed {int(result.run.get('changed_row_count') or 0):,} row(s)."
            st.session_state["selected_batch_id"] = batch["id"]
            st.session_state["_pending_batch_picker"] = batch["id"]
            set_flash(
                f"Ingested {len(parsed.rows):,} rows from **{source_name}** into **{batch['name']}**.{execution_message}"
            )
            safe_rerun()
        except Exception as exc:
            render_actionable_exception(
                "The Product Request source was prepared, but Snowflake ingestion failed.",
                exc,
                component="Product Request ingestion",
                context={
                    "source_name": source_name,
                    "source_kind": source_kind,
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
            first = st.columns(4)
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
            audit_options = options_with_current(
                AUDIT_ACTION_OPTIONS,
                selected_row.get("audit_action"),
            )
            audit_action = first[2].selectbox(
                "Audit Action",
                audit_options,
                index=(
                    audit_options.index(clean_text(selected_row.get("audit_action")))
                    if clean_text(selected_row.get("audit_action")) in audit_options
                    else 0
                ),
            )
            buysmart_options = options_with_current(BUYSMART_OPTIONS, selected_row.get("buysmart_action"))
            buysmart = first[3].selectbox(
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
                    "audit_action": audit_action,
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
            "Audit Action": clean_text(selected_row.get("audit_action")),
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


def render_rules_distillery_page_legacy(store: SnowflakeRulesStore) -> None:
    render_page_header(
        "Rules Distillery",
        (
            "Reverse engineer reusable rules from paired BEFORE and AFTER "
            "evidence, validate them, and promote the approved catalog "
            "directly into Snowflake."
        ),
        kicker="Mechanized Rule Discovery",
    )
    st.info(
        "Generated catalogs are held only in this browser session until "
        "promotion. Promotion writes rule JSON to the Snowflake rules table "
        "and records the validation report in the audit table."
    )
    controls = st.columns([2, 1])
    profile_id = controls[0].selectbox(
        "Ruleset profile",
        list(DISTILLERY_PROFILES),
        format_func=lambda value: clean_text(
            DISTILLERY_PROFILES[value].get("description")
        ),
        key="distillery_profile",
    )
    run_holdouts = controls[1].checkbox(
        "Leave-one-source-group-out validation",
        value=True,
        key="distillery_holdouts",
        help=(
            "Recommended for promotion. This measures reusable rules against "
            "each unseen source group without using evidence exceptions."
        ),
    )
    source_columns = st.columns(2)
    with source_columns[0]:
        before_upload = st.file_uploader(
            "BEFORE evidence",
            type=["zip", *sorted(DISTILLERY_SUPPORTED_EXTENSIONS)],
            key="distillery_before",
            help=(
                "Upload one source file or a ZIP containing accumulated "
                "BEFORE sources. ZIP member basenames define pairing groups."
            ),
        )
    with source_columns[1]:
        after_upload = st.file_uploader(
            "AFTER evidence",
            type=["zip", *sorted(DISTILLERY_SUPPORTED_EXTENSIONS)],
            key="distillery_after",
            help=(
                "Upload the corresponding AFTER file or ZIP. Source-group "
                "basenames must match the BEFORE collection."
            ),
        )
    run_disabled = before_upload is None or after_upload is None
    if st.button(
        "Distill and validate rules",
        type="primary",
        disabled=run_disabled,
        key="distillery_run",
    ):
        try:
            with st.spinner(
                "Aligning evidence, inducing pure filters, closing residuals, "
                "and validating the catalog…"
            ):
                result = run_rules_distillery(
                    profile_id=profile_id,
                    before_file_name=clean_text(before_upload.name),
                    before_bytes=before_upload.getvalue(),
                    after_file_name=clean_text(after_upload.name),
                    after_bytes=after_upload.getvalue(),
                    run_holdouts=run_holdouts,
                )
            st.session_state["_distillery_result"] = result
            report = result["report"]
            set_flash(
                (
                    f"Distillery run {result['run_id']} completed: "
                    f"{report['validation']['exact_count']:,}/"
                    f"{report['validation']['row_count']:,} exact rows and "
                    f"{report['rules']['total']:,} generated rules."
                ),
                "success"
                if result.get("deployment_eligible")
                else "warning",
            )
            safe_rerun()
        except Exception as exc:
            render_actionable_exception(
                "Rules Distillery could not complete this evidence run.",
                exc,
                component="Rules Distillery",
                context={
                    "profile_id": profile_id,
                    "before_file": clean_text(
                        getattr(before_upload, "name", "")
                    ),
                    "after_file": clean_text(
                        getattr(after_upload, "name", "")
                    ),
                    "run_holdouts": run_holdouts,
                },
            )
    result = st.session_state.get("_distillery_result")
    if not isinstance(result, Mapping):
        st.caption(
            "Upload paired evidence and run the Distillery to create an "
            "in-memory candidate catalog."
        )
        return
    report = result.get("report") or {}
    if clean_text(report.get("profile_id")) != profile_id:
        st.warning(
            "The displayed result belongs to a different profile. Run the "
            "selected profile to replace it."
        )
    matching = report.get("matching") or {}
    rules = report.get("rules") or {}
    validation = report.get("validation") or {}
    holdout = report.get("holdout") or {}
    gate = report.get("deployment_gate") or {}
    metrics = st.columns(6)
    metrics[0].metric("Aligned pairs", f"{int(matching.get('pairs') or 0):,}")
    metrics[1].metric("Unmatched", f"{int(matching.get('unmatched') or 0):,}")
    metrics[2].metric("General rules", f"{int(rules.get('general') or 0):,}")
    metrics[3].metric("Evidence rules", f"{int(rules.get('exception') or 0):,}")
    metrics[4].metric(
        "Corpus parity",
        f"{float(validation.get('accuracy') or 0.0):.2%}",
    )
    metrics[5].metric(
        "Holdout accuracy",
        (
            f"{float(holdout.get('mean_accuracy') or 0.0):.2%}"
            if clean_text(holdout.get("strategy")) != "not-run"
            else "Not run"
        ),
    )
    if bool_value(gate.get("eligible")):
        st.success(
            "Deployment gate passed: exact corpus parity, zero unmatched rows, "
            "and zero contradictory identical states."
        )
    else:
        st.error(
            "Deployment gate failed. The candidate catalog is disabled and "
            "cannot be promoted."
        )
    folds = holdout.get("folds") or {}
    if isinstance(folds, Mapping) and folds:
        with st.expander("Temporal/source-group validation", expanded=False):
            dataframe(
                pd.DataFrame(
                    [
                        {
                            "Source group": group,
                            "Testing rows": int(values.get("testing_rows") or 0),
                            "Rules": int(values.get("rule_count") or 0),
                            "Accuracy": float(values.get("accuracy") or 0.0),
                            "Exact": int(values.get("exact") or 0),
                            "Uncovered": int(values.get("uncovered") or 0),
                            "Mismatched": int(values.get("mismatched") or 0),
                        }
                        for group, values in folds.items()
                    ]
                )
            )
    with st.expander("Distillery run report", expanded=False):
        st.json(report)
        st.download_button(
            "Download validation report",
            data=json_dumps(report, pretty=True),
            file_name=(
                f"one_engine_distillery_{clean_text(result.get('run_id'))}.json"
            ),
            mime="application/json",
            key="distillery_report_download",
        )
    st.markdown("#### Promote to Snowflake")
    confirmation = st.checkbox(
        (
            "I confirm this validated catalog should replace the active "
            f"{profile_id} Distillery catalog."
        ),
        key="distillery_promote_confirm",
    )
    if st.button(
        "Promote catalog to COMPLIANCE_RULES_RULES",
        disabled=not (
            confirmation and bool_value(result.get("deployment_eligible"))
        ),
        key="distillery_promote",
    ):
        try:
            promotion = store.promote_distilled_catalog(
                result.get("catalog") or [],
                report,
            )
            set_flash(
                (
                    f"Promoted {promotion['promoted_rule_count']:,} "
                    f"{profile_id} rules to Snowflake and retired "
                    f"{promotion['retired_rule_count']:,} obsolete rules."
                )
            )
            safe_rerun()
        except Exception as exc:
            render_actionable_exception(
                "The validated catalog could not be promoted to Snowflake.",
                exc,
                component="Rules Distillery promotion",
                context={
                    "run_id": clean_text(result.get("run_id")),
                    "profile_id": profile_id,
                    "rule_count": len(result.get("catalog") or []),
                },
            )


def render_rules_distillery_page(store: SnowflakeRulesStore) -> None:
    render_page_header(
        "Rules Distillery",
        (
            "Reconstruct literal business filters from every matching dated "
            "BEFORE/AFTER pair, then test an immutable candidate before activation."
        ),
        kicker="Mechanized Rule Discovery",
    )
    st.info(
        "A Distillery run never overwrites active rules or stored workflow rows. "
        "Only an explicit activation materializes one complete workflow version; "
        "every prior version remains available for exact rollback."
    )
    controls = st.columns([2, 1])
    profile_id = controls[0].selectbox(
        "Ruleset profile",
        list(DISTILLERY_PROFILES),
        format_func=lambda value: clean_text(
            DISTILLERY_PROFILES[value].get("description")
        ),
        key="literal_distillery_profile",
    )
    run_holdouts = controls[1].checkbox(
        "Leave-one-date-out validation",
        value=False,
        key="literal_distillery_holdouts",
        help=(
            "Optional and compute-intensive: retrains reusable filters ten "
            "times, each with one date withheld from discovery."
        ),
    )
    try:
        persisted_aliases = store.load_outcome_aliases(profile_id)
    except Exception as exc:
        persisted_aliases = {}
        render_actionable_exception(
            "Outcome aliases could not be loaded.",
            exc,
            component="Distillery outcome aliases",
            context={
                "profile_id": profile_id,
                "required_table": store.table("outcome_aliases"),
            },
        )

    approval_key = f"_literal_rule_approvals_{profile_id}"
    approved_signatures = [
        clean_text(value)
        for value in st.session_state.get(approval_key, [])
        if clean_text(value)
    ]
    upload_columns = st.columns(2)
    with upload_columns[0]:
        before_upload = st.file_uploader(
            "BEFORE evidence",
            type=["zip", *sorted(DISTILLERY_SUPPORTED_EXTENSIONS)],
            key="literal_distillery_before",
            help="Dated member basenames are paired with the AFTER collection.",
        )
    with upload_columns[1]:
        after_upload = st.file_uploader(
            "AFTER evidence",
            type=["zip", *sorted(DISTILLERY_SUPPORTED_EXTENSIONS)],
            key="literal_distillery_after",
            help=(
                "ACTION, If In Stock: Action, and Audit Action form one "
                "atomic final result."
            ),
        )

    def execute_distillery(
        signatures: Sequence[str],
    ) -> dict[str, Any]:
        if before_upload is None or after_upload is None:
            raise ValueError("Both BEFORE and AFTER evidence are required.")
        return run_rules_distillery(
            profile_id=profile_id,
            before_file_name=clean_text(before_upload.name),
            before_bytes=before_upload.getvalue(),
            after_file_name=clean_text(after_upload.name),
            after_bytes=after_upload.getvalue(),
            run_holdouts=run_holdouts,
            outcome_aliases=persisted_aliases,
            approved_rule_signatures=signatures,
        )

    if st.button(
        "Reconstruct literal filters",
        type="primary",
        disabled=before_upload is None or after_upload is None,
        key="literal_distillery_run",
    ):
        try:
            with st.spinner(
                "Pairing every date, reconstructing pure filters, minimizing "
                "logic, and validating all three outcomes..."
            ):
                result = execute_distillery(approved_signatures)
            st.session_state["_literal_distillery_result"] = result
            report = result["report"]
            set_flash(
                (
                    f"Distillery run {result['run_id']} completed: "
                    f"{report['validation']['exact_count']:,}/"
                    f"{report['validation']['row_count']:,} exact rows and "
                    f"{report['rules']['total']:,} literal filters."
                ),
                "success"
                if result.get("deployment_eligible")
                else "warning",
            )
            safe_rerun()
        except Exception as exc:
            render_actionable_exception(
                "Rules Distillery could not complete this evidence run.",
                exc,
                component="Rules Distillery",
                context={
                    "profile_id": profile_id,
                    "before_file": clean_text(
                        getattr(before_upload, "name", "")
                    ),
                    "after_file": clean_text(
                        getattr(after_upload, "name", "")
                    ),
                },
            )

    result = st.session_state.get("_literal_distillery_result")
    if (
        isinstance(result, Mapping)
        and clean_text(result.get("profile_id")) == profile_id
    ):
        report = result.get("report") or {}
        matching = report.get("matching") or {}
        rules = report.get("rules") or {}
        validation = report.get("validation") or {}
        holdout = report.get("holdout") or {}
        gate = report.get("deployment_gate") or {}
        metrics = st.columns(8)
        metrics[0].metric(
            "Logical rows", f"{int(matching.get('pairs') or 0):,}"
        )
        metrics[1].metric(
            "Unmatched", f"{int(matching.get('unmatched') or 0):,}"
        )
        metrics[2].metric(
            "Reusable", f"{int(rules.get('reusable') or 0):,}"
        )
        metrics[3].metric(
            "One-date", f"{int(rules.get('one_date') or 0):,}"
        )
        metrics[4].metric(
            "Gaps", f"{int((report.get('gaps') or {}).get('count') or 0):,}"
        )
        metrics[5].metric(
            "Conflicts",
            f"{int((report.get('conflicts') or {}).get('count') or 0):,}",
        )
        metrics[6].metric(
            "Corpus parity",
            f"{float(validation.get('accuracy') or 0.0):.2%}",
        )
        metrics[7].metric(
            "Holdout",
            (
                f"{float(holdout.get('mean_accuracy') or 0.0):.2%}"
                if clean_text(holdout.get("strategy")) != "not-run"
                else "Not run"
            ),
        )
        if bool_value(gate.get("eligible")):
            st.success(
                "Activation gate passed: exact three-field parity and zero "
                "unmatched rows, contradictions, gaps, conflicts, pending "
                "aliases, or pending one-date approvals."
            )
        else:
            observed = gate.get("observed") or {}
            failures = [
                f"{key.replace('_', ' ')}={value}"
                for key, value in observed.items()
                if (
                    (key == "corpus_accuracy" and float(value or 0) != 1.0)
                    or (key != "corpus_accuracy" and int(value or 0) != 0)
                )
            ]
            st.error(
                "Activation is blocked. "
                + (", ".join(failures) if failures else "Gate requirements failed.")
            )

        labels = report.get("labels") or {}
        alias_registry = labels.get("alias_registry") or {}
        st.markdown("#### Outcome aliases")
        st.caption(
            "Case and spacing normalize automatically. For a near-match, set "
            "both raw values to the same canonical value to merge them, or "
            "leave them distinct and save to record that review decision."
        )
        alias_entries = alias_registry.get("entries") or []
        alias_frame = pd.DataFrame(
            [
                {
                    "Field": clean_text(entry.get("field_name")),
                    "Raw value": clean_text(entry.get("raw_value")),
                    "Canonical value": clean_text(
                        entry.get("canonical_value")
                    ),
                    "Rows": int(entry.get("row_count") or 0),
                    "Status": clean_text(entry.get("status")),
                }
                for entry in alias_entries
            ]
        )
        edited_aliases = st.data_editor(
            alias_frame,
            disabled=["Field", "Raw value", "Rows", "Status"],
            hide_index=True,
            use_container_width=True,
            key=f"literal_alias_editor_{result.get('run_id')}",
        )
        suggestions = alias_registry.get("suggestions") or []
        if suggestions:
            st.warning(
                f"{len(suggestions):,} near-match alias decision(s) require review."
            )
            dataframe(
                pd.DataFrame(
                    [
                        {
                            "Field": item.get("field_name"),
                            "Value A": item.get("left_value"),
                            "Value B": item.get("right_value"),
                            "Similarity": item.get("similarity"),
                        }
                        for item in suggestions
                    ]
                )
            )
        if st.button(
            "Save reviewed outcome aliases",
            key="literal_save_aliases",
            disabled=alias_frame.empty,
        ):
            try:
                alias_values = [
                    {
                        "field_name": clean_text(row.get("Field")),
                        "raw_value": clean_text(row.get("Raw value")),
                        "canonical_value": clean_text(
                            row.get("Canonical value")
                        ),
                    }
                    for row in edited_aliases.to_dict(orient="records")
                ]
                saved_count = store.save_outcome_aliases(
                    profile_id,
                    alias_values,
                )
                st.session_state.pop("_literal_distillery_result", None)
                set_flash(
                    f"Saved {saved_count:,} reviewed aliases. Reconstruct the "
                    "filters to apply them."
                )
                safe_rerun()
            except Exception as exc:
                render_actionable_exception(
                    "Reviewed outcome aliases could not be saved.",
                    exc,
                    component="Distillery outcome aliases",
                    context={"profile_id": profile_id},
                )

        st.markdown("#### Outcome permutations by date")
        permutation_records: list[dict[str, Any]] = []
        for view_label, key in (
            ("Raw", "raw_permutations_by_date"),
            ("Canonical", "canonical_permutations_by_date"),
        ):
            for source_group, values in (labels.get(key) or {}).items():
                for outcome_json, count in (
                    (values or {}).get("counts") or {}
                ).items():
                    outcome = normalize_persisted_json(outcome_json, {})
                    permutation_records.append(
                        {
                            "Date / source": source_group,
                            "View": view_label,
                            "ACTION": clean_text(
                                (outcome or {}).get("action")
                            ),
                            "If In Stock: Action": clean_text(
                                (outcome or {}).get("if_in_stock_action")
                            ),
                            "Audit Action": clean_text(
                                (outcome or {}).get("audit_action")
                            ),
                            "Rows": int(count or 0),
                        }
                    )
        if permutation_records:
            dataframe(pd.DataFrame(permutation_records), height=520)

        catalog = result.get("catalog") or []
        rule_records: list[dict[str, Any]] = []
        for rule in catalog:
            source = rule.get("source") or {}
            outcome = source.get("outcome") or {}
            rule_records.append(
                {
                    "Rule": clean_text(rule.get("name")),
                    "Kind": clean_text(source.get("distilled_rule_kind")),
                    "Filter logic": clean_text(source.get("filter_logic")),
                    "ACTION": clean_text(outcome.get("action")),
                    "If In Stock: Action": clean_text(
                        outcome.get("if_in_stock_action")
                    ),
                    "Audit Action": clean_text(outcome.get("audit_action")),
                    "Supporting dates": ", ".join(
                        source.get("source_groups") or []
                    ),
                    "Covered rows": int(source.get("support") or 0),
                    "Approval": (
                        "Approved"
                        if bool_value(source.get("approved"))
                        else "Required"
                    ),
                    "Logic signature": clean_text(
                        source.get("logic_signature")
                    ),
                }
            )
        reusable_records = [
            row for row in rule_records if row["Kind"] == "reusable"
        ]
        one_date_records = [
            row for row in rule_records if row["Kind"] == "one_date"
        ]
        rules_tabs = st.tabs(
            [
                f"Reusable filters ({len(reusable_records)})",
                f"One-date review ({len(one_date_records)})",
                f"Gaps ({len(result.get('gaps') or [])})",
                f"Conflicts ({len(result.get('conflicts') or [])})",
            ]
        )
        with rules_tabs[0]:
            if reusable_records:
                dataframe(pd.DataFrame(reusable_records), height=560)
            else:
                st.caption("No reusable filters were discovered.")
        with rules_tabs[1]:
            if one_date_records:
                dataframe(pd.DataFrame(one_date_records), height=480)
                pending = report.get("pending_rule_approvals") or []
                pending_by_signature = {
                    clean_text(item.get("logic_signature")): item
                    for item in pending
                }
                selected_approvals = st.multiselect(
                    "Approve one-date filters",
                    list(pending_by_signature),
                    format_func=lambda value: clean_text(
                        pending_by_signature[value].get("name")
                    ),
                    key=f"literal_pending_rules_{result.get('run_id')}",
                )
                if st.button(
                    "Rebuild candidate with selected approvals",
                    disabled=not selected_approvals,
                    key="literal_apply_rule_approvals",
                ):
                    try:
                        combined = sorted(
                            set(approved_signatures) | set(selected_approvals)
                        )
                        st.session_state[approval_key] = combined
                        with st.spinner(
                            "Revalidating the complete corpus with approvals..."
                        ):
                            rebuilt = execute_distillery(combined)
                        st.session_state["_literal_distillery_result"] = rebuilt
                        set_flash(
                            f"Applied {len(combined):,} explicit one-date "
                            "approval(s) and rebuilt the candidate."
                        )
                        safe_rerun()
                    except Exception as exc:
                        render_actionable_exception(
                            "The candidate could not be rebuilt with approvals.",
                            exc,
                            component="Distillery one-date approval",
                            context={"profile_id": profile_id},
                        )
            else:
                st.caption("No one-date filters require approval.")
        with rules_tabs[2]:
            gaps = result.get("gaps") or []
            if gaps:
                dataframe(pd.DataFrame(gaps), height=560)
            else:
                st.success("No unexplained rows remain.")
        with rules_tabs[3]:
            conflicts = result.get("conflicts") or []
            if conflicts:
                dataframe(pd.DataFrame(conflicts), height=560)
            else:
                st.success("No conflicting filters remain.")

        folds = holdout.get("folds") or {}
        if isinstance(folds, Mapping) and folds:
            with st.expander("Leave-one-date-out results", expanded=False):
                dataframe(
                    pd.DataFrame(
                        [
                            {
                                "Date / source": group,
                                "Testing rows": int(
                                    values.get("testing_rows") or 0
                                ),
                                "Reusable rules": int(
                                    values.get("rule_count") or 0
                                ),
                                "Accuracy": float(
                                    values.get("accuracy") or 0.0
                                ),
                                "Exact": int(values.get("exact") or 0),
                                "Uncovered": int(
                                    values.get("uncovered") or 0
                                ),
                                "Mismatched": int(
                                    values.get("mismatched") or 0
                                ),
                            }
                            for group, values in folds.items()
                        ]
                    )
                )
        with st.expander("Complete Distillery report", expanded=False):
            st.json(report)
            st.download_button(
                "Download validation report",
                data=json_dumps(report, pretty=True),
                file_name=(
                    "one_engine_distillery_"
                    f"{clean_text(result.get('run_id'))}.json"
                ),
                mime="application/json",
                key="literal_distillery_report_download",
            )

        st.markdown("#### Save immutable candidate")
        st.caption(
            "Saving creates a versioned sandbox. It does not change active "
            "rules, execute a batch, or update workflow rows."
        )
        if st.button(
            "Save candidate version to Snowflake",
            key="literal_save_candidate",
            disabled=not catalog,
        ):
            try:
                version = store.save_catalog_candidate(
                    catalog,
                    report,
                    result.get("gaps") or [],
                )
                st.session_state["_literal_selected_version"] = clean_text(
                    version.get("id")
                )
                set_flash(
                    f"Saved immutable {profile_id} catalog version "
                    f"{int(version.get('version_number') or 0)}."
                )
                safe_rerun()
            except Exception as exc:
                render_actionable_exception(
                    "The immutable candidate could not be saved.",
                    exc,
                    component="Distillery catalog version",
                    context={
                        "profile_id": profile_id,
                        "run_id": clean_text(result.get("run_id")),
                    },
                )
    else:
        st.caption(
            "Upload paired evidence and reconstruct literal filters. Existing "
            "saved versions remain available below."
        )

    st.divider()
    st.markdown("### Candidate testing, activation, and rollback")
    try:
        versions = store.list_catalog_versions(profile_id)
    except Exception as exc:
        render_actionable_exception(
            "Catalog versions could not be loaded.",
            exc,
            component="Distillery catalog versions",
            context={
                "profile_id": profile_id,
                "required_table": store.table("catalog_versions"),
            },
        )
        return
    if not versions:
        st.caption(
            "No catalog versions exist yet. Saving a candidate first captures "
            "the current Product Request catalog as the legacy rollback version."
        )
        return
    dataframe(
        pd.DataFrame(
            [
                {
                    "Version": int(item.get("version_number") or 0),
                    "Status": clean_text(item.get("status")),
                    "Rules": int(item.get("rule_count") or 0),
                    "Gaps": int(item.get("gap_count") or 0),
                    "Eligible": bool_value(item.get("deployment_eligible")),
                    "Created": timestamp_text(item.get("created_at")),
                    "Activated": timestamp_text(item.get("activated_at")),
                    "ID": clean_text(item.get("id")),
                }
                for item in versions
            ]
        ),
        height=min(430, 38 + len(versions) * 35),
    )
    version_by_id = {
        clean_text(item.get("id")): item for item in versions
    }
    preferred_version = clean_text(
        st.session_state.get("_literal_selected_version")
    )
    version_ids = list(version_by_id)
    selected_index = (
        version_ids.index(preferred_version)
        if preferred_version in version_ids
        else 0
    )
    selected_version_id = st.selectbox(
        "Catalog version",
        version_ids,
        index=selected_index,
        format_func=lambda value: (
            f"v{int(version_by_id[value].get('version_number') or 0)} - "
            f"{clean_text(version_by_id[value].get('status'))} - {value[:8]}"
        ),
        key="literal_version_select",
    )
    selected_version = version_by_id[selected_version_id]

    test_tab, activation_tab, gap_tab, history_tab = st.tabs(
        [
            "Isolated candidate test",
            "Activate / rollback",
            "Persistent gaps",
            "Audit history",
        ]
    )
    with test_tab:
        st.caption(
            "Active and selected catalogs run against cloned in-memory rows. "
            "No run, result, batch, or workflow-row record is written."
        )
        test_source = st.radio(
            "Test data",
            [
                "Existing batch",
                "Upload Product Request file",
                "Live Product Request data",
            ],
            horizontal=True,
            key="literal_test_source",
        )
        batches: list[dict[str, Any]] = []
        selected_batch_id = ""
        candidate_upload = None
        if test_source == "Existing batch":
            batches = store.list_batches()
            if batches:
                selected_batch_id = st.selectbox(
                    "Batch",
                    [clean_text(item.get("id")) for item in batches],
                    format_func=lambda value: next(
                        (
                            f"{clean_text(item.get('name'))} - "
                            f"{int(item.get('row_count') or 0):,} rows"
                        )
                        for item in batches
                        if clean_text(item.get("id")) == value
                    ),
                    key="literal_test_batch",
                )
            else:
                st.caption("No stored batches are available.")
        elif test_source == "Upload Product Request file":
            candidate_upload = st.file_uploader(
                "Product Request test file",
                type=["csv", "txt", "tsv", "xlsx", "xlsm"],
                key="literal_candidate_test_upload",
            )
        else:
            st.caption(
                f"Reads {TARGET_DATABASE}.{TARGET_SCHEMA}."
                f"{LIVE_PRODUCT_REQUEST_VIEW} once for this comparison."
            )
        can_test = (
            bool(selected_batch_id)
            if test_source == "Existing batch"
            else candidate_upload is not None
            if test_source == "Upload Product Request file"
            else True
        )
        if st.button(
            "Compare active vs selected version",
            type="primary",
            disabled=not can_test,
            key="literal_compare_candidate",
        ):
            try:
                if test_source == "Existing batch":
                    source_rows = store.load_rows(selected_batch_id)
                    source_label = next(
                        clean_text(item.get("name"))
                        for item in batches
                        if clean_text(item.get("id")) == selected_batch_id
                    )
                elif test_source == "Upload Product Request file":
                    parsed = parse_source_workbook(
                        clean_text(candidate_upload.name),
                        candidate_upload.getvalue(),
                    )
                    source_rows = workflow_rows_from_parsed(parsed)
                    source_label = clean_text(candidate_upload.name)
                else:
                    parsed, _, metadata = store.load_live_product_request_data()
                    source_rows = workflow_rows_from_parsed(
                        parsed,
                        "candidate-live-test",
                    )
                    source_label = clean_text(metadata.get("source_view"))
                comparison = compare_catalog_version(
                    store,
                    selected_version_id,
                    source_rows,
                    source_label=source_label,
                )
                st.session_state["_literal_distillery_comparison"] = comparison
                set_flash(
                    f"Compared {comparison['row_count']:,} rows without "
                    "changing active rules or stored data."
                )
                safe_rerun()
            except Exception as exc:
                render_actionable_exception(
                    "The isolated catalog comparison failed.",
                    exc,
                    component="Distillery candidate test",
                    context={
                        "catalog_version_id": selected_version_id,
                        "test_source": test_source,
                    },
                )
        comparison = st.session_state.get("_literal_distillery_comparison")
        if (
            isinstance(comparison, Mapping)
            and clean_text(comparison.get("catalog_version_id"))
            == selected_version_id
        ):
            comparison_metrics = st.columns(3)
            comparison_metrics[0].metric(
                "Rows", f"{int(comparison.get('row_count') or 0):,}"
            )
            comparison_metrics[1].metric(
                "Same atomic result",
                f"{int(comparison.get('same_count') or 0):,}",
            )
            comparison_metrics[2].metric(
                "Different",
                f"{int(comparison.get('different_count') or 0):,}",
            )
            records = comparison.get("records") or []
            differences = [
                row
                for row in records
                if not bool_value(row.get("Same atomic result"))
            ]
            dataframe(pd.DataFrame(differences or records), height=560)
    with activation_tab:
        status = clean_text(selected_version.get("status")).upper()
        eligible = bool_value(selected_version.get("deployment_eligible"))
        if status == "ACTIVE":
            st.success("This is the active materialized workflow catalog.")
        else:
            action_word = "Roll back" if status == "RETIRED" else "Activate"
            confirmation = st.checkbox(
                (
                    f"I confirm {action_word.lower()} to Product Request "
                    f"catalog version "
                    f"{int(selected_version.get('version_number') or 0)}."
                ),
                key=f"literal_activate_confirm_{selected_version_id}",
            )
            if status == "CANDIDATE" and not eligible:
                st.error(
                    "This candidate cannot activate because its saved "
                    "Distillery gate was not eligible."
                )
            if st.button(
                f"{action_word} selected catalog version",
                disabled=not confirmation
                or (status == "CANDIDATE" and not eligible),
                key=f"literal_activate_{selected_version_id}",
            ):
                try:
                    activated = store.activate_catalog_version(
                        selected_version_id
                    )
                    set_flash(
                        f"Product Request catalog version "
                        f"{int(activated.get('version_number') or 0)} is active."
                    )
                    safe_rerun()
                except Exception as exc:
                    render_actionable_exception(
                        "The catalog version could not be activated.",
                        exc,
                        component="Distillery activation",
                        context={
                            "catalog_version_id": selected_version_id,
                            "status": status,
                        },
                    )
    with gap_tab:
        version_gaps = store.list_distillery_gaps(selected_version_id)
        if version_gaps:
            dataframe(pd.DataFrame(version_gaps), height=520)
            open_gaps = [
                item
                for item in version_gaps
                if clean_text(item.get("status")).upper() != "RESOLVED"
            ]
            if open_gaps:
                gap_by_id = {
                    clean_text(item.get("id")): item for item in open_gaps
                }
                selected_gap_id = st.selectbox(
                    "Open gap",
                    list(gap_by_id),
                    format_func=lambda value: (
                        f"{clean_text(gap_by_id[value].get('source_group'))} - "
                        f"{clean_text(gap_by_id[value].get('pair_id'))}"
                    ),
                    key=f"literal_gap_select_{selected_version_id}",
                )
                resolution = st.text_area(
                    "Resolution note",
                    help=(
                        "Record why this evidence is explained or excluded. "
                        "Then rebuild the Distillery candidate; resolving a "
                        "gap never silently changes immutable rules."
                    ),
                    key=f"literal_gap_resolution_{selected_gap_id}",
                )
                if st.button(
                    "Resolve selected gap",
                    disabled=not clean_text(resolution),
                    key=f"literal_gap_resolve_{selected_gap_id}",
                ):
                    try:
                        store.resolve_distillery_gap(
                            selected_gap_id,
                            resolution,
                        )
                        set_flash(
                            "Resolved the selected gap. Rebuild a candidate "
                            "before activation."
                        )
                        safe_rerun()
                    except Exception as exc:
                        render_actionable_exception(
                            "The Distillery gap could not be resolved.",
                            exc,
                            component="Distillery gap queue",
                            context={
                                "gap_id": selected_gap_id,
                                "catalog_version_id": selected_version_id,
                            },
                        )
            else:
                st.success("Every persisted gap in this version is resolved.")
        else:
            st.success("This catalog version has no persisted gaps.")
    with history_tab:
        events = [
            event
            for event in store.list_audit(limit=500)
            if clean_text(event.get("entity_type")) == "catalog_version"
        ]
        if events:
            dataframe(pd.DataFrame(audit_table_records(events)), height=560)
        else:
            st.caption("No catalog-version audit events have been recorded.")


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
        "Audit Action": clean_text(row.get("audit_action")),
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


def one_engine_brand_image_path() -> str:
    """Find the optional Snowflake project asset across supported app roots."""
    file_path = clean_text(globals().get("__file__"))
    file_directory = os.path.dirname(os.path.abspath(file_path)) if file_path else ""
    try:
        working_directory = os.path.abspath(os.getcwd())
    except Exception:
        working_directory = ""
    roots: list[str] = []

    def add_root(value: str) -> None:
        normalized = os.path.normpath(value) if value else ""
        if normalized and normalized not in roots and os.path.isdir(normalized):
            roots.append(normalized)

    add_root(file_directory)
    add_root(working_directory)
    if file_directory:
        add_root(os.path.dirname(file_directory))
    if working_directory:
        add_root(os.path.join(working_directory, "app"))
        add_root(os.path.join(working_directory, "streamlit"))

    preferred = (
        "oneengine_brand.png",
        "one_engine_brand.png",
        "oneengine.png",
        "one_engine.png",
    )
    for root in roots:
        for name in preferred:
            candidate = os.path.join(root, name)
            if os.path.isfile(candidate):
                return os.path.abspath(candidate)
    for root in roots:
        try:
            for name in sorted(os.listdir(root)):
                lowered = name.lower()
                if (
                    "oneengine" in lowered.replace("_", "").replace("-", "")
                    and lowered.endswith((".png", ".jpg", ".jpeg", ".webp"))
                ):
                    candidate = os.path.join(root, name)
                    if os.path.isfile(candidate):
                        return os.path.abspath(candidate)
        except Exception:
            continue
    return ""


def render_live_build_proof(brand_image: str = "") -> None:
    """Render an unmistakable identity before Snowflake initialization."""
    require_streamlit()
    identity = source_code_fingerprint()
    brand_image = brand_image or one_engine_brand_image_path()
    if brand_image:
        logo_renderer = getattr(st, "logo", None)
        if callable(logo_renderer):
            try:
                logo_renderer(brand_image, icon_image=brand_image)
            except TypeError:
                logo_renderer(brand_image)
            except Exception:
                pass
        st.sidebar.image(brand_image, use_container_width=True)
    else:
        st.sidebar.warning(
            "Brand asset unavailable in this session. Add "
            "`oneengine_brand.png` beside `streamlit_app.py`, then restart "
            "the Snowflake Streamlit session."
        )
    st.sidebar.markdown(f"### {APP_TITLE}")
    st.sidebar.markdown(
        f'<div class="rules-live-badge">{xml_escape(LIVE_BUILD_BADGE)}</div>',
        unsafe_allow_html=True,
    )
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
    brand_image = one_engine_brand_image_path()
    st.set_page_config(
        page_title=APP_TITLE,
        page_icon=brand_image or "⚙️",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    app_styles()
    render_live_build_proof(brand_image)
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
        elif page == "Rules Distillery":
            render_rules_distillery_page(store)
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
