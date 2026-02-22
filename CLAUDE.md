# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

KNX Lens is a terminal UI (TUI) application for monitoring and analyzing KNX building automation bus traffic. It correlates raw KNX telegrams with ETS project structure to provide human-readable insight into bus activity.

## Running the Application

```bash
# First-time setup wizard (discovers gateway, writes .env)
python setup.py

# Start bus logger (captures telegrams from KNX/IP gateway to log file)
python knx-lens-logger.py
python knx-lens-logger.py --daemon   # background mode

# Launch interactive TUI explorer
python knx-lens.py
python knx-lens.py -v                  # verbose/debug logging to console + knx_app_debug.log
python knx-lens.py /path/to/log.zip   # load a specific log file

# Serve TUI over HTTP (port 8000, uses textual-serve)
python knx-lens-web.py

# Docker
docker compose up
```

There are no build steps, linters, or automated tests. All runtime config lives in `.env` (see `.env.example`).

## Architecture

The project splits into four layers:

**Data acquisition** — `knx-lens-logger.py` connects to a KNX/IP gateway via `xknx`, decodes telegrams using the ETS project file, and writes rotating pipe-separated log files (`knx_bus.log`).

**Data access utilities** — Two modules handle parsing and caching:
- `knx_log_utils.py` — reads and caches log files (pipe-separated or CSV), supports incremental append and `.zip` archives
- `knx_project_utils.py` — parses `.knxproj` files via `xknxproject`, caches results using MD5-based invalidation, builds three tree structures: Building (Floor→Room→Device), Physical Address (line topology), and Group Address (functional)

**Business logic** — `knx_tui_logic.py` is a **mixin class** (`KNXTuiLogic`) that is combined with the Textual `App` class in `knx-lens.py`. It handles all non-UI concerns: filter state, selection groups, log reloading, payload history, statistics, and time-range filtering. When modifying app behavior, changes typically belong here rather than in `knx-lens.py`.

**UI layer** — `knx-lens.py` defines the Textual app (`KNXLens(KNXTuiLogic, App)`), its tabs (Building, Topology, Group Addresses, Selection Groups, Statistics, Log View, Files), keyboard bindings, and widget event handlers. `knx_tui_screens.py` provides modal dialogs (file browser, text input, time range picker). Styling is in `knx-lens.css`.

**Web wrapper** — `knx-lens-web.py` uses `textual-serve` to expose the TUI over HTTP. No application logic lives here.

## Key Concepts

**Named filters** (`named_filters.yaml`) — User-defined selection groups combining group addresses and regex rules, persisted to a fixed path set in `.env` (`NAMED_FILTERS_PATH`). Managed entirely in `knx_tui_logic.py`.

```yaml
My Group:
  gas: ["1/1/1", "1/1/2"]
  rules:
    - rule_name: "Label"
      pattern: ".*Floor 1.*"
```

**Filtering precedence** (in `_filter_log_data()`):
1. If any named filter (selection group) is active → row must match ≥1 group's GAs or regex rules (**OR** across groups)
2. If global `regex_filter` is set → row must also match (**AND** with step 1)
3. Time filter applied last

**Caching** — Project data is cached in `*.cache.json` files alongside the `.knxproj`, invalidated by MD5 hash. Log data is cached in memory (default 10 000 lines, configurable via `MAX_LOG_LINES`). Deleting cache files forces a full re-parse.

**Payload history** — The logic layer tracks up to 3 previous values per group address so the UI can show state-change context in the log view.

**Tree data format** — All three tree views (Building, Physical, Group Address) use the same dict shape built by `knx_project_utils.py`:
```python
{
    "children": {...},       # nested subtree (dict, not list)
    "com_objects": [...],    # GAs at this node
    "last_value": "...",     # latest payload for live display
    "node_id": "unique_key"
}
```

**Project data wrapper** — `load_or_parse_project()` returns `{"md5": "...", "project": {...}}`. Some functions expect the full wrapper; others expect `project_data["project"]` (unwrapped). Check call sites in `knx_log_utils.py` (~line 25) when passing project data between modules.

**Log format** — Pipe-separated columns: `timestamp | source_pa | dest_pa | group_addr | decoded_value | raw_payload`. The parser in `knx_log_utils.py` handles both this format and legacy CSV via heuristic detection.

## Important Pitfalls

1. **Windows paths**: Always use forward slashes in `.env` paths. Backslashes break ZIP detection logic.
2. **CO ordering**: When adding remaining (non-channel) communication objects to a tree node, sort by CO number **before** calling `add_com_objects_to_node()`: `sorted(rem_ids, key=lambda x: com_objects_dict.get(x, {}).get('number', 0))`. Converting a `set` to a `list` without sorting causes unpredictable display order.
3. **Log rotation timing**: `ZipTimedRotatingFileHandler` rotates at **midnight UTC**, not local time. Set `TZ` env var in Docker if local-time rotation matters.
4. **Gateway scanner**: Linux-only via XKNX. Windows/macOS must enter the gateway IP manually; setup falls back gracefully.
5. **Stale cache**: If a `.knxproj` file is replaced and the MD5 changes, the `.cache.json` auto-invalidates. If parsing seems wrong, delete the cache file manually to force a fresh parse.
6. **Textual modals**: Custom screens dismiss with `self.dismiss(result)`, triggering the app's registered result callback — not a direct return value.

## Dependencies

```
textual          # TUI framework
xknx             # KNX/IP gateway communication
xknxproject      # .knxproj file parsing
textual-serve    # HTTP wrapper for TUI (web mode)
python-dotenv    # .env config loading
PyYAML           # named_filters.yaml persistence
```

Install: `pip install -r requirements.txt` (use a virtualenv).

## Docker

`docker-compose.yaml` runs two services: `knx-lens-logger` and `knx-lens-web`. Use `.env.docker.example` as the config template. The logger writes logs to a shared volume that the web service reads.
