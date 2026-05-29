from __future__ import annotations

import json
import html
import mimetypes
import os
import re
import sqlite3
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from loguru import logger


ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
DB_PATH = DATA_DIR / "agent.sqlite3"
LOG_DIR = DATA_DIR / "logs"


MANAGER_IDS = {
    2328073: "Павел",
    3298921: "Ольга Шевелева",
    7074220: "Дегтярева Юлия",
    8476783: "Юлия Миллер",
}

MANAGER_ALIASES = {
    "павел": 2328073,
    "карепов": 2328073,
    "ольга шевелева": 3298921,
    "шевелева": 3298921,
    "дегтярева юлия": 7074220,
    "дегтярева": 7074220,
    "юлия миллер": 8476783,
    "миллер": 8476783,
}

DEFAULT_PIPELINE_IDS = {
    867829: "Отдел продаж",
    1312204: "Юридический отдел",
}


def load_env() -> dict[str, str]:
    values: dict[str, str] = dict(os.environ)
    for path in [ROOT.parent / ".env", ROOT / ".env"]:
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            match = re.match(r"\s*([^#=]+)=(.*)$", line)
            if match:
                values[match.group(1).strip()] = match.group(2).strip()
    return values


ENV = load_env()


def setup_logging() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger.remove()
    logger.add(
        sys.stderr,
        level=ENV.get("LOG_LEVEL", "DEBUG"),
        colorize=True,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
        ),
    )
    logger.add(
        LOG_DIR / "agent_{time:YYYY-MM-DD}.log",
        level="DEBUG",
        rotation="00:00",
        retention="14 days",
        encoding="utf-8",
        enqueue=True,
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {function}:{line} - {message}",
    )


def env_int(name: str, default: int) -> int:
    raw = ENV.get(name, "").strip()
    return int(raw) if raw else default


def require_env(name: str) -> str:
    value = ENV.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def kras_now() -> datetime:
    return datetime.now(timezone(timedelta(hours=7)))


def day_range(day: str) -> tuple[int, int, str]:
    now = kras_now()
    if day == "today":
        target = now.date()
    elif day == "yesterday":
        target = (now - timedelta(days=1)).date()
    else:
        raise ValueError(f"Unknown day: {day}")
    start = datetime(target.year, target.month, target.day, tzinfo=now.tzinfo)
    end = start + timedelta(days=1)
    return int(start.timestamp()), int(end.timestamp()), target.isoformat()


def combined_range(days: list[str]) -> tuple[int, int, str]:
    ranges = [day_range(day) for day in days]
    return min(r[0] for r in ranges), max(r[1] for r in ranges), "+".join(r[2] for r in ranges)


def safe_name(value: str) -> str:
    value = re.sub(r'[<>:"/\\|?*\s]+', "_", value.strip())
    return value.strip("._") or "item"


class Store:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.lock = threading.RLock()
        self._init_db()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                create table if not exists calls (
                    note_id integer primary key,
                    lead_id integer not null,
                    lead_name text,
                    lead_url text,
                    pipeline_status text,
                    manager_id integer,
                    manager_name text,
                    call_time integer,
                    duration integer,
                    note_type text,
                    audio_url text,
                    audio_path text,
                    transcript_path text,
                    analysis_path text,
                    is_substantive integer,
                    status text not null,
                    error text,
                    created_at integer not null
                );

                create table if not exists settings (
                    key text primary key,
                    value text not null
                );
                """
            )
            existing_columns = {
                row["name"] for row in conn.execute("pragma table_info(calls)").fetchall()
            }
            for column, definition in {
                "lead_name": "text",
                "lead_url": "text",
                "pipeline_status": "text",
            }.items():
                if column not in existing_columns:
                    conn.execute(f"alter table calls add column {column} {definition}")

    def get_setting(self, key: str, default: str = "") -> str:
        with self.lock, self.connect() as conn:
            row = conn.execute("select value from settings where key = ?", (key,)).fetchone()
            return str(row["value"]) if row else default

    def set_setting(self, key: str, value: str) -> None:
        with self.lock, self.connect() as conn:
            conn.execute(
                "insert into settings(key, value) values(?, ?) on conflict(key) do update set value = excluded.value",
                (key, value),
            )

    def has_processed_call(self, note_id: int) -> bool:
        with self.lock, self.connect() as conn:
            row = conn.execute("select 1 from calls where note_id = ?", (note_id,)).fetchone()
            return row is not None

    def lead_has_substantive(self, lead_id: int) -> bool:
        with self.lock, self.connect() as conn:
            row = conn.execute(
                "select 1 from calls where lead_id = ? and is_substantive = 1 and status = 'done'",
                (lead_id,),
            ).fetchone()
            return row is not None

    def save_call(
        self,
        candidate: "CallCandidate",
        status: str,
        audio_path: Path | None = None,
        transcript_path: Path | None = None,
        analysis_path: Path | None = None,
        is_substantive: bool | None = None,
        error: str | None = None,
    ) -> None:
        with self.lock, self.connect() as conn:
            conn.execute(
                """
                insert into calls(
                    note_id, lead_id, lead_name, lead_url, pipeline_status,
                    manager_id, manager_name, call_time, duration, note_type,
                    audio_url, audio_path, transcript_path, analysis_path, is_substantive,
                    status, error, created_at
                )
                values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                on conflict(note_id) do update set
                    lead_name = excluded.lead_name,
                    lead_url = excluded.lead_url,
                    pipeline_status = excluded.pipeline_status,
                    manager_id = excluded.manager_id,
                    manager_name = excluded.manager_name,
                    call_time = excluded.call_time,
                    duration = excluded.duration,
                    note_type = excluded.note_type,
                    audio_url = excluded.audio_url,
                    audio_path = excluded.audio_path,
                    transcript_path = excluded.transcript_path,
                    analysis_path = excluded.analysis_path,
                    is_substantive = excluded.is_substantive,
                    status = excluded.status,
                    error = excluded.error
                """,
                (
                    candidate.note_id,
                    candidate.lead_id,
                    candidate.lead_name,
                    candidate.lead_url,
                    candidate.pipeline_status,
                    candidate.manager_id,
                    candidate.manager_name,
                    candidate.call_time,
                    candidate.duration,
                    candidate.note_type,
                    candidate.audio_url,
                    str(audio_path) if audio_path else None,
                    str(transcript_path) if transcript_path else None,
                    str(analysis_path) if analysis_path else None,
                    None if is_substantive is None else int(is_substantive),
                    status,
                    error,
                    int(time.time()),
                ),
            )

    def get_call(self, note_id: int) -> dict[str, Any] | None:
        with self.lock, self.connect() as conn:
            row = conn.execute("select * from calls where note_id = ?", (note_id,)).fetchone()
            return dict(row) if row else None

    def list_done_calls(
        self,
        manager_id: int | None = None,
        lead_id: int | None = None,
        limit: int = 30,
    ) -> list[dict[str, Any]]:
        clauses = ["status = 'done'", "analysis_path is not null"]
        params: list[Any] = []
        if manager_id is not None:
            clauses.append("manager_id = ?")
            params.append(manager_id)
        if lead_id is not None:
            clauses.append("lead_id = ?")
            params.append(lead_id)
        params.append(limit)
        with self.lock, self.connect() as conn:
            rows = conn.execute(
                f"""
                select * from calls
                where {' and '.join(clauses)}
                order by call_time desc, created_at desc
                limit ?
                """,
                params,
            ).fetchall()
            return [dict(row) for row in rows]

    def manager_archive_counts(self) -> list[dict[str, Any]]:
        with self.lock, self.connect() as conn:
            rows = conn.execute(
                """
                select manager_id, manager_name, count(*) as total, max(call_time) as last_call_time
                from calls
                where status = 'done' and analysis_path is not null
                group by manager_id, manager_name
                order by manager_name
                """
            ).fetchall()
            return [dict(row) for row in rows]

    def stats(self) -> dict[str, Any]:
        with self.lock, self.connect() as conn:
            total = conn.execute("select count(*) as c from calls").fetchone()["c"]
            done = conn.execute("select count(*) as c from calls where status = 'done'").fetchone()["c"]
            errors = conn.execute("select count(*) as c from calls where status = 'error'").fetchone()["c"]
            substantive = conn.execute(
                "select count(*) as c from calls where is_substantive = 1 and status = 'done'"
            ).fetchone()["c"]
            last = conn.execute(
                "select note_id, lead_id, manager_name, call_time, status from calls order by created_at desc limit 5"
            ).fetchall()
        return {
            "total": total,
            "done": done,
            "errors": errors,
            "substantive": substantive,
            "last": [dict(row) for row in last],
        }


@dataclass(frozen=True)
class LeadInfo:
    lead_id: int
    name: str
    pipeline_id: int
    pipeline_name: str
    status_id: int
    responsible_user_id: int
    manager_field_id: int | None
    manager_field_name: str | None
    status_name: str
    contact_ids: list[int]


@dataclass(frozen=True)
class CallCandidate:
    note_id: int
    lead_id: int
    lead_name: str
    lead_url: str
    manager_id: int
    manager_name: str
    call_time: int
    duration: int
    note_type: str
    audio_url: str
    phone: str
    source: str
    pipeline_status: str
    note_entity_type: str
    note_entity_id: int


class AmoClient:
    def __init__(self, base_url: str, token: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {token}"})
        self.sales_pipeline_id = env_int("AMOCRM_SALES_PIPELINE_ID", 867829)
        self.legal_pipeline_id = env_int("AMOCRM_LEGAL_PIPELINE_ID", 1312204)
        self.pipeline_ids = self._load_pipeline_ids()
        self.cost_status_id = env_int("AMOCRM_COST_STATUS_ID", 25039045)
        self._pipeline_names: dict[int, str] = {}
        self._status_names: dict[tuple[int, int], str] = {}
        self._pipeline_status_ids: dict[int, set[int]] = {}
        self._user_names: dict[int, str] = dict(MANAGER_IDS)

    def _load_pipeline_ids(self) -> list[int]:
        raw = ENV.get("AMOCRM_PIPELINE_IDS", "").strip()
        if raw:
            ids = [int(item.strip()) for item in raw.split(",") if item.strip()]
        else:
            ids = [self.sales_pipeline_id, self.legal_pipeline_id]
        return list(dict.fromkeys(ids))

    def _get(self, path: str, params: list[tuple[str, str]] | dict[str, Any] | None = None) -> Any:
        logger.debug("amoCRM GET {} params={}", path, params)
        last_error: Exception | None = None
        for attempt in range(1, 4):
            try:
                response = self.session.get(f"{self.base_url}{path}", params=params, timeout=40)
                logger.debug("amoCRM response {} {} bytes for {}", response.status_code, len(response.content), path)
                if response.status_code == 204:
                    return {}
                response.raise_for_status()
                return json.loads(response.content.decode("utf-8"))
            except requests.RequestException as exc:
                last_error = exc
                logger.warning("amoCRM GET failed attempt {}/3 for {}: {}", attempt, path, exc)
                if attempt < 3:
                    time.sleep(1.5 * attempt)
        raise last_error or RuntimeError(f"amoCRM GET failed: {path}")

    def load_pipeline_statuses(self) -> dict[int, set[int]]:
        logger.info(
            "Loading target pipeline statuses: pipeline_ids={}",
            self.pipeline_ids,
        )
        payload = self._get("/api/v4/leads/pipelines")
        found_pipeline_ids: set[int] = set()
        for pipeline in payload.get("_embedded", {}).get("pipelines", []):
            pipeline_id = int(pipeline.get("id", 0))
            if pipeline_id not in self.pipeline_ids:
                continue
            found_pipeline_ids.add(pipeline_id)
            self._pipeline_names[pipeline_id] = str(pipeline.get("name") or pipeline_id)
            statuses = pipeline.get("_embedded", {}).get("statuses", [])
            self._pipeline_status_ids[pipeline_id] = {int(status["id"]) for status in statuses}
            for status in statuses:
                status_id = int(status["id"])
                self._status_names[(pipeline_id, status_id)] = str(status["name"])
        missing = [pipeline_id for pipeline_id in self.pipeline_ids if pipeline_id not in found_pipeline_ids]
        if missing:
            raise RuntimeError(f"Target pipelines were not found: {missing}")
        logger.info(
            "Allowed statuses by pipeline: {}",
            {
                self.pipeline_name(pipeline_id): [
                    f"{status_id}:{self.status_name(pipeline_id, status_id)}"
                    for status_id in sorted(status_ids)
                ]
                for pipeline_id, status_ids in self._pipeline_status_ids.items()
            },
        )
        return self._pipeline_status_ids

    def pipeline_name(self, pipeline_id: int) -> str:
        return self._pipeline_names.get(pipeline_id, DEFAULT_PIPELINE_IDS.get(pipeline_id, str(pipeline_id)))

    def status_name(self, pipeline_id: int, status_id: int) -> str:
        if not self._status_names:
            self.load_pipeline_statuses()
        return self._status_names.get((pipeline_id, status_id), str(status_id))

    def user_name(self, user_id: int) -> str:
        if user_id in self._user_names:
            return self._user_names[user_id]
        try:
            payload = self._get(f"/api/v4/users/{user_id}")
            name = str(payload.get("name") or user_id)
        except Exception as exc:
            logger.warning("Could not load amoCRM user {}: {}", user_id, exc)
            name = str(user_id)
        self._user_names[user_id] = name
        return name

    def manager_from_lead_field(self, lead: dict[str, Any]) -> tuple[int | None, str | None]:
        for field in lead.get("custom_fields_values") or []:
            if str(field.get("field_name") or "").strip().lower() != "менеджер":
                continue
            for value in field.get("values") or []:
                raw = str(value.get("value") or "").strip()
                manager_id = MANAGER_ALIASES.get(raw.lower())
                if manager_id:
                    return manager_id, MANAGER_IDS[manager_id]
                if raw:
                    return None, raw
        return None, None

    def resolve_call_manager_id(self, note: dict[str, Any], lead: LeadInfo, audio_url: str) -> int | None:
        created_by = int(note.get("created_by") or 0)
        if created_by in MANAGER_IDS:
            return created_by
        if lead.manager_field_id in MANAGER_IDS:
            return lead.manager_field_id

        haystack = " ".join(
            [
                str(lead.manager_field_name or ""),
                audio_url,
                json.dumps(note.get("params") or {}, ensure_ascii=False),
            ]
        ).lower()
        for alias, manager_id in MANAGER_ALIASES.items():
            if alias in haystack:
                return manager_id

        note_responsible = int(note.get("responsible_user_id") or 0)
        if note_responsible in MANAGER_IDS:
            return note_responsible
        if lead.responsible_user_id in MANAGER_IDS:
            return lead.responsible_user_id
        return note_responsible or lead.responsible_user_id or None

    def list_candidate_leads(self, start_ts: int, end_ts: int) -> list[LeadInfo]:
        if not self._pipeline_status_ids:
            self.load_pipeline_statuses()

        leads: list[LeadInfo] = []
        logger.info(
            "Searching leads: date_range={}..{}, pipeline_ids={}, sales_managers={}, status_scope=all_statuses",
            fmt_dt(start_ts),
            fmt_dt(end_ts),
            self.pipeline_ids,
            MANAGER_IDS,
        )
        for target_pipeline_id in self.pipeline_ids:
            if target_pipeline_id == self.sales_pipeline_id:
                query_scopes = [("updated_at", start_ts - 86400, end_ts + 86400)]
            else:
                query_scopes = [
                    ("updated_at", start_ts, end_ts),
                    ("created_at", start_ts, end_ts),
                ]
            seen_lead_ids: set[int] = set()
            for time_field, from_ts, to_ts in query_scopes:
                page = 1
                while True:
                    params: list[tuple[str, str]] = [("limit", "250"), ("page", str(page)), ("with", "contacts")]
                    params.append(("filter[pipeline_id]", str(target_pipeline_id)))
                    params.append((f"filter[{time_field}][from]", str(from_ts)))
                    params.append((f"filter[{time_field}][to]", str(to_ts)))
                    if target_pipeline_id == self.sales_pipeline_id:
                        for index, user_id in enumerate(MANAGER_IDS):
                            params.append((f"filter[responsible_user_id][{index}]", str(user_id)))

                    payload = self._get("/api/v4/leads", params=params)
                    page_leads = payload.get("_embedded", {}).get("leads", [])
                    logger.info(
                        "Pipeline {} {} page {} returned {} raw leads",
                        self.pipeline_name(target_pipeline_id),
                        time_field,
                        page,
                        len(page_leads),
                    )
                    for lead in page_leads:
                        lead_id = int(lead["id"])
                        if lead_id in seen_lead_ids:
                            continue
                        seen_lead_ids.add(lead_id)
                        pipeline_id = int(lead.get("pipeline_id", 0))
                        status_id = int(lead.get("status_id", 0))
                        responsible = int(lead.get("responsible_user_id", 0))
                        manager_field_id, manager_field_name = self.manager_from_lead_field(lead)
                        if pipeline_id != target_pipeline_id:
                            logger.debug("Skip lead {}: pipeline_id {}", lead.get("id"), pipeline_id)
                            continue
                        if status_id not in self._pipeline_status_ids.get(pipeline_id, set()):
                            logger.debug("Skip lead {}: status_id {}", lead.get("id"), status_id)
                            continue
                        if pipeline_id == self.sales_pipeline_id and responsible not in MANAGER_IDS:
                            logger.debug("Skip sales lead {}: responsible_user_id {}", lead.get("id"), responsible)
                            continue
                        if pipeline_id != self.sales_pipeline_id and (
                            manager_field_id not in MANAGER_IDS and responsible not in MANAGER_IDS
                        ):
                            logger.debug(
                                "Skip non-sales lead {}: manager_field={} responsible_user_id={}",
                                lead.get("id"),
                                manager_field_name,
                                responsible,
                            )
                            continue
                        logger.debug(
                            "Candidate lead {} accepted: pipeline={} status={} {}, responsible={} {}, manager_field={}, contacts={}, name={}",
                            lead.get("id"),
                            self.pipeline_name(pipeline_id),
                            status_id,
                            self.status_name(pipeline_id, status_id),
                            responsible,
                            self.user_name(responsible),
                            manager_field_name,
                            [c.get("id") for c in lead.get("_embedded", {}).get("contacts", [])],
                            lead.get("name"),
                        )
                        leads.append(
                            LeadInfo(
                                lead_id=lead_id,
                                name=str(lead.get("name") or ""),
                                pipeline_id=pipeline_id,
                                pipeline_name=self.pipeline_name(pipeline_id),
                                status_id=status_id,
                                responsible_user_id=responsible,
                                manager_field_id=manager_field_id,
                                manager_field_name=manager_field_name,
                                status_name=self.status_name(pipeline_id, status_id),
                                contact_ids=[
                                    int(contact["id"])
                                    for contact in lead.get("_embedded", {}).get("contacts", [])
                                    if contact.get("id")
                                ],
                            )
                        )

                    if "next" not in payload.get("_links", {}):
                        break
                    page += 1
        logger.info("Accepted leads after filters: {}", len(leads))
        return leads

    def list_notes(
        self,
        entity_type: str,
        entity_id: int,
        start_ts: int | None = None,
        end_ts: int | None = None,
    ) -> list[dict[str, Any]]:
        notes: list[dict[str, Any]] = []
        page = 1
        logger.debug("Loading notes for {} {}", entity_type, entity_id)
        while True:
            params: dict[str, Any] = {"limit": 250, "page": page}
            if start_ts is not None:
                params["filter[created_at][from]"] = start_ts
            if end_ts is not None:
                params["filter[created_at][to]"] = end_ts
            payload = self._get(f"/api/v4/{entity_type}/{entity_id}/notes", params=params)
            page_notes = payload.get("_embedded", {}).get("notes", [])
            logger.debug("{} {} notes page {} returned {}", entity_type, entity_id, page, len(page_notes))
            notes.extend(page_notes)
            if not page_notes or "next" not in payload.get("_links", {}):
                break
            page += 1
        logger.debug("{} {} total notes loaded: {}", entity_type, entity_id, len(notes))
        return notes

    def find_call_candidates(self, start_ts: int, end_ts: int, store: Store) -> list[CallCandidate]:
        min_duration = env_int("CALL_MIN_DURATION_SECONDS", 300)
        leads = self.list_candidate_leads(start_ts, end_ts)
        candidates: list[CallCandidate] = []
        reject_counts: dict[str, int] = {}

        def reject(reason: str, detail: str) -> None:
            reject_counts[reason] = reject_counts.get(reason, 0) + 1
            logger.debug("Reject call candidate: {} | {}", reason, detail)

        for lead in leads:
            if store.lead_has_substantive(lead.lead_id):
                logger.info("Skip lead {}: already has substantive processed call", lead.lead_id)
                continue
            logger.info(
                "Inspecting lead {} | {} | status={} | manager={} | contacts={}",
                lead.lead_id,
                lead.name,
                lead.status_name,
                MANAGER_IDS.get(lead.responsible_user_id, lead.responsible_user_id),
                lead.contact_ids,
            )
            note_sources: list[tuple[str, int]] = (
                [("contacts", contact_id) for contact_id in lead.contact_ids]
                if lead.contact_ids
                else [("leads", lead.lead_id)]
            )
            seen_notes: set[int] = set()
            for entity_type, entity_id in note_sources:
                logger.info("Inspecting notes source: lead={} source={}/{}", lead.lead_id, entity_type, entity_id)
                for note in self.list_notes(entity_type, entity_id, start_ts, end_ts):
                    note_id = int(note.get("id") or 0)
                    if note_id in seen_notes:
                        reject("duplicate_note", f"lead={lead.lead_id} note={note_id} source={entity_type}/{entity_id}")
                        continue
                    seen_notes.add(note_id)
                    self._inspect_note_for_candidate(
                        note=note,
                        lead=lead,
                        start_ts=start_ts,
                        end_ts=end_ts,
                        min_duration=min_duration,
                        store=store,
                        candidates=candidates,
                        reject=reject,
                        entity_type=entity_type,
                        entity_id=entity_id,
                    )

        candidates.sort(key=lambda item: (item.lead_id, item.call_time))
        first_per_lead: dict[int, CallCandidate] = {}
        for candidate in candidates:
            if candidate.lead_id in first_per_lead:
                reject("not_first_long_call_in_lead", f"lead={candidate.lead_id} note={candidate.note_id}")
                continue
            first_per_lead[candidate.lead_id] = candidate
        result = sorted(first_per_lead.values(), key=lambda item: item.call_time)
        logger.info(
            "Call search summary: leads={}, raw_candidates={}, final_candidates={}, rejects={}",
            len(leads),
            len(candidates),
            len(result),
            reject_counts,
        )
        return result

    def _inspect_note_for_candidate(
        self,
        note: dict[str, Any],
        lead: LeadInfo,
        start_ts: int,
        end_ts: int,
        min_duration: int,
        store: Store,
        candidates: list[CallCandidate],
        reject: Any,
        entity_type: str,
        entity_id: int,
    ) -> None:
                note_type = str(note.get("note_type") or "")
                if note_type not in {"call_in", "call_out"}:
                    reject(
                        "not_call_note",
                        f"lead={lead.lead_id} note={note.get('id')} source={entity_type}/{entity_id} type={note_type}",
                    )
                    return
                params = note.get("params") or {}
                call_time = int(note.get("created_at") or 0)
                duration = int(params.get("duration") or 0)
                link = str(params.get("link") or "").strip()
                created_by = int(note.get("created_by") or 0)
                note_responsible = int(note.get("responsible_user_id") or 0)
                manager_id = self.resolve_call_manager_id(note, lead, link)
                manager_name = self.user_name(manager_id) if manager_id else "не указан"
                logger.debug(
                    "Call note seen: lead={} note={} type={} source={}/{} time={} duration={} link={} created_by={} note_resp={} lead_resp={} manager_field={} final_manager={}",
                    lead.lead_id,
                    note.get("id"),
                    note_type,
                    entity_type,
                    entity_id,
                    fmt_dt(call_time) if call_time else "no_time",
                    duration,
                    bool(link),
                    created_by,
                    note_responsible,
                    lead.responsible_user_id,
                    lead.manager_field_id or lead.manager_field_name,
                    manager_id,
                )
                if manager_id not in MANAGER_IDS:
                    reject(
                        "wrong_manager",
                        f"lead={lead.lead_id} note={note.get('id')} source={entity_type}/{entity_id} manager={manager_id}",
                    )
                    return
                if not (start_ts <= call_time < end_ts):
                    reject(
                        "outside_date_range",
                        f"lead={lead.lead_id} note={note.get('id')} source={entity_type}/{entity_id} call_time={fmt_dt(call_time)} range={fmt_dt(start_ts)}..{fmt_dt(end_ts)}",
                    )
                    return
                if duration < min_duration:
                    reject(
                        "too_short",
                        f"lead={lead.lead_id} note={note.get('id')} source={entity_type}/{entity_id} duration={duration} min={min_duration}",
                    )
                    return
                if not link:
                    reject(
                        "no_recording_link",
                        f"lead={lead.lead_id} note={note.get('id')} source={entity_type}/{entity_id} duration={duration}",
                    )
                    return
                if store.has_processed_call(int(note["id"])):
                    reject(
                        "already_processed",
                        f"lead={lead.lead_id} note={note.get('id')} source={entity_type}/{entity_id}",
                    )
                    return
                logger.info(
                    "Call candidate accepted: lead={} note={} note_source={}/{} manager={} time={} duration={} status={} source={}",
                    lead.lead_id,
                    note.get("id"),
                    entity_type,
                    entity_id,
                    manager_name,
                    fmt_dt(call_time),
                    duration,
                    f"{lead.pipeline_name} / {lead.status_name}",
                    params.get("source"),
                )
                candidates.append(
                    CallCandidate(
                        note_id=int(note["id"]),
                        lead_id=lead.lead_id,
                        lead_name=lead.name,
                        lead_url=f"{self.base_url}/leads/detail/{lead.lead_id}",
                        manager_id=manager_id,
                        manager_name=manager_name,
                        call_time=call_time,
                        duration=duration,
                        note_type=note_type,
                        audio_url=link,
                        phone=str(params.get("phone") or ""),
                        source=str(params.get("source") or ""),
                        pipeline_status=f"{lead.pipeline_name} / {lead.status_name}",
                        note_entity_type=entity_type,
                        note_entity_id=entity_id,
                    )
                )

    def download_audio(self, candidate: CallCandidate) -> Path:
        call_day = datetime.fromtimestamp(candidate.call_time, timezone(timedelta(hours=7))).date().isoformat()
        out_dir = DATA_DIR / "audio" / call_day
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / f"{candidate.lead_id}_{candidate.note_id}.mp3"
        if path.exists() and path.stat().st_size > 0:
            logger.info("Audio already exists: {}", path)
            return path

        # Use a clean request here so the amoCRM Authorization header is never sent to telephony hosts.
        logger.info("Downloading audio: lead={} note={} url={}", candidate.lead_id, candidate.note_id, candidate.audio_url)
        response = requests.get(candidate.audio_url, timeout=180, allow_redirects=True)
        logger.info(
            "Audio download response: status={} content_type={} bytes={}",
            response.status_code,
            response.headers.get("content-type"),
            len(response.content),
        )
        response.raise_for_status()
        content_type = response.headers.get("content-type", "")
        suffix = mimetypes.guess_extension(content_type.split(";")[0].strip()) or ".mp3"
        if suffix not in {".mp3", ".mpeg", ".wav", ".m4a", ".ogg", ".oga", ".webm"}:
            suffix = ".mp3"
        path = path.with_suffix(suffix)
        path.write_bytes(response.content)
        logger.info("Audio saved: {}", path)
        return path


class AIClient:
    def __init__(self, store: Store) -> None:
        self.store = store
        self.openai_transcribe_model = ENV.get("OPENAI_TRANSCRIBE_MODEL", "gpt-4o-mini-transcribe")
        self.openai_analysis_model = ENV.get("OPENAI_ANALYSIS_MODEL", "gpt-4o-mini")
        self.groq_transcribe_model = ENV.get("GROQ_TRANSCRIBE_MODEL", "whisper-large-v3-turbo")
        self.groq_analysis_model = ENV.get(
            "GROQ_ANALYSIS_MODEL",
            ENV.get("GROQ_MODEL", "llama-3.3-70b-versatile"),
        )
        self.freellm_analysis_model = ENV.get(
            "FREELLM_ANALYSIS_MODEL",
            ENV.get("FREELLM_MODEL", ENV.get("LLM_MODEL", "llama-3.3-70b-versatile")),
        )

    def mode(self) -> str:
        return self.store.get_setting("ai_mode", ENV.get("AI_MODE", "test")).lower()

    def set_mode(self, mode: str) -> None:
        if mode not in {"test", "paid"}:
            raise ValueError("mode must be test or paid")
        self.store.set_setting("ai_mode", mode)

    def provider_summary(self) -> dict[str, str]:
        mode = self.mode()
        if mode == "paid":
            transcriber = f"openai/{self.openai_transcribe_model}"
            analyzer = f"openai/{self.openai_analysis_model}"
        else:
            transcriber = f"groq/{self.groq_transcribe_model}"
            analyzer = f"groq/{self.groq_analysis_model} -> freellm/{self.freellm_analysis_model}"
        return {"mode": mode, "transcriber": transcriber, "analyzer": analyzer}

    def transcribe(self, audio_path: Path) -> str:
        if self.mode() == "paid":
            api_key = require_env("OPENAI_API_KEY")
            url = "https://api.openai.com/v1/audio/transcriptions"
            model = self.openai_transcribe_model
            service_name = "OpenAI"
        else:
            api_key = require_env("GROQ_API_KEY")
            url = "https://api.groq.com/openai/v1/audio/transcriptions"
            model = self.groq_transcribe_model
            service_name = "Groq"

        logger.info("Transcribing audio via {} model={} file={}", service_name, model, audio_path)
        data = {
            "model": model,
            "language": "ru",
            "response_format": "json",
            "temperature": "0",
            "prompt": (
                "Телефонный разговор менеджера компании Синай с клиентом по банкротству физических лиц. "
                "Важно точно распознать стоимость, договор, оплату, рассрочку, следующий шаг и возражения."
            ),
        }
        with audio_path.open("rb") as handle:
            files = {"file": (audio_path.name, handle, "audio/mpeg")}
            response = requests.post(
                url,
                headers={"Authorization": f"Bearer {api_key}"},
                data=data,
                files=files,
                timeout=600,
            )
        response.raise_for_status()
        payload = response.json()
        text = str(payload.get("text") or "").strip()
        if not text:
            raise RuntimeError(f"{service_name} returned empty transcription")
        logger.info("Transcription completed via {}: chars={}", service_name, len(text))
        return text

    def analyze(self, candidate: CallCandidate, transcript: str) -> dict[str, Any]:
        configs = self._chat_configs()
        errors: list[str] = []
        for provider_name, url, api_key, model in configs:
            try:
                logger.info("Analyzing transcript via {} model={}", provider_name, model)
                return self._analyze_with_config(candidate, transcript, provider_name, url, api_key, model)
            except Exception as exc:
                errors.append(f"{provider_name}: {exc}")
                logger.warning("Analysis provider failed: {} error={}", provider_name, exc)
                if self.mode() == "paid":
                    break
        raise RuntimeError("All analysis providers failed: " + " | ".join(errors))

    def _analyze_with_config(
        self,
        candidate: CallCandidate,
        transcript: str,
        provider_name: str,
        url: str,
        api_key: str,
        model: str,
    ) -> dict[str, Any]:
        prompt = build_analysis_prompt(candidate, transcript)
        body = {
            "model": model,
            "temperature": 0,
            "response_format": {"type": "json_object"},
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "Ты строгий руководитель отдела продаж юридической компании. "
                        "Анализируй только факты из транскрипта, не выдумывай."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
        }
        response = requests.post(
            url,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json=body,
            timeout=240,
        )
        if response.status_code == 400 and "response_format" in response.text:
            body.pop("response_format", None)
            response = requests.post(
                url,
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json=body,
                timeout=240,
        )
        response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"]
        try:
            result = json.loads(content)
        except json.JSONDecodeError:
            match = re.search(r"\{.*\}", content, flags=re.S)
            if not match:
                raise
            result = json.loads(match.group(0))
        result["_analysis_provider"] = provider_name
        logger.info("Analysis completed via {}: keys={}", provider_name, sorted(result.keys()))
        return result

    def _chat_configs(self) -> list[tuple[str, str, str, str]]:
        if self.mode() == "paid":
            return [
                (
                    "openai",
                    "https://api.openai.com/v1/chat/completions",
                    require_env("OPENAI_API_KEY"),
                    self.openai_analysis_model,
                )
            ]

        configs: list[tuple[str, str, str, str]] = []
        groq_key = ENV.get("GROQ_API_KEY", "").strip()
        if groq_key:
            configs.append(
                (
                    "groq",
                    "https://api.groq.com/openai/v1/chat/completions",
                    groq_key,
                    self.groq_analysis_model,
                )
            )

        freellm_key = ENV.get("FREELLM_API_KEY", "").strip()
        if freellm_key:
            base_url = (
                ENV.get("FREELLM_BASE_URL", "").strip()
                or ENV.get("LLM_BASE_URL", "").strip()
                or ENV.get("OPENAI_COMPATIBLE_BASE_URL", "").strip()
                or "http://155.212.217.115:3001/v1"
            )
            configs.append(
                (
                    "freellm",
                    base_url.rstrip("/") + "/chat/completions",
                    freellm_key,
                    self.freellm_analysis_model,
                )
            )

        if not configs:
            raise RuntimeError("Set GROQ_API_KEY or FREELLM_API_KEY for test analysis mode")
        return configs


def build_analysis_prompt(candidate: CallCandidate, transcript: str) -> str:
    meta = {
        "lead_id": candidate.lead_id,
        "lead_name": candidate.lead_name,
        "manager": candidate.manager_name,
        "duration_seconds": candidate.duration,
        "note_type": candidate.note_type,
        "pipeline_status": candidate.pipeline_status,
        "lead_url": candidate.lead_url,
    }
    return (
        "Проанализируй первый содержательный звонок с новым клиентом.\n"
        "Сначала определи, был ли звонок содержательным: обсуждалась ли потребность, услуга, цена, договор, "
        "следующий шаг, запись или решение клиента. Если это технический/короткий/нецелевой разговор, отметь это.\n\n"
        "Верни строго JSON со структурой:\n"
        "{\n"
        '  "is_substantive": true,\n'
        '  "confidence": 0.0,\n'
        '  "summary": "...",\n'
        '  "price_named": {"value": "да/нет/неясно", "format": "как названа стоимость", "evidence": "короткий фрагмент"},\n'
        '  "contract_offered": {"value": "да/нет/неясно", "evidence": "короткий фрагмент"},\n'
        '  "need_discovered": {"value": "да/нет/частично", "comment": "..."},\n'
        '  "procedure_explained": {"value": "да/нет/частично", "comment": "..."},\n'
        '  "objections": ["..."],\n'
        '  "objection_handling": "...",\n'
        '  "next_step": "...",\n'
        '  "manager_strengths": ["..."],\n'
        '  "manager_mistakes": ["..."],\n'
        '  "recommendations": ["..."],\n'
        '  "score_10": 0\n'
        "}\n\n"
        "Метаданные:\n"
        f"{json.dumps(meta, ensure_ascii=False, indent=2)}\n\n"
        "Транскрипт:\n"
        f"{transcript}"
    )


class TelegramBot:
    def __init__(self, token: str, admin_id: int, store: Store, amo: AmoClient, ai: AIClient) -> None:
        self.token = token
        self.admin_id = admin_id
        self.store = store
        self.amo = amo
        self.ai = ai
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.offset = 0
        self.job_lock = threading.Lock()
        self.monitor_stop = threading.Event()
        self.monitor_thread: threading.Thread | None = None
        self.awaiting_lead_search = False

    def api(self, method: str, **payload: Any) -> Any:
        payload = {key: value for key, value in payload.items() if value is not None}
        attempts = 1 if method == "answerCallbackQuery" else 3
        last_error: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                response = requests.post(f"{self.base_url}/{method}", json=payload, timeout=60)
                if not response.ok:
                    raise RuntimeError(f"Telegram {method} failed: {response.status_code} {response.text}")
                return response.json().get("result")
            except (requests.RequestException, RuntimeError) as exc:
                last_error = exc
                logger.warning("Telegram {} failed attempt {}/{}: {}", method, attempt, attempts, exc)
                if attempt < attempts:
                    time.sleep(2 * attempt)
        raise last_error or RuntimeError(f"Telegram {method} failed")

    def send(
        self,
        text: str,
        reply_markup: dict[str, Any] | None = None,
        parse_mode: str | None = None,
    ) -> None:
        chunks = [text[i : i + 3900] for i in range(0, len(text), 3900)] or [""]
        for index, chunk in enumerate(chunks):
            self.api(
                "sendMessage",
                chat_id=self.admin_id,
                text=chunk,
                parse_mode=parse_mode,
                disable_web_page_preview=True,
                reply_markup=reply_markup if index == len(chunks) - 1 else None,
            )

    def panel_markup(self) -> dict[str, Any]:
        return {
            "inline_keyboard": [
                [
                    {"text": "🟢 Сегодня", "callback_data": "analyze:today"},
                    {"text": "🟡 Вчера", "callback_data": "analyze:yesterday"},
                ],
                [{"text": "🟣 Сегодня + вчера", "callback_data": "analyze:both"}],
                [{"text": "🗂 База анализов", "callback_data": "archive:main"}],
                [{"text": "🔎 Найденные звонки", "callback_data": "candidates:both"}],
                [
                    {"text": "▶️ Мониторинг", "callback_data": "monitor:start"},
                    {"text": "⏹ Стоп", "callback_data": "monitor:stop"},
                ],
                [
                    {"text": "📊 Статистика", "callback_data": "stats"},
                    {"text": "⚙️ Состояние", "callback_data": "status"},
                ],
                [
                    {"text": "🧪 Тест", "callback_data": "mode:test"},
                    {"text": "💳 Платный", "callback_data": "mode:paid"},
                ],
            ]
        }

    def analysis_summary_markup(self, items: list[tuple[CallCandidate, dict[str, Any]]]) -> dict[str, Any]:
        keyboard = [[{"text": "🏠 Главное меню", "callback_data": "main:panel"}]]
        for candidate, _analysis in items:
            keyboard.append(
                [
                    {
                        "text": f"📄 {short_manager_name(candidate.manager_name)} #{candidate.lead_id} (анализ)",
                        "callback_data": f"report:{candidate.note_id}",
                    }
                ]
            )
        return {"inline_keyboard": keyboard}

    def report_markup(self) -> dict[str, Any]:
        return {
            "inline_keyboard": [
                [
                    {"text": "🗂 База", "callback_data": "archive:main"},
                    {"text": "⌂ Меню", "callback_data": "main:panel"},
                ],
            ]
        }

    def archive_markup(self) -> dict[str, Any]:
        keyboard = [
            [
                {"text": "🔎 ID сделки", "callback_data": "archive:search"},
                {"text": "🕘 Последние", "callback_data": "archive:recent"},
            ],
        ]
        manager_names = dict(MANAGER_IDS)
        for row in self.store.manager_archive_counts():
            manager_id = int(row.get("manager_id") or 0)
            if manager_id:
                manager_names.setdefault(manager_id, str(row.get("manager_name") or manager_id))
        manager_buttons = []
        for manager_id, manager_name in manager_names.items():
            manager_buttons.append(
                {
                    "text": f"👤 {short_manager_name(manager_name)}",
                    "callback_data": f"archive:manager:{manager_id}",
                }
            )
        for index in range(0, len(manager_buttons), 2):
            keyboard.append(manager_buttons[index : index + 2])
        keyboard.append([{"text": "⌂ Меню", "callback_data": "main:panel"}])
        return {"inline_keyboard": keyboard}

    def archive_list_markup(self, rows: list[dict[str, Any]], back_callback: str = "archive:main") -> dict[str, Any]:
        keyboard = [[{"text": "← База", "callback_data": back_callback}]]
        for row in rows[:30]:
            manager = short_manager_name(str(row.get("manager_name") or "Менеджер"))
            lead_id = row.get("lead_id")
            call_date = fmt_dt(int(row.get("call_time") or 0))[5:] if row.get("call_time") else ""
            keyboard.append(
                [
                    {
                        "text": f"📄 {manager} #{lead_id} {call_date}",
                        "callback_data": f"report:{row['note_id']}",
                    }
                ]
            )
        keyboard.append([{"text": "⌂ Меню", "callback_data": "main:panel"}])
        return {"inline_keyboard": keyboard}

    def start(self) -> None:
        print("Bot is running. Open Telegram and send /start to the bot.", flush=True)
        while True:
            try:
                updates = requests.get(
                    f"{self.base_url}/getUpdates",
                    params={"timeout": 45, "offset": self.offset},
                    timeout=60,
                ).json().get("result", [])
                for update in updates:
                    self.offset = max(self.offset, int(update["update_id"]) + 1)
                    self.handle_update(update)
            except Exception as exc:
                print(f"Telegram polling error: {exc}", flush=True)
                logger.warning("Telegram polling error: {}", exc)
                time.sleep(5)

    def handle_update(self, update: dict[str, Any]) -> None:
        message = update.get("message") or {}
        callback = update.get("callback_query") or {}
        from_id = int((message.get("from") or callback.get("from") or {}).get("id") or 0)
        if from_id != self.admin_id:
            if message:
                chat_id = message.get("chat", {}).get("id")
                if chat_id:
                    self.api("sendMessage", chat_id=chat_id, text="Нет доступа.")
            return

        if callback:
            try:
                self.api("answerCallbackQuery", callback_query_id=callback["id"])
            except Exception as exc:
                logger.debug("Ignoring stale callback answer error: {}", exc)
            self.handle_callback(str(callback.get("data") or ""))
            return

        text = str(message.get("text") or "")
        if self.awaiting_lead_search:
            self.awaiting_lead_search = False
            self.handle_lead_search_text(text)
        elif text.startswith("/start") or text.startswith("/panel"):
            self.send("Панель анализа звонков. amoCRM только читается.", self.panel_markup())
        elif text.startswith("/stats"):
            self.send(self.render_stats(), self.panel_markup())
        elif text.startswith("/status"):
            self.send(self.render_status(), self.panel_markup())
        elif text.startswith("/lead") or re.fullmatch(r"\s*\d{5,}\s*", text):
            self.handle_lead_search_text(text)
        else:
            self.send("Выбери действие.", self.panel_markup())

    def handle_callback(self, data: str) -> None:
        if data == "main:panel":
            self.awaiting_lead_search = False
            self.send("Панель анализа звонков. amoCRM только читается.", self.panel_markup())
        elif data.startswith("report:"):
            self.show_saved_report(data)
        elif data == "archive:main":
            self.awaiting_lead_search = False
            self.show_archive()
        elif data == "archive:recent":
            self.awaiting_lead_search = False
            self.show_archive_list("Последние анализы", self.store.list_done_calls(limit=30))
        elif data == "archive:search":
            self.awaiting_lead_search = True
            self.send(
                "🔎 <b>Поиск по сделке</b>\n\nОтправь ID сделки одним сообщением. Например: <code>32311331</code>",
                self.archive_markup(),
                parse_mode="HTML",
            )
        elif data.startswith("archive:manager:"):
            self.awaiting_lead_search = False
            self.show_manager_archive(data)
        elif data == "analyze:today":
            self.start_job("Анализ сегодня", lambda: self.run_analysis(["today"]))
        elif data == "analyze:yesterday":
            self.start_job("Анализ вчера", lambda: self.run_analysis(["yesterday"]))
        elif data == "analyze:both":
            self.start_job("Анализ сегодня+вчера", lambda: self.run_analysis(["yesterday", "today"]))
        elif data == "candidates:both":
            self.start_job("Проверка кандидатов", lambda: self.preview_candidates(["yesterday", "today"]))
        elif data == "monitor:start":
            self.start_monitor()
        elif data == "monitor:stop":
            self.stop_monitor()
        elif data == "stats":
            self.send(self.render_stats(), self.panel_markup())
        elif data == "status":
            self.send(self.render_status(), self.panel_markup())
        elif data == "mode:test":
            self.ai.set_mode("test")
            self.send(
                "Включен тестовый режим: транскрибация через Groq, анализ через Groq с fallback на FreeLLM.",
                self.panel_markup(),
            )
        elif data == "mode:paid":
            self.ai.set_mode("paid")
            self.send("Включен платный режим: транскрибация и анализ через OpenAI API.", self.panel_markup())

    def show_archive(self) -> None:
        counts = self.store.manager_archive_counts()
        total = sum(int(row["total"]) for row in counts)
        lines = [
            "🗂 <b>База анализов</b>",
            f"Всего отчетов: <b>{total}</b>",
            "",
            "Менеджеры, последние отчеты и поиск по ID сделки.",
        ]
        if counts:
            lines.append("")
            lines.append("<b>По менеджерам</b>")
            for row in counts:
                lines.append(
                    f"• {h(short_manager_name(str(row.get('manager_name') or 'Менеджер')))}: "
                    f"<b>{int(row['total'])}</b>"
                )
        self.send("\n".join(lines), self.archive_markup(), parse_mode="HTML")

    def show_archive_list(self, title: str, rows: list[dict[str, Any]], back_callback: str = "archive:main") -> None:
        if not rows:
            self.send(f"{title}: отчетов пока нет.", self.archive_markup())
            return
        self.send(render_archive_rows(title, rows), self.archive_list_markup(rows, back_callback), parse_mode="HTML")

    def show_manager_archive(self, data: str) -> None:
        try:
            manager_id = int(data.rsplit(":", 1)[1])
        except ValueError:
            self.send("Не смог открыть менеджера.", self.archive_markup())
            return
        manager_name = MANAGER_IDS.get(manager_id, f"Менеджер {manager_id}")
        rows = self.store.list_done_calls(manager_id=manager_id, limit=30)
        self.show_archive_list(f"Анализы: {manager_name}", rows)

    def handle_lead_search_text(self, text: str) -> None:
        match = re.search(r"\d{5,}", text)
        if not match:
            self.awaiting_lead_search = True
            self.send(
                "Не вижу ID сделки. Отправь только число, например: 32311331.",
                self.archive_markup(),
            )
            return
        lead_id = int(match.group(0))
        rows = self.store.list_done_calls(lead_id=lead_id, limit=50)
        if not rows:
            self.send(
                f"По сделке #{lead_id} анализов пока нет.",
                self.archive_markup(),
            )
            return
        self.show_archive_list(f"Анализы по сделке #{lead_id}", rows)

    def show_saved_report(self, data: str) -> None:
        try:
            note_id = int(data.split(":", 1)[1])
        except ValueError:
            self.send("Не смог открыть отчет: неверный номер звонка.", self.panel_markup())
            return
        row = self.store.get_call(note_id)
        if not row or not row.get("analysis_path"):
            self.send("Отчет не найден. Возможно, анализ еще не завершился.", self.panel_markup())
            return
        path = Path(str(row["analysis_path"]))
        if not path.exists():
            self.send("Файл отчета не найден на диске.", self.panel_markup())
            return
        analysis = json.loads(path.read_text(encoding="utf-8"))
        self.send(render_report_html(row, analysis), self.report_markup(), parse_mode="HTML")

    def start_job(self, title: str, target: Any) -> None:
        if self.job_lock.locked():
            self.send("Уже идет задача. Дождись завершения.", self.panel_markup())
            return
        self.send(f"Запускаю: {title}")
        thread = threading.Thread(target=self._run_job, args=(target,), daemon=True)
        thread.start()

    def _run_job(self, target: Any) -> None:
        with self.job_lock:
            try:
                target()
            except Exception as exc:
                logger.exception("Job failed")
                try:
                    self.send(f"Ошибка задачи: {exc}", self.panel_markup())
                except Exception as send_exc:
                    logger.error("Could not send job error to Telegram: {}", send_exc)

    def preview_candidates(self, days: list[str]) -> None:
        start_ts, end_ts, label = combined_range(days)
        logger.info("Telegram action: preview candidates for {} ({})", days, label)
        candidates = self.amo.find_call_candidates(start_ts, end_ts, self.store)
        if not candidates:
            self.send(f"Кандидатов за {label} не найдено.", self.panel_markup())
            return
        lines = [f"Кандидаты за {label}: {len(candidates)}"]
        for item in candidates[:30]:
            lines.append(
                f"- {fmt_dt(item.call_time)} | {item.manager_name} | {item.duration // 60} мин | "
                f"{item.pipeline_status} | {item.lead_name} | {item.lead_url}"
            )
        if len(candidates) > 30:
            lines.append(f"...и еще {len(candidates) - 30}")
        self.send("\n".join(lines), self.panel_markup())

    def run_analysis(self, days: list[str], silent_no_candidates: bool = False) -> None:
        start_ts, end_ts, label = combined_range(days)
        logger.info("Telegram action: run analysis for {} ({})", days, label)
        candidates = self.amo.find_call_candidates(start_ts, end_ts, self.store)
        if not candidates:
            if not silent_no_candidates:
                self.send(f"Новых кандидатов за {label} не найдено.", self.panel_markup())
            return
        self.send(f"Нашел кандидатов за {label}: {len(candidates)}. Начинаю обработку.")
        done = 0
        processed: list[tuple[CallCandidate, dict[str, Any]]] = []
        for candidate in candidates:
            try:
                analysis = self.process_candidate(candidate)
                processed.append((candidate, analysis))
                done += 1
            except Exception as exc:
                self.store.save_call(candidate, status="error", error=str(exc)[:2000])
                self.send(f"Ошибка по звонку {candidate.note_id}: {exc}")
        if processed:
            self.send(
                render_done_message(label, done, len(candidates)),
                self.analysis_summary_markup(processed),
                parse_mode="HTML",
            )
        else:
            self.send(f"Готово за {label}. Обработано: {done}/{len(candidates)}.", self.panel_markup())

    def process_candidate(self, candidate: CallCandidate) -> dict[str, Any]:
        logger.info(
            "Processing candidate start: lead={} note={} manager={} duration={} url={}",
            candidate.lead_id,
            candidate.note_id,
            candidate.manager_name,
            candidate.duration,
            candidate.lead_url,
        )
        self.store.save_call(candidate, status="processing")
        audio_path = self.amo.download_audio(candidate)
        transcript = self.ai.transcribe(audio_path)
        transcript_path = save_transcript(candidate, transcript)
        analysis = self.ai.analyze(candidate, transcript)
        analysis_path = save_analysis(candidate, analysis)
        is_substantive = bool(analysis.get("is_substantive"))
        self.store.save_call(
            candidate,
            status="done",
            audio_path=audio_path,
            transcript_path=transcript_path,
            analysis_path=analysis_path,
            is_substantive=is_substantive,
        )
        logger.info(
            "Processing candidate done: lead={} note={} substantive={} transcript={} analysis={}",
            candidate.lead_id,
            candidate.note_id,
            is_substantive,
            transcript_path,
            analysis_path,
        )
        return analysis

    def start_monitor(self) -> None:
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.send("Мониторинг уже включен.", self.panel_markup())
            return
        self.monitor_stop.clear()
        self.monitor_thread = threading.Thread(target=self.monitor_loop, daemon=True)
        self.monitor_thread.start()
        self.store.set_setting("monitor_started_at", str(int(time.time())))
        self.send("Мониторинг включен. Буду проверять новые подходящие звонки.", self.panel_markup())

    def stop_monitor(self) -> None:
        self.monitor_stop.set()
        self.send("Мониторинг остановлен.", self.panel_markup())

    def monitor_loop(self) -> None:
        interval = env_int("MONITOR_INTERVAL_SECONDS", 300)
        while not self.monitor_stop.is_set():
            if not self.job_lock.locked():
                with self.job_lock:
                    try:
                        self.run_analysis(["yesterday", "today"], silent_no_candidates=True)
                    except Exception as exc:
                        self.send(f"Ошибка мониторинга: {exc}", self.panel_markup())
            self.monitor_stop.wait(interval)

    def render_stats(self) -> str:
        stats = self.store.stats()
        lines = [
            "Статистика",
            f"Всего в базе: {stats['total']}",
            f"Готово: {stats['done']}",
            f"Содержательных: {stats['substantive']}",
            f"Ошибок: {stats['errors']}",
        ]
        if stats["last"]:
            lines.append("")
            lines.append("Последние:")
            for row in stats["last"]:
                lines.append(
                    f"- {fmt_dt(row['call_time'])} | {row['manager_name']} | "
                    f"lead {row['lead_id']} | note {row['note_id']} | {row['status']}"
                )
        return "\n".join(lines)

    def render_status(self) -> str:
        active = bool(self.monitor_thread and self.monitor_thread.is_alive())
        provider = self.ai.provider_summary()
        return "\n".join(
            [
                "Состояние",
                f"Мониторинг: {'включен' if active else 'выключен'}",
                f"Режим: {provider['mode']}",
                f"Минимальная длительность: {env_int('CALL_MIN_DURATION_SECONDS', 300)} сек",
                f"Воронки: Отдел продаж ({env_int('AMOCRM_SALES_PIPELINE_ID', 867829)}) + Юридический отдел ({env_int('AMOCRM_LEGAL_PIPELINE_ID', 1312204)})",
                "Этапы: все этапы выбранных воронок",
                f"Транскрибация: {provider['transcriber']}",
                f"Анализ: {provider['analyzer']}",
            ]
        )


def fmt_dt(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, timezone(timedelta(hours=7))).strftime("%Y-%m-%d %H:%M")


def fmt_duration(seconds: int) -> str:
    minutes, sec = divmod(int(seconds), 60)
    return f"{minutes}:{sec:02d}"


def save_transcript(candidate: CallCandidate, transcript: str) -> Path:
    day = datetime.fromtimestamp(candidate.call_time, timezone(timedelta(hours=7))).date().isoformat()
    out_dir = DATA_DIR / "transcripts" / day
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{candidate.lead_id}_{candidate.note_id}.txt"
    path.write_text(transcript, encoding="utf-8")
    logger.info("Transcript saved: {} chars={}", path, len(transcript))
    return path


def save_analysis(candidate: CallCandidate, analysis: dict[str, Any]) -> Path:
    day = datetime.fromtimestamp(candidate.call_time, timezone(timedelta(hours=7))).date().isoformat()
    out_dir = DATA_DIR / "analysis" / day
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{candidate.lead_id}_{candidate.note_id}.json"
    path.write_text(json.dumps(analysis, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Analysis saved: {}", path)
    return path


def value_block(value: Any) -> str:
    if isinstance(value, dict):
        parts = []
        for key in ["value", "format", "evidence", "comment"]:
            if value.get(key):
                parts.append(str(value[key]))
        return "; ".join(parts) if parts else json.dumps(value, ensure_ascii=False)
    if isinstance(value, list):
        return ", ".join(str(item) for item in value) if value else "нет"
    return str(value or "нет")


def h(value: Any) -> str:
    return html.escape(str(value or ""), quote=False)


def clip(value: Any, limit: int = 420) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


def short_manager_name(manager_name: str) -> str:
    mapping = {
        "Павел": "Павел",
        "Ольга Шевелева": "Шевелева",
        "Дегтярева Юлия": "Дегтярева",
        "Юлия Миллер": "Миллер",
    }
    if manager_name in mapping:
        return mapping[manager_name]
    parts = manager_name.split()
    return parts[-1] if len(parts) > 1 else (manager_name or "Менеджер")


def render_done_message(label: str, done: int, total: int) -> str:
    return (
        "✅ <b>Готово</b>\n"
        f"Дата: <b>{h(label)}</b>\n"
        f"Обработано: <b>{done}/{total}</b>\n\n"
        "Разборы лежат под кнопками ниже."
    )


def render_archive_rows(title: str, rows: list[dict[str, Any]]) -> str:
    lines = [
        f"🗂 <b>{h(title)}</b>",
        f"Найдено: <b>{len(rows)}</b>",
        "",
    ]
    for index, row in enumerate(rows[:30], start=1):
        call_time = fmt_dt(int(row.get("call_time") or 0)) if row.get("call_time") else "нет даты"
        duration = fmt_duration(int(row.get("duration") or 0))
        substantive = "да" if row.get("is_substantive") else "нет"
        lead_id = row.get("lead_id") or "?"
        lead_name = row.get("lead_name") or f"Сделка #{lead_id}"
        lines.extend(
            [
                f"<b>{index}. #{h(lead_id)} · {h(short_manager_name(str(row.get('manager_name') or 'Менеджер')))}</b>",
                f"🕒 {h(call_time)} · ⏱ {h(duration)} · 🎯 {h(substantive)}",
                f"📍 {h(row.get('pipeline_status') or 'этап не сохранен')}",
                f"🧾 {h(clip(lead_name, 120))}",
                "",
            ]
        )
    if len(rows) > 30:
        lines.append(f"Показаны первые 30 из {len(rows)}.")
    lines.append("Открой нужный отчет кнопкой ниже.")
    return fit_html_lines(lines)


def render_field(title: str, value: Any, emoji: str = "•") -> str:
    return f"{emoji} <b>{h(title)}:</b> {h(clip(value_block(value)))}"


def render_items(title: str, items: Any, emoji: str) -> list[str]:
    if not items:
        return []
    lines = ["", f"{emoji} <b>{h(title)}</b>"]
    if isinstance(items, list):
        lines.extend(f"  • {h(clip(item, 260))}" for item in items)
    else:
        lines.append(f"  • {h(clip(items, 260))}")
    return lines


def fit_html_lines(lines: list[str], limit: int = 3800) -> str:
    fitted: list[str] = []
    length = 0
    suffix = "\n\n…отчет сокращен, полная JSON-версия лежит в папке data/analysis."
    for line in lines:
        add = len(line) + (1 if fitted else 0)
        if length + add + len(suffix) > limit:
            fitted.append(suffix.strip())
            break
        fitted.append(line)
        length += add
    return "\n".join(fitted)


def render_report_html(source: dict[str, Any] | "CallCandidate", analysis: dict[str, Any]) -> str:
    if isinstance(source, dict):
        lead_id = int(source.get("lead_id") or 0)
        lead_name = source.get("lead_name") or f"Сделка #{lead_id}"
        lead_url = source.get("lead_url") or f"{ENV.get('AMOCRM_BASE_URL', '').rstrip('/')}/leads/detail/{lead_id}"
        manager_name = source.get("manager_name") or "не указан"
        call_time = int(source.get("call_time") or 0)
        duration = int(source.get("duration") or 0)
        pipeline_status = source.get("pipeline_status") or "не указан"
    else:
        lead_id = source.lead_id
        lead_name = source.lead_name
        lead_url = source.lead_url
        manager_name = source.manager_name
        call_time = source.call_time
        duration = source.duration
        pipeline_status = source.pipeline_status

    substantive = "да" if analysis.get("is_substantive") else "нет"
    confidence = analysis.get("confidence", "не указана")
    score = analysis.get("score_10", "не указана")
    lines = [
        "📞 <b>Анализ звонка</b>",
        f"<b>{h(lead_name)}</b>",
        f'<a href="{html.escape(str(lead_url), quote=True)}">Открыть сделку в amoCRM</a>',
        "",
        f"👤 <b>Менеджер:</b> {h(manager_name)}",
        f"🕒 <b>Дата:</b> {h(fmt_dt(call_time))}",
        f"⏱ <b>Длительность:</b> {h(fmt_duration(duration))}",
        f"📍 <b>Этап:</b> {h(pipeline_status)}",
        f"🎯 <b>Содержательный:</b> {h(substantive)}",
        f"📊 <b>Уверенность:</b> {h(confidence)}",
        f"⭐ <b>Оценка:</b> {h(score)}/10",
        "",
        f"🧾 <b>Кратко:</b> {h(clip(analysis.get('summary'), 700))}",
        "",
        render_field("Стоимость", analysis.get("price_named"), "💰"),
        render_field("Договор", analysis.get("contract_offered"), "📝"),
        render_field("Потребность", analysis.get("need_discovered"), "🔎"),
        render_field("Процедура", analysis.get("procedure_explained"), "⚙️"),
        render_field("Возражения", analysis.get("objections"), "🧱"),
        render_field("Отработка", analysis.get("objection_handling"), "🤝"),
        render_field("Следующий шаг", analysis.get("next_step"), "➡️"),
    ]
    lines.extend(render_items("Сильные стороны", analysis.get("manager_strengths"), "✅"))
    lines.extend(render_items("Ошибки", analysis.get("manager_mistakes"), "⚠️"))
    lines.extend(render_items("Рекомендации", analysis.get("recommendations"), "💡"))
    return fit_html_lines(lines)


def render_report(candidate: CallCandidate, analysis: dict[str, Any]) -> str:
    lines = [
        "Анализ звонка",
        f"Сделка: {candidate.lead_name}",
        f"Ссылка: {candidate.lead_url}",
        f"Менеджер: {candidate.manager_name}",
        f"Дата: {fmt_dt(candidate.call_time)}",
        f"Длительность: {fmt_duration(candidate.duration)}",
        f"Этап: {candidate.pipeline_status}",
        f"Содержательный: {'да' if analysis.get('is_substantive') else 'нет'}",
        f"Уверенность: {analysis.get('confidence', 'не указана')}",
        "",
        f"Кратко: {analysis.get('summary', '')}",
        f"Стоимость: {value_block(analysis.get('price_named'))}",
        f"Договор: {value_block(analysis.get('contract_offered'))}",
        f"Потребность: {value_block(analysis.get('need_discovered'))}",
        f"Процедура: {value_block(analysis.get('procedure_explained'))}",
        f"Возражения: {value_block(analysis.get('objections'))}",
        f"Отработка: {value_block(analysis.get('objection_handling'))}",
        f"Следующий шаг: {value_block(analysis.get('next_step'))}",
        f"Оценка: {analysis.get('score_10', 'не указана')}/10",
    ]
    for title, key in [
        ("Сильные стороны", "manager_strengths"),
        ("Ошибки", "manager_mistakes"),
        ("Рекомендации", "recommendations"),
    ]:
        items = analysis.get(key) or []
        if items:
            lines.append("")
            lines.append(title + ":")
            if isinstance(items, list):
                lines.extend(f"- {item}" for item in items)
            else:
                lines.append(str(items))
    return "\n".join(lines)


def main() -> None:
    setup_logging()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("Starting Sinai Call Agent")
    logger.info("Data dir: {}", DATA_DIR)
    logger.info(
        "Config: sales_pipeline_id={} legal_pipeline_id={} status_scope=all_target_pipeline_statuses min_duration={} monitor_interval={}",
        env_int("AMOCRM_SALES_PIPELINE_ID", 867829),
        env_int("AMOCRM_LEGAL_PIPELINE_ID", 1312204),
        env_int("CALL_MIN_DURATION_SECONDS", 300),
        env_int("MONITOR_INTERVAL_SECONDS", 300),
    )
    store = Store(DB_PATH)
    amo = AmoClient(require_env("AMOCRM_BASE_URL"), require_env("AMOCRM_ACCESS_TOKEN"))
    ai = AIClient(store)
    logger.info("AI provider summary: {}", ai.provider_summary())
    bot = TelegramBot(require_env("TG_BOT_TOKEN"), int(require_env("ADMIN_ID")), store, amo, ai)
    bot.start()


if __name__ == "__main__":
    main()
