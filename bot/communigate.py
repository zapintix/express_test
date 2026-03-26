import asyncio
import base64
import logging
import re
import ssl
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen
from uuid import uuid4
from zoneinfo import ZoneInfo

from bot.settings import settings

logger = logging.getLogger(__name__)

COMMUNIGATE_BASE_URL = "https://cgp.peremena.ru"
COMMUNIGATE_TIMEZONE = "Europe/Moscow"
COMMUNIGATE_TZ = ZoneInfo(COMMUNIGATE_TIMEZONE)
# The live server currently exposes an incomplete certificate chain, so verified TLS fails.
COMMUNIGATE_SSL_CONTEXT = ssl._create_unverified_context()
COMMUNIGATE_XIMSS_VERSION = "6.1"


class CommuniGateError(Exception):
    """Raised when CommuniGate calendar sync fails."""


@dataclass(frozen=True)
class CalendarEvent:
    room_name: str
    calendar_mailbox: str
    booking_date: date
    start_time: time
    end_time: time
    description: str
    attendee_email: str
    user_huid: str
    user_name: str


@dataclass(frozen=True)
class CalendarEntry:
    uid: str
    room_name: str
    calendar_mailbox: str
    start: datetime
    end: datetime
    summary: str
    location: str
    description: str
    attendees: tuple[str, ...]
    organizer_email: str | None
    user_huid: str | None
    user_email: str | None
    user_name: str | None
    rrule: str | None


@dataclass(frozen=True)
class BusyInterval:
    start: datetime
    end: datetime
    summary: str
    uid: str


def _format_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _escape_ical(value: str) -> str:
    return (
        value.replace("\\", "\\\\")
        .replace(";", r"\;")
        .replace(",", r"\,")
        .replace("\n", r"\n")
    )


def _unescape_ical(value: str) -> str:
    return (
        value.replace(r"\n", "\n")
        .replace(r"\,", ",")
        .replace(r"\;", ";")
        .replace("\\\\", "\\")
    )


def _calendar_url(calendar_mailbox: str) -> str:
    mailbox = quote(calendar_mailbox.strip(), safe="")
    return f"{COMMUNIGATE_BASE_URL}/CalendarData/{mailbox}"


def _basic_auth_header() -> str:
    raw = f"{settings.communigate_username}:{settings.communigate_password}".encode(
        "utf-8"
    )
    token = base64.b64encode(raw).decode("ascii")
    return f"Basic {token}"


def _send_request(request: Request) -> bytes:
    try:
        with urlopen(request, timeout=20, context=COMMUNIGATE_SSL_CONTEXT) as response:
            if response.status not in {200, 201, 204}:
                raise CommuniGateError(
                    f"CommuniGate returned unexpected status {response.status}."
                )
            return response.read()
    except HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace")
        logger.error("CommuniGate HTTP error %s: %s", exc.code, details)
        raise CommuniGateError(
            f"CommuniGate returned HTTP {exc.code}."
        ) from exc
    except URLError as exc:
        raise CommuniGateError("CommuniGate is unreachable.") from exc


def _ximss_login_sync() -> str:
    params = urlencode(
        {
            "userName": settings.communigate_username,
            "password": settings.communigate_password,
            "version": COMMUNIGATE_XIMSS_VERSION,
            "errorAsXML": "yes",
        }
    )
    request = Request(
        f"{COMMUNIGATE_BASE_URL}/ximsslogin/?{params}",
        method="GET",
    )
    xml_text = _send_request(request).decode("utf-8", errors="replace")
    root = ET.fromstring(xml_text)
    session = root.find("session")
    if session is None or not session.get("urlID"):
        raise CommuniGateError("CommuniGate XIMSS login failed.")
    return session.get("urlID", "")


def _ximss_sync(session_id: str, operations: list[ET.Element]) -> ET.Element:
    root = ET.Element("XIMSS")
    for operation in operations:
        root.append(operation)

    request = Request(
        f"{COMMUNIGATE_BASE_URL}/Session/{quote(session_id, safe='')}/sync",
        data=ET.tostring(root, encoding="utf-8"),
        method="POST",
        headers={"Content-Type": "text/xml; charset=utf-8"},
    )
    xml_text = _send_request(request).decode("utf-8", errors="replace")
    response_root = ET.fromstring(xml_text)

    for response in response_root.findall("response"):
        if response.get("errorNum"):
            raise CommuniGateError(
                response.get("errorText") or "CommuniGate XIMSS request failed."
            )

    return response_root


def _build_icalendar(event: CalendarEvent, uid: str) -> str:
    start_dt = datetime.combine(event.booking_date, event.start_time, tzinfo=COMMUNIGATE_TZ)
    end_dt = datetime.combine(event.booking_date, event.end_time, tzinfo=COMMUNIGATE_TZ)
    now = datetime.now(timezone.utc)

    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//ExpressBronBot//CommuniGate Sync//RU",
        "CALSCALE:GREGORIAN",
        "BEGIN:VEVENT",
        f"UID:{uid}",
        f"DTSTAMP:{_format_utc(now)}",
        "SUMMARY:Бронирование переговорки",
        f"DTSTART:{_format_utc(start_dt)}",
        f"DTEND:{_format_utc(end_dt)}",
        f"LOCATION:{_escape_ical(event.room_name)}",
        f"DESCRIPTION:{_escape_ical(event.description)}",
        f"ORGANIZER:MAILTO:{settings.communigate_username}",
        f"ATTENDEE;ROLE=REQ-PARTICIPANT;PARTSTAT=ACCEPTED:MAILTO:{event.attendee_email}",
        f"X-EXPRESSBRONBOT-USER-HUID:{event.user_huid}",
        f"X-EXPRESSBRONBOT-USER-EMAIL:{event.attendee_email}",
        f"X-EXPRESSBRONBOT-USER-NAME:{_escape_ical(event.user_name)}",
        "STATUS:CONFIRMED",
        "TRANSP:OPAQUE",
        "END:VEVENT",
        "END:VCALENDAR",
        "",
    ]
    return "\r\n".join(lines)


def _unfold_ical(text: str) -> str:
    return re.sub(r"\r?\n[ \t]", "", text)


def _parse_event_datetime(line: str) -> datetime:
    head, value = line.split(":", 1)
    params = {}
    for token in head.split(";")[1:]:
        if "=" in token:
            key, param_value = token.split("=", 1)
            params[key] = param_value

    if params.get("VALUE") == "DATE":
        return datetime.strptime(value, "%Y%m%d").replace(tzinfo=COMMUNIGATE_TZ)

    if value.endswith("Z"):
        return datetime.strptime(value, "%Y%m%dT%H%M%SZ").replace(
            tzinfo=timezone.utc
        ).astimezone(COMMUNIGATE_TZ)

    tzid = params.get("TZID")
    if tzid in {None, COMMUNIGATE_TIMEZONE, "Russia/Moscow"}:
        tz = COMMUNIGATE_TZ
    else:
        tz = ZoneInfo(tzid)

    return datetime.strptime(value, "%Y%m%dT%H%M%S").replace(tzinfo=tz)


def _extract_mailto(line: str) -> str | None:
    if ":" not in line:
        return None
    value = line.split(":", 1)[1]
    if value.upper().startswith("MAILTO:"):
        return value[7:]
    return value or None


def _field_value(fields: dict[str, str], name: str) -> str | None:
    raw = fields.get(name)
    if not raw or ":" not in raw:
        return None
    return raw.split(":", 1)[1]


def _parse_calendar_entries(
    ical_text: str,
    calendar_mailbox: str,
    room_name: str,
) -> list[CalendarEntry]:
    unfolded = _unfold_ical(ical_text)
    entries: list[CalendarEntry] = []

    for chunk in unfolded.split("BEGIN:VEVENT")[1:]:
        lines = ("BEGIN:VEVENT" + chunk).splitlines()
        single_fields: dict[str, str] = {}
        attendees: list[str] = []

        for line in lines:
            if ":" not in line:
                continue
            field_name = line.split(":", 1)[0].split(";", 1)[0]
            if field_name in {
                "UID",
                "DTSTART",
                "DTEND",
                "SUMMARY",
                "LOCATION",
                "DESCRIPTION",
                "ORGANIZER",
                "RRULE",
                "X-EXPRESSBRONBOT-USER-HUID",
                "X-EXPRESSBRONBOT-USER-EMAIL",
                "X-EXPRESSBRONBOT-USER-NAME",
            }:
                single_fields[field_name] = line
            elif field_name == "ATTENDEE":
                attendee = _extract_mailto(line)
                if attendee:
                    attendees.append(attendee)

        if "UID" not in single_fields or "DTSTART" not in single_fields or "DTEND" not in single_fields:
            continue

        entries.append(
            CalendarEntry(
                uid=_field_value(single_fields, "UID") or "",
                room_name=room_name,
                calendar_mailbox=calendar_mailbox,
                start=_parse_event_datetime(single_fields["DTSTART"]),
                end=_parse_event_datetime(single_fields["DTEND"]),
                summary=_unescape_ical(_field_value(single_fields, "SUMMARY") or ""),
                location=_unescape_ical(_field_value(single_fields, "LOCATION") or ""),
                description=_unescape_ical(
                    _field_value(single_fields, "DESCRIPTION") or ""
                ),
                attendees=tuple(attendees),
                organizer_email=_extract_mailto(single_fields.get("ORGANIZER", "")),
                user_huid=_field_value(single_fields, "X-EXPRESSBRONBOT-USER-HUID")
                or None,
                user_email=_field_value(single_fields, "X-EXPRESSBRONBOT-USER-EMAIL")
                or None,
                user_name=_unescape_ical(
                    _field_value(single_fields, "X-EXPRESSBRONBOT-USER-NAME") or ""
                )
                or None,
                rrule=_field_value(single_fields, "RRULE") or None,
            )
        )

    return sorted(entries, key=lambda item: (item.start, item.end, item.uid))


def _fetch_calendar_sync(calendar_mailbox: str) -> str:
    request = Request(
        _calendar_url(calendar_mailbox),
        headers={"Authorization": _basic_auth_header()},
        method="GET",
    )
    return _send_request(request).decode("utf-8", errors="replace")


def _publish_event_sync(event: CalendarEvent) -> str:
    uid = f"{uuid4()}@expressbronbot"
    request = Request(
        _calendar_url(event.calendar_mailbox),
        data=_build_icalendar(event, uid).encode("utf-8"),
        method="PUT",
        headers={
            "Authorization": _basic_auth_header(),
            "Content-Type": "text/calendar; charset=utf-8",
        },
    )
    _send_request(request)
    return uid


def _cancel_event_sync(calendar_mailbox: str, item_uid: str) -> None:
    session_id = _ximss_login_sync()
    try:
        operations = [
            ET.Element(
                "calendarOpen",
                {"id": "open", "calendar": "room", "mailbox": calendar_mailbox},
            ),
            ET.Element(
                "calendarCancel",
                {
                    "id": "cancel",
                    "calendar": "room",
                    "itemUID": item_uid,
                    "sendRequests": "no",
                },
            ),
            ET.Element("bye", {"id": "bye"}),
        ]
        _ximss_sync(session_id, operations)
    except ET.ParseError as exc:
        raise CommuniGateError("CommuniGate returned invalid XIMSS XML.") from exc


def _to_busy_interval(entry: CalendarEntry, booking_date: date) -> BusyInterval | None:
    day_start = datetime.combine(booking_date, time.min, tzinfo=COMMUNIGATE_TZ)
    day_end = day_start + timedelta(days=1)
    if entry.end <= day_start or entry.start >= day_end:
        return None
    return BusyInterval(
        start=max(entry.start, day_start),
        end=min(entry.end, day_end),
        summary=entry.summary,
        uid=entry.uid,
    )


def _weekday_code(target_date: date) -> str:
    return ["MO", "TU", "WE", "TH", "FR", "SA", "SU"][target_date.weekday()]


def _parse_rrule(rule: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for part in rule.split(";"):
        if "=" in part:
            key, value = part.split("=", 1)
            result[key] = value
    return result


def _matches_rrule_on_date_core(
    entry: CalendarEntry,
    rule: dict[str, str],
    target_date: date,
) -> bool:
    if target_date < entry.start.date():
        return False

    frequency = rule.get("FREQ")
    interval = int(rule.get("INTERVAL", "1"))

    occurrence_start = datetime.combine(
        target_date,
        entry.start.timetz().replace(tzinfo=None),
        tzinfo=entry.start.tzinfo or COMMUNIGATE_TZ,
    )

    until_raw = rule.get("UNTIL")
    if until_raw:
        until_line = f"UNTIL:{until_raw}"
        if occurrence_start > _parse_event_datetime(until_line):
            return False

    if frequency == "DAILY":
        days = (target_date - entry.start.date()).days
        if days % interval != 0:
            return False
    elif frequency == "WEEKLY":
        allowed_days = rule.get("BYDAY", _weekday_code(entry.start.date())).split(",")
        if _weekday_code(target_date) not in allowed_days:
            return False
        anchor_week_start = entry.start.date() - timedelta(days=entry.start.date().weekday())
        target_week_start = target_date - timedelta(days=target_date.weekday())
        weeks = (target_week_start - anchor_week_start).days // 7
        if weeks < 0 or weeks % interval != 0:
            return False
        if weeks == 0 and target_date < entry.start.date():
            return False
    else:
        logger.warning("Unsupported RRULE frequency for UID %s: %s", entry.uid, entry.rrule)
        return False

    return True


def _matches_rrule_on_date(entry: CalendarEntry, target_date: date) -> bool:
    if not entry.rrule:
        return False

    rule = _parse_rrule(entry.rrule)
    if not _matches_rrule_on_date_core(entry, rule, target_date):
        return False

    count_raw = rule.get("COUNT")
    if count_raw:
        count_limit = int(count_raw)
        occurrence_index = 0
        current_date = entry.start.date()
        while current_date <= target_date:
            if _matches_rrule_on_date_core(entry, rule, current_date):
                occurrence_index += 1
                if current_date == target_date:
                    return occurrence_index <= count_limit
            current_date += timedelta(days=1)
        return False

    return True


def _expand_entry_for_date(entry: CalendarEntry, booking_date: date) -> CalendarEntry | None:
    day_start = datetime.combine(booking_date, time.min, tzinfo=COMMUNIGATE_TZ)
    day_end = day_start + timedelta(days=1)

    if entry.rrule:
        if not _matches_rrule_on_date(entry, booking_date):
            return None
        duration = entry.end - entry.start
        occurrence_start = datetime.combine(
            booking_date,
            entry.start.timetz().replace(tzinfo=None),
            tzinfo=entry.start.tzinfo or COMMUNIGATE_TZ,
        )
        occurrence_end = occurrence_start + duration
        return CalendarEntry(
            uid=entry.uid,
            room_name=entry.room_name,
            calendar_mailbox=entry.calendar_mailbox,
            start=occurrence_start,
            end=occurrence_end,
            summary=entry.summary,
            location=entry.location,
            description=entry.description,
            attendees=entry.attendees,
            organizer_email=entry.organizer_email,
            user_huid=entry.user_huid,
            user_email=entry.user_email,
            user_name=entry.user_name,
            rrule=entry.rrule,
        )

    if entry.end <= day_start or entry.start >= day_end:
        return None
    return entry


async def list_calendar_entries(
    calendar_mailbox: str,
    room_name: str,
) -> list[CalendarEntry]:
    ical_text = await asyncio.to_thread(_fetch_calendar_sync, calendar_mailbox)
    return _parse_calendar_entries(ical_text, calendar_mailbox, room_name)


async def list_events_for_date(
    calendar_mailbox: str,
    room_name: str,
    booking_date: date,
) -> list[CalendarEntry]:
    entries = await list_calendar_entries(calendar_mailbox, room_name)
    expanded_entries = [
        expanded
        for entry in entries
        if (expanded := _expand_entry_for_date(entry, booking_date)) is not None
    ]
    return sorted(expanded_entries, key=lambda item: (item.start, item.end, item.uid))


async def get_busy_intervals(
    calendar_mailbox: str,
    room_name: str,
    booking_date: date,
) -> list[BusyInterval]:
    entries = await list_events_for_date(calendar_mailbox, room_name, booking_date)
    intervals = [
        interval
        for entry in entries
        if (interval := _to_busy_interval(entry, booking_date)) is not None
    ]
    return sorted(intervals, key=lambda item: item.start)


async def get_available_start_times(
    calendar_mailbox: str,
    room_name: str,
    booking_date: date,
    candidate_times: list[time],
    minimum_duration_minutes: int = 30,
) -> list[time]:
    intervals = await get_busy_intervals(calendar_mailbox, room_name, booking_date)
    available: list[time] = []

    now_local = datetime.now(COMMUNIGATE_TZ)
    for candidate in candidate_times:
        candidate_dt = datetime.combine(booking_date, candidate, tzinfo=COMMUNIGATE_TZ)
        candidate_end = candidate_dt + timedelta(minutes=minimum_duration_minutes)
        if candidate_dt < now_local:
            continue
        if any(interval.start < candidate_end and interval.end > candidate_dt for interval in intervals):
            continue
        available.append(candidate)

    return available


async def get_available_durations(
    calendar_mailbox: str,
    room_name: str,
    booking_date: date,
    start_time: time,
    durations: list[int],
) -> list[int]:
    intervals = await get_busy_intervals(calendar_mailbox, room_name, booking_date)
    start_dt = datetime.combine(booking_date, start_time, tzinfo=COMMUNIGATE_TZ)
    available: list[int] = []

    for duration in durations:
        end_dt = start_dt + timedelta(minutes=duration)
        if any(interval.start < end_dt and interval.end > start_dt for interval in intervals):
            continue
        available.append(duration)

    return available


async def find_conflict(
    calendar_mailbox: str,
    room_name: str,
    booking_date: date,
    start_time: time,
    end_time: time,
) -> BusyInterval | None:
    start_dt = datetime.combine(booking_date, start_time, tzinfo=COMMUNIGATE_TZ)
    end_dt = datetime.combine(booking_date, end_time, tzinfo=COMMUNIGATE_TZ)
    intervals = await get_busy_intervals(calendar_mailbox, room_name, booking_date)

    for interval in intervals:
        if interval.start < end_dt and interval.end > start_dt:
            return interval
    return None


async def publish_booking(
    room_name: str,
    calendar_mailbox: str,
    booking_date: date,
    start_time: time,
    end_time: time,
    description: str,
    attendee_email: str,
    user_huid: str,
    user_name: str,
) -> str:
    event = CalendarEvent(
        room_name=room_name,
        calendar_mailbox=calendar_mailbox,
        booking_date=booking_date,
        start_time=start_time,
        end_time=end_time,
        description=description,
        attendee_email=attendee_email,
        user_huid=user_huid,
        user_name=user_name,
    )
    return await asyncio.to_thread(_publish_event_sync, event)


async def cancel_booking(calendar_mailbox: str, item_uid: str) -> None:
    await asyncio.to_thread(_cancel_event_sync, calendar_mailbox, item_uid)
