"""In-memory user state management for multi-step booking flows."""

from dataclasses import dataclass, field
from datetime import date, time
from uuid import UUID


@dataclass
class BookingDraft:
    room_id: int | None = None
    room_name: str = ""
    room_calendar_mailbox: str = ""
    booking_date: date | None = None
    start_time: time | None = None
    duration_minutes: int | None = None


# Maps user HUID -> current state name
_user_states: dict[UUID, str] = {}

# Maps user HUID -> draft booking data
_user_drafts: dict[UUID, BookingDraft] = {}


def get_state(user_id: UUID) -> str | None:
    return _user_states.get(user_id)


def set_state(user_id: UUID, state: str) -> None:
    _user_states[user_id] = state


def clear_state(user_id: UUID) -> None:
    _user_states.pop(user_id, None)
    _user_drafts.pop(user_id, None)


def get_draft(user_id: UUID) -> BookingDraft:
    if user_id not in _user_drafts:
        _user_drafts[user_id] = BookingDraft()
    return _user_drafts[user_id]
