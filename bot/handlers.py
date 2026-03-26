import asyncio
import logging
import re
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, time, timedelta

from pybotx import Bot, HandlerCollector, IncomingMessage

from bot.communigate import (
    COMMUNIGATE_TZ,
    CalendarEntry,
    CommuniGateError,
    cancel_booking as cancel_calendar_booking,
    find_conflict,
    get_available_durations,
    get_available_start_times,
    list_calendar_entries,
    list_events_for_date,
    publish_booking,
)
from bot.keyboards import (
    DEFAULT_DURATION_OPTIONS,
    DEFAULT_START_TIMES,
    get_back_to_menu_bubbles,
    get_cancel_booking_bubbles,
    get_confirm_bubbles,
    get_date_bubbles,
    get_duration_bubbles,
    get_main_menu_bubbles,
    get_room_bubbles,
    get_start_bubbles,
    get_time_bubbles,
)
from bot.rooms import get_room_by_id, get_rooms
from bot.settings import settings
from bot.user_state import clear_state, get_draft, get_state, set_state

logger = logging.getLogger(__name__)
EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

collector = HandlerCollector()


def _candidate_start_times() -> list[time]:
    return [time.fromisoformat(value) for value in DEFAULT_START_TIMES]


def _format_times(times: list[time]) -> list[str]:
    return [value.strftime("%H:%M") for value in times]


def _duration_options() -> list[tuple[int, str]]:
    return DEFAULT_DURATION_OPTIONS


def _now_local() -> datetime:
    return datetime.now(COMMUNIGATE_TZ)


def _display_event_title(entry: CalendarEntry) -> str:
    if entry.user_name:
        return entry.user_name
    if entry.summary:
        return entry.summary
    if entry.attendees:
        return entry.attendees[0]
    return "занято"


def _format_entry_time(entry: CalendarEntry, target_date: date) -> str:
    day_start = datetime.combine(target_date, time.min, tzinfo=COMMUNIGATE_TZ)
    day_end = day_start + timedelta(days=1)
    start = max(entry.start, day_start)
    end = min(entry.end, day_end)
    if start == day_start and end == day_end:
        return "весь день"
    return f"{start.strftime('%H:%M')}-{end.strftime('%H:%M')}"


def _booking_button_payload(entry: CalendarEntry, room_id: int) -> dict:
    return {
        "uid": entry.uid,
        "room_id": room_id,
        "room_name": entry.room_name,
        "booking_date": entry.start.strftime("%Y-%m-%d"),
        "start_time": entry.start.strftime("%H:%M"),
        "end_time": entry.end.strftime("%H:%M"),
    }


async def _gather_room_events_for_date(target_date: date) -> list[tuple[dict, list[CalendarEntry]]]:
    rooms = get_rooms()
    results = await asyncio.gather(
        *[
            list_events_for_date(room["calendar_mailbox"], room["name"], target_date)
            for room in rooms
        ]
    )
    return list(zip(rooms, results, strict=True))


async def _gather_room_entries() -> list[tuple[dict, list[CalendarEntry]]]:
    rooms = get_rooms()
    results = await asyncio.gather(
        *[
            list_calendar_entries(room["calendar_mailbox"], room["name"])
            for room in rooms
        ]
    )
    return list(zip(rooms, results, strict=True))


def _entry_belongs_to_user(
    entry: CalendarEntry,
    user_huid: str,
    user_email: str | None,
) -> bool:
    if entry.user_huid == user_huid:
        return True
    if user_email and entry.user_email == user_email:
        return True
    if (
        user_email
        and entry.organizer_email == settings.communigate_username
        and entry.summary == "Бронирование переговорки"
        and user_email in entry.attendees
    ):
        return True
    return False


def _extract_email_from_payload(payload: object) -> str | None:
    candidates: list[tuple[int, str]] = []
    seen: set[int] = set()

    def walk(value: object, path: tuple[str, ...] = ()) -> None:
        if value is None:
            return

        value_id = id(value)
        if value_id in seen:
            return
        seen.add(value_id)

        if isinstance(value, str):
            candidate = value.strip()
            if EMAIL_PATTERN.fullmatch(candidate):
                path_text = ".".join(path).lower()
                if "email" in path_text:
                    priority = 0
                elif "login" in path_text or "username" in path_text:
                    priority = 1
                else:
                    priority = 2
                candidates.append((priority, candidate))
            return

        if is_dataclass(value):
            walk(asdict(value), path)
            return

        if isinstance(value, dict):
            for key, nested_value in value.items():
                walk(nested_value, (*path, str(key)))
            return

        if isinstance(value, (list, tuple, set)):
            for index, nested_value in enumerate(value):
                walk(nested_value, (*path, str(index)))
            return

        for method_name in ("model_dump", "dict"):
            dump_method = getattr(value, method_name, None)
            if callable(dump_method):
                try:
                    dumped = dump_method()
                except TypeError:
                    continue
                walk(dumped, path)
                return

        if hasattr(value, "__dict__"):
            walk(vars(value), path)

    walk(payload)
    if not candidates:
        return None

    candidates.sort(key=lambda item: item[0])
    return candidates[0][1]


async def _resolve_attendee_email(message: IncomingMessage, bot: Bot) -> str | None:
    sender_email = _extract_email_from_payload(message.sender)
    if sender_email:
        return sender_email

    try:
        user_info = await bot.search_user_by_huid(
            bot_id=message.bot.id,
            huid=message.sender.huid,
        )
    except Exception:
        logger.exception("Failed to resolve sender email via BotX user profile")
        return None

    return _extract_email_from_payload(user_info)


@collector.command("/start", description="Запуск бота")
async def start_handler(message: IncomingMessage, bot: Bot) -> None:
    clear_state(message.sender.huid)
    await bot.answer_message(
        "Добро пожаловать в бот бронирования переговорных!\n"
        "Я помогу вам забронировать переговорную, посмотреть расписание или отменить бронь.",
        bubbles=get_start_bubbles(),
    )


@collector.command("/main_menu", description="Главное меню")
async def main_menu_handler(message: IncomingMessage, bot: Bot) -> None:
    clear_state(message.sender.huid)
    await bot.answer_message(
        "Выберите действие:",
        bubbles=get_main_menu_bubbles(),
    )


@collector.command("/book_room", description="Забронировать переговорную")
async def book_room_handler(message: IncomingMessage, bot: Bot) -> None:
    clear_state(message.sender.huid)
    await bot.answer_message(
        "Выберите переговорную:",
        bubbles=get_room_bubbles(get_rooms()),
    )


@collector.command("/select_room", visible=False)
async def select_room_handler(message: IncomingMessage, bot: Bot) -> None:
    args = (message.body or "").split()
    if len(args) < 2 or not args[1].isdigit():
        await bot.answer_message(
            "Ошибка выбора комнаты.",
            bubbles=get_back_to_menu_bubbles(),
        )
        return

    room = get_room_by_id(int(args[1]))
    if not room:
        await bot.answer_message(
            "Переговорная не найдена.",
            bubbles=get_back_to_menu_bubbles(),
        )
        return

    draft = get_draft(message.sender.huid)
    draft.room_id = room["id"]
    draft.room_name = room["name"]
    draft.room_calendar_mailbox = room["calendar_mailbox"]
    set_state(message.sender.huid, "selecting_date")

    await bot.answer_message(
        f"Выбрана: {room['name']}\nВыберите дату:",
        bubbles=get_date_bubbles(),
    )


@collector.command("/select_date", visible=False)
async def select_date_handler(message: IncomingMessage, bot: Bot) -> None:
    args = (message.body or "").split()
    if len(args) < 2:
        await bot.answer_message(
            "Ошибка выбора даты.",
            bubbles=get_back_to_menu_bubbles(),
        )
        return

    try:
        booking_date = date.fromisoformat(args[1])
    except ValueError:
        await bot.answer_message(
            "Неверный формат даты.",
            bubbles=get_back_to_menu_bubbles(),
        )
        return

    if booking_date < _now_local().date():
        await bot.answer_message(
            "Нельзя бронировать на прошедшую дату.",
            bubbles=get_date_bubbles(),
        )
        return

    draft = get_draft(message.sender.huid)
    draft.booking_date = booking_date

    try:
        available_times = await get_available_start_times(
            draft.room_calendar_mailbox,
            draft.room_name,
            booking_date,
            _candidate_start_times(),
            minimum_duration_minutes=30,
        )
    except CommuniGateError:
        logger.exception("Failed to load room availability from CommuniGate")
        await bot.answer_message(
            "Не удалось получить занятость переговорки из календаря. Попробуйте позже.",
            bubbles=get_back_to_menu_bubbles(),
        )
        return

    if not available_times:
        await bot.answer_message(
            f"На {booking_date.strftime('%d.%m.%Y')} у переговорки {draft.room_name} нет свободных стартовых слотов.\n"
            "Выберите другую дату:",
            bubbles=get_date_bubbles(),
        )
        return

    set_state(message.sender.huid, "selecting_time")
    await bot.answer_message(
        f"Дата: {booking_date.strftime('%d.%m.%Y')}\nВыберите время начала:",
        bubbles=get_time_bubbles(_format_times(available_times)),
    )


@collector.command("/select_time", visible=False)
async def select_time_handler(message: IncomingMessage, bot: Bot) -> None:
    args = (message.body or "").split()
    if len(args) < 2:
        await bot.answer_message(
            "Ошибка выбора времени.",
            bubbles=get_back_to_menu_bubbles(),
        )
        return

    try:
        start_time = time.fromisoformat(args[1])
    except ValueError:
        await bot.answer_message(
            "Неверный формат времени.",
            bubbles=get_back_to_menu_bubbles(),
        )
        return

    draft = get_draft(message.sender.huid)

    try:
        available_times = await get_available_start_times(
            draft.room_calendar_mailbox,
            draft.room_name,
            draft.booking_date,
            _candidate_start_times(),
            minimum_duration_minutes=30,
        )
    except CommuniGateError:
        logger.exception("Failed to refresh room availability from CommuniGate")
        await bot.answer_message(
            "Не удалось обновить занятость переговорки. Попробуйте позже.",
            bubbles=get_back_to_menu_bubbles(),
        )
        return

    if start_time not in available_times:
        await bot.answer_message(
            "Это время уже занято или стало недоступно. Выберите другой слот:",
            bubbles=get_time_bubbles(_format_times(available_times)),
        )
        set_state(message.sender.huid, "selecting_time")
        return

    try:
        available_durations = await get_available_durations(
            draft.room_calendar_mailbox,
            draft.room_name,
            draft.booking_date,
            start_time,
            [minutes for minutes, _ in _duration_options()],
        )
    except CommuniGateError:
        logger.exception("Failed to load available durations from CommuniGate")
        await bot.answer_message(
            "Не удалось определить доступную длительность. Попробуйте позже.",
            bubbles=get_back_to_menu_bubbles(),
        )
        return

    if not available_durations:
        await bot.answer_message(
            "Для этого времени нет доступной длительности. Выберите другой слот:",
            bubbles=get_time_bubbles(_format_times(available_times)),
        )
        set_state(message.sender.huid, "selecting_time")
        return

    draft.start_time = start_time
    set_state(message.sender.huid, "selecting_duration")

    duration_options = [
        option for option in _duration_options() if option[0] in available_durations
    ]
    await bot.answer_message(
        f"Время начала: {start_time.strftime('%H:%M')}\nВыберите длительность:",
        bubbles=get_duration_bubbles(duration_options),
    )


@collector.command("/select_duration", visible=False)
async def select_duration_handler(message: IncomingMessage, bot: Bot) -> None:
    args = (message.body or "").split()
    if len(args) < 2 or not args[1].isdigit():
        await bot.answer_message(
            "Ошибка выбора длительности.",
            bubbles=get_back_to_menu_bubbles(),
        )
        return

    duration = int(args[1])
    draft = get_draft(message.sender.huid)

    try:
        available_durations = await get_available_durations(
            draft.room_calendar_mailbox,
            draft.room_name,
            draft.booking_date,
            draft.start_time,
            [minutes for minutes, _ in _duration_options()],
        )
    except CommuniGateError:
        logger.exception("Failed to re-check available durations in CommuniGate")
        await bot.answer_message(
            "Не удалось перепроверить доступную длительность. Попробуйте позже.",
            bubbles=get_back_to_menu_bubbles(),
        )
        return

    if duration not in available_durations:
        try:
            available_times = await get_available_start_times(
                draft.room_calendar_mailbox,
                draft.room_name,
                draft.booking_date,
                _candidate_start_times(),
                minimum_duration_minutes=30,
            )
        except CommuniGateError:
            logger.exception("Failed to refresh times after invalid duration")
            available_times = []
        await bot.answer_message(
            "Эта длительность уже недоступна. Выберите другой слот времени:",
            bubbles=get_time_bubbles(_format_times(available_times)),
        )
        set_state(message.sender.huid, "selecting_time")
        return

    draft.duration_minutes = duration

    start_dt = datetime.combine(draft.booking_date, draft.start_time)
    end_dt = start_dt + timedelta(minutes=duration)
    end_time = end_dt.time()

    try:
        conflict = await find_conflict(
            draft.room_calendar_mailbox,
            draft.room_name,
            draft.booking_date,
            draft.start_time,
            end_time,
        )
    except CommuniGateError:
        logger.exception("Failed to check room conflict in CommuniGate")
        await bot.answer_message(
            "Не удалось проверить занятость переговорки. Попробуйте позже.",
            bubbles=get_back_to_menu_bubbles(),
        )
        return

    if conflict:
        try:
            available_times = await get_available_start_times(
                draft.room_calendar_mailbox,
                draft.room_name,
                draft.booking_date,
                _candidate_start_times(),
                minimum_duration_minutes=30,
            )
        except CommuniGateError:
            logger.exception("Failed to refresh room availability from CommuniGate")
            available_times = []

        await bot.answer_message(
            f"Конфликт! {draft.room_name} занята на это время:\n"
            f"{conflict.start.strftime('%H:%M')}-{conflict.end.strftime('%H:%M')} ({conflict.summary or 'занято'})\n\n"
            "Выберите другое время:",
            bubbles=get_time_bubbles(_format_times(available_times)),
        )
        set_state(message.sender.huid, "selecting_time")
        return

    set_state(message.sender.huid, "confirming")
    await bot.answer_message(
        f"Подтвердите бронирование:\n\n"
        f"Переговорная: {draft.room_name}\n"
        f"Дата: {draft.booking_date.strftime('%d.%m.%Y')}\n"
        f"Время: {draft.start_time.strftime('%H:%M')} - {end_time.strftime('%H:%M')}\n"
        f"Длительность: {duration} мин.",
        bubbles=get_confirm_bubbles(),
    )


@collector.command("/confirm_booking", visible=False)
async def confirm_booking_handler(message: IncomingMessage, bot: Bot) -> None:
    user_id = message.sender.huid
    if get_state(user_id) != "confirming":
        await bot.answer_message(
            "Нет активного бронирования для подтверждения.",
            bubbles=get_main_menu_bubbles(),
        )
        return

    draft = get_draft(user_id)
    end_dt = datetime.combine(draft.booking_date, draft.start_time) + timedelta(
        minutes=draft.duration_minutes
    )
    end_time = end_dt.time()

    try:
        conflict = await find_conflict(
            draft.room_calendar_mailbox,
            draft.room_name,
            draft.booking_date,
            draft.start_time,
            end_time,
        )
    except CommuniGateError:
        logger.exception("Failed to re-check room conflict in CommuniGate")
        await bot.answer_message(
            "Не удалось повторно проверить календарь переговорки. Попробуйте позже.",
            bubbles=get_confirm_bubbles(),
        )
        return

    if conflict:
        clear_state(user_id)
        await bot.answer_message(
            f"К сожалению, {draft.room_name} уже занята на это время.\n"
            "Попробуйте выбрать другое время.",
            bubbles=get_main_menu_bubbles(),
        )
        return

    user_name = message.sender.username or "Неизвестный"
    attendee_email = await _resolve_attendee_email(message, bot)
    if attendee_email is None:
        await bot.answer_message(
            "Не удалось определить вашу email-почту для поля 'Участники'. Обратитесь к администратору.",
            bubbles=get_confirm_bubbles(),
        )
        return
    description = (
        f"Переговорная: {draft.room_name}\n"
        f"Дата: {draft.booking_date.strftime('%d.%m.%Y')}\n"
        f"Время: {draft.start_time.strftime('%H:%M')} - {end_time.strftime('%H:%M')}\n"
        f"Пользователь: {user_name}\n"
        f"Участник: {attendee_email}"
    )

    try:
        event_uid = await publish_booking(
            room_name=draft.room_name,
            calendar_mailbox=draft.room_calendar_mailbox,
            booking_date=draft.booking_date,
            start_time=draft.start_time,
            end_time=end_time,
            description=description,
            attendee_email=attendee_email,
            user_huid=str(user_id),
            user_name=user_name,
        )
    except CommuniGateError:
        logger.exception("Failed to publish booking to CommuniGate")
        await bot.answer_message(
            "Не удалось создать событие в календаре CommuniGate Pro. Попробуйте позже.",
            bubbles=get_confirm_bubbles(),
        )
        return

    clear_state(user_id)
    await bot.answer_message(
        f"Бронирование подтверждено!\n\n"
        f"Переговорная: {draft.room_name}\n"
        f"Дата: {draft.booking_date.strftime('%d.%m.%Y')}\n"
        f"Время: {draft.start_time.strftime('%H:%M')} - {end_time.strftime('%H:%M')}\n"
        f"UID события: {event_uid}",
        bubbles=get_back_to_menu_bubbles(),
    )


@collector.command("/view_bookings", description="Посмотреть бронирования")
async def view_bookings_handler(message: IncomingMessage, bot: Bot) -> None:
    clear_state(message.sender.huid)
    today = _now_local().date()
    dates = [today + timedelta(days=i) for i in range(7)]

    try:
        per_day_events = await asyncio.gather(
            *[_gather_room_events_for_date(d) for d in dates]
        )
    except CommuniGateError:
        logger.exception("Failed to load daily room bookings from CommuniGate")
        await bot.answer_message(
            "Не удалось загрузить бронирования из календарей переговорок.",
            bubbles=get_back_to_menu_bubbles(),
        )
        return

    # собираем все события в один список
    flattened = [
        (d, entry)
        for d, room_events in zip(dates, per_day_events, strict=True)
        for _, entries in room_events
        for entry in entries
    ]
    if not flattened:
        await bot.answer_message(
            "В ближайшие 7 дней бронирований нет.",
            bubbles=get_back_to_menu_bubbles(),
        )
        return

    from collections import defaultdict

    # уникальные комнаты
    rooms = sorted({entry.room_name for _, entry in flattened})

    # проходим по каждой дате отдельно
    for target_date in sorted({d for d, _ in flattened}):
        day_entries = [entry for d, entry in flattened if d == target_date]

        # уникальные слоты времени
        time_slots = sorted({entry.start.strftime("%H:%M") for entry in day_entries})
        if not time_slots:
            continue

        # создаём матрицу slot × room → текст
        matrix = defaultdict(lambda: defaultdict(str))
        for entry in day_entries:
            slot = entry.start.strftime("%H:%M")
            matrix[slot][entry.room_name] = _display_event_title(entry)

        # формируем bubbles для BotX
        bubbles = []

        # заголовок с комнатами
        header_row = ["Время"] + rooms
        bubbles.append({"text": " | ".join(header_row)})

        # строки с кнопками
        for slot in time_slots:
            row_buttons = []
            row_text = [slot]  # первая колонка — время
            for room in rooms:
                cell_text = matrix[slot].get(room, " ")  # пустая ячейка
                row_text.append(cell_text)
                # кнопка просто отображает текст, команды нет
                row_buttons.append({"text": cell_text, "command": None})
            bubbles.append({"text": " | ".join(row_text), "buttons": row_buttons})

        await bot.answer_message(
            f"Бронирования на {target_date.strftime('%d.%m.%Y')}:",
            bubbles=bubbles,
        )

@collector.command("/my_bookings", description="Мои бронирования")
async def my_bookings_handler(message: IncomingMessage, bot: Bot) -> None:
    clear_state(message.sender.huid)
    user_huid = str(message.sender.huid)
    user_email = _extract_email_from_payload(message.sender)
    now_local = _now_local()

    try:
        room_entries = await _gather_room_entries()
    except CommuniGateError:
        logger.exception("Failed to load user bookings from CommuniGate")
        await bot.answer_message(
            "Не удалось загрузить ваши бронирования из календарей переговорок.",
            bubbles=get_back_to_menu_bubbles(),
        )
        return

    matched_entries: list[tuple[CalendarEntry, int]] = []
    for room, entries in room_entries:
        for entry in entries:
            if entry.end < now_local:
                continue
            if not _entry_belongs_to_user(entry, user_huid, user_email):
                continue
            matched_entries.append((entry, room["id"]))

    matched_entries.sort(key=lambda item: (item[0].start, item[0].room_name, item[0].uid))

    if not matched_entries:
        await bot.answer_message(
            "У вас нет предстоящих бронирований.",
            bubbles=get_back_to_menu_bubbles(),
        )
        return

    bookings = [
        _booking_button_payload(entry, room_id)
        for entry, room_id in matched_entries
    ]
    lines = ["Ваши предстоящие бронирования:\n"]
    for entry, _ in matched_entries:
        lines.append(
            f"• {entry.room_name} | {entry.start.strftime('%d.%m.%Y %H:%M')}-{entry.end.strftime('%H:%M')}"
        )
    lines.append("\nНажмите на бронирование, чтобы отменить:")
    await bot.answer_message(
        "\n".join(lines),
        bubbles=get_cancel_booking_bubbles(bookings),
    )


@collector.command("/cancel_booking", visible=False)
async def cancel_booking_handler(message: IncomingMessage, bot: Bot) -> None:
    args = (message.body or "").split()
    if len(args) < 3 or not args[1].isdigit():
        await bot.answer_message(
            "Ошибка отмены.",
            bubbles=get_back_to_menu_bubbles(),
        )
        return

    room = get_room_by_id(int(args[1]))
    item_uid = args[2]
    if not room:
        await bot.answer_message(
            "Переговорная не найдена.",
            bubbles=get_back_to_menu_bubbles(),
        )
        return

    user_huid = str(message.sender.huid)
    user_email = _extract_email_from_payload(message.sender)

    try:
        entries = await list_calendar_entries(room["calendar_mailbox"], room["name"])
    except CommuniGateError:
        logger.exception("Failed to load room calendar before cancellation")
        await bot.answer_message(
            "Не удалось проверить бронирование перед отменой.",
            bubbles=get_back_to_menu_bubbles(),
        )
        return

    entry = next((item for item in entries if item.uid == item_uid), None)
    if entry is None or not _entry_belongs_to_user(entry, user_huid, user_email):
        await bot.answer_message(
            "Бронирование не найдено или вы не являетесь его автором.",
            bubbles=get_back_to_menu_bubbles(),
        )
        return

    try:
        await cancel_calendar_booking(room["calendar_mailbox"], item_uid)
    except CommuniGateError:
        logger.exception("Failed to cancel booking in CommuniGate")
        await bot.answer_message(
            "Не удалось отменить бронирование в календаре CommuniGate Pro.",
            bubbles=get_back_to_menu_bubbles(),
        )
        return

    await bot.answer_message(
        f"Бронирование {entry.room_name} на {entry.start.strftime('%d.%m.%Y %H:%M')} отменено.",
        bubbles=get_back_to_menu_bubbles(),
    )


@collector.default_message_handler
async def default_handler(message: IncomingMessage, bot: Bot) -> None:
    await bot.answer_message(
        "Не понимаю команду. Выберите действие из меню:",
        bubbles=get_main_menu_bubbles(),
    )
