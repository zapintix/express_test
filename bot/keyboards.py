from pybotx import BubbleMarkup

DEFAULT_START_TIMES = [
    "09:00",
    "10:00",
    "11:00",
    "12:00",
    "13:00",
    "14:00",
    "15:00",
    "16:00",
    "17:00",
    "18:00",
]

DEFAULT_DURATION_OPTIONS = [
    (30, "30 минут"),
    (60, "1 час"),
    (90, "1.5 часа"),
    (120, "2 часа"),
]


def get_start_bubbles():
    bubbles = BubbleMarkup()
    bubbles.add_button(command="/main_menu", label="Начать")
    return bubbles


def get_main_menu_bubbles():
    bubbles = BubbleMarkup()
    bubbles.add_button(command="/book_room", label="Забронировать переговорную")
    bubbles.add_button(command="/view_bookings", label="Посмотреть бронирования")
    bubbles.add_button(command="/my_bookings", label="Мои бронирования")
    return bubbles


def get_room_bubbles(rooms: list[dict]):
    bubbles = BubbleMarkup()
    for room in rooms:
        bubbles.add_button(
            command=f"/select_room {room['id']}",
            label=room["name"],
        )
    bubbles.add_button(command="/main_menu", label="Отмена")
    return bubbles


def get_date_bubbles():
    """Predefined date options: today and next 4 days."""
    from datetime import date, timedelta

    bubbles = BubbleMarkup()
    today = date.today()
    day_names = {
        0: "Пн", 1: "Вт", 2: "Ср", 3: "Чт", 4: "Пт", 5: "Сб", 6: "Вс",
    }
    for i in range(7):
        d = today + timedelta(days=i)
        if i == 0:
            label = f"Сегодня ({d.strftime('%d.%m')})"
        elif i == 1:
            label = f"Завтра ({d.strftime('%d.%m')})"
        else:
            label = f"{day_names[d.weekday()]} {d.strftime('%d.%m')}"
        bubbles.add_button(
            command=f"/select_date {d.isoformat()}",
            label=label,
        )
    bubbles.add_button(command="/main_menu", label="Отмена")
    return bubbles


def get_time_bubbles(times: list[str] | None = None):
    """Available meeting start times."""
    bubbles = BubbleMarkup()
    times = DEFAULT_START_TIMES if times is None else times
    for i in range(0, len(times), 2):
        bubbles.add_button(
            command=f"/select_time {times[i]}",
            label=times[i],
        )
        if i + 1 < len(times):
            bubbles.add_button(
                command=f"/select_time {times[i+1]}",
                label=times[i+1],
                new_row=False,
            )
    bubbles.add_button(command="/main_menu", label="Отмена")
    return bubbles


def get_duration_bubbles(options: list[tuple[int, str]] | None = None):
    bubbles = BubbleMarkup()
    options = DEFAULT_DURATION_OPTIONS if options is None else options
    for minutes, label in options:
        bubbles.add_button(
            command=f"/select_duration {minutes}",
            label=label,
        )
    bubbles.add_button(command="/main_menu", label="Отмена")
    return bubbles


def get_confirm_bubbles():
    bubbles = BubbleMarkup()
    bubbles.add_button(command="/confirm_booking", label="Подтвердить")
    bubbles.add_button(command="/main_menu", label="Отмена", new_row=False)
    return bubbles


def get_cancel_booking_bubbles(bookings: list[dict]):
    bubbles = BubbleMarkup()
    for b in bookings:
        label = (
            f"❌ {b['room_name']} | {b['booking_date']} "
            f"{b['start_time']}-{b['end_time']}"
        )
        bubbles.add_button(
            command=f"/cancel_booking {b['room_id']} {b['uid']}",
            label=label,
        )
    bubbles.add_button(command="/main_menu", label="Назад в меню")
    return bubbles


def get_back_to_menu_bubbles():
    bubbles = BubbleMarkup()
    bubbles.add_button(command="/main_menu", label="В главное меню")
    return bubbles
