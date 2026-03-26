ROOMS = [
    {
        "id": 1,
        "name": "Переговорка большая",
        "calendar_mailbox": "Переговорка большая",
    },
    {
        "id": 2,
        "name": "Переговорка средняя",
        "calendar_mailbox": "Переговорка средняя",
    },
    {
        "id": 3,
        "name": "Переговорка малая",
        "calendar_mailbox": "Переговорка малая",
    },
]


def get_rooms() -> list[dict]:
    return [room.copy() for room in ROOMS]


def get_room_by_id(room_id: int) -> dict | None:
    for room in ROOMS:
        if room["id"] == room_id:
            return room.copy()
    return None
