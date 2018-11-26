import attr


@attr.s(slots=True)
class Entity:
    def as_dict(self):
        return attr.asdict(self)


@attr.s(slots=True)
class Symbol(Entity):
    full_name = attr.ib()
    short_code = attr.ib()

    available_count = attr.ib(default=None)
    category = attr.ib(default=None)
    icon_url = attr.ib(default=None)
    currency = attr.ib(default=None)


@attr.s(slots=True, frozen=True)
class Tick(Entity):
    short_code = attr.ib()
    event_at = attr.ib()
    current_value = attr.ib()
    current_volume = attr.ib()
