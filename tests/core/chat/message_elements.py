"""Stub of ``core.chat.message_elements`` — only the element classes the
plugin imports (used by its media filters and attachment passthrough)."""


class Text:
    def __init__(self, text=""):
        self.text = text


class At:
    def __init__(self, pid=""):
        self.pid = pid
        self.nickname = ""


class Reply:
    def __init__(self, chain=None):
        self.chain = chain or []


class Poke:
    pass


class Record:
    pass


class Image:
    pass


class Sticker(Image):
    pass


class File:
    pass


class Video:
    pass


class Forward:
    def __init__(self, chains=None):
        self.chains = chains or []
