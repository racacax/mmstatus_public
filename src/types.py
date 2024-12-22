from typing import TypedDict, Union


class NadeoPublicConfig(TypedDict):
    script: str
    maps: list[str]


class NadeoMatch(TypedDict):
    id: int
    liveId: str
    name: str
    startDate: int
    endDate: int
    status: str
    participantType: str
    joinLink: str
    serverStatus: str
    manialink: Union[str, None]
    publicConfig: NadeoPublicConfig


class NadeoParticipant(TypedDict):
    participant: str
    position: int
    teamPosition: int
    rank: int
    score: int
    mvp: bool
    leaver: Union[bool, None]
    eliminated: bool
