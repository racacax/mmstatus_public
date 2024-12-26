from typing import TypedDict, Union, Optional


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


class NadeoMapInfo(TypedDict):
    uid: str
    mapId: str
    name: str
    author: str
    submitter: str
    authorTime: int
    goldTime: int
    silverTime: int
    bronzeTime: int
    nbLaps: int
    valid: bool
    downloadUrl: str
    thumbnailUrl: str
    uploadTimestamp: int
    updateTimestamp: int
    fileSize: Optional[int]
    public: bool
    favorite: bool
    playable: bool
    mapStyle: str
    mapType: str
    collectionName: str


class NadeoMatchTeam(TypedDict):
    position: int
    score: int
    rank: int


class NadeoPlayerRank(TypedDict):
    player: str
    score: int
    rank: int


class NadeoPlayerRanks(TypedDict):
    matchmakingId: int
    cardinal: int
    results: list[NadeoPlayerRank]


class NadeoAccountInfo(TypedDict):
    accountId: str
    clubTag: Optional[str]
    timestamp: str
