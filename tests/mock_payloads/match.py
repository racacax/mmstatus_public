from datetime import datetime, timedelta


def get_match_participants(with_scores=False):
    return [
        {
            "participant": "387daf87-6d15-44d1-a59f-d59eb4873516",
            "position": 0,
            "teamPosition": 0,
            "rank": with_scores and 2 or None,
            "score": with_scores and 20 or None,
            "mvp": False,
            "leaver": None,
            "eliminated": False,
        },
        {
            "participant": "d9a8e973-3231-4e8b-8ed3-46810afe263e",
            "position": 1,
            "teamPosition": 0,
            "rank": with_scores and 3 or None,
            "score": with_scores and 19 or None,
            "mvp": False,
            "leaver": None,
            "eliminated": False,
        },
        {
            "participant": "9dceb5c6-7268-4768-947d-7d51a4525280",
            "position": 2,
            "teamPosition": 0,
            "rank": with_scores and 1 or None,
            "score": with_scores and 21 or None,
            "mvp": True,
            "leaver": None,
            "eliminated": False,
        },
        {
            "participant": "84078894-bae1-4399-b869-7b42a5240f02",
            "position": 3,
            "teamPosition": 1,
            "rank": with_scores and 4 or None,
            "score": with_scores and 18 or None,
            "mvp": False,
            "leaver": None,
            "eliminated": False,
        },
        {
            "participant": "217e8d68-4d83-4295-bb4b-3b6c2922caf3",
            "position": 4,
            "teamPosition": 1,
            "rank": with_scores and 5 or None,
            "score": with_scores and 17 or None,
            "mvp": False,
            "leaver": None,
            "eliminated": False,
        },
        {
            "participant": "5caf0042-0e73-4766-bbcc-a1c16cb4afd7",
            "position": 5,
            "teamPosition": 1,
            "rank": with_scores and 6 or None,
            "score": with_scores and 16 or None,
            "mvp": False,
            "leaver": None,
            "eliminated": False,
        },
    ]


DEFAULT_MAP_UID = "FAKE_UID"
DEFAULT_MATCH_ID = 1


def get_match(match_id=None, map_uid=None, start_date=None, end_date=None, name=None):
    return {
        "id": match_id or DEFAULT_MATCH_ID,
        "liveId": "LID-MTCH-1fxvjhm2miuzyoy",
        "name": name or "Official 3v3 - match",
        "startDate": start_date or (datetime.now() - timedelta(minutes=5)).timestamp(),
        "endDate": end_date or (datetime.now() + timedelta(minutes=5)).timestamp(),
        "status": "COMPLETED",
        "participantType": "team",
        "joinLink": None,
        "serverStatus": "DELETED",
        "manialink": None,
        "publicConfig": {
            "script": "TrackMania/TM_Teams_Matchmaking_Online.Script.txt",
            "maps": [map_uid or DEFAULT_MAP_UID],
        },
        "mediaUrl": "",
    }
