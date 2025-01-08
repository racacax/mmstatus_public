from datetime import datetime
from typing import Union

from peewee import fn, Case

from models import Player, PlayerGame, Game, Zone


def get_player_counts(Opponent, OpponentGame):
    obj = {
        "total_played": fn.COUNT(Opponent.uuid),
        "total_played_against": fn.SUM(Case(None, [((PlayerGame.is_win != OpponentGame.is_win), 1)], 0)),
        "total_played_along": fn.SUM(Case(None, [((PlayerGame.is_win == OpponentGame.is_win), 1)], 0)),
        "total_games_lost_against": fn.SUM(
            Case(
                None,
                [(((PlayerGame.is_win == False) & (OpponentGame.is_win == True)), 1)],
                0,
            )
        ),
        "total_games_won_against": fn.SUM(
            Case(
                None,
                [(((OpponentGame.is_win == False) & (PlayerGame.is_win == True)), 1)],
                0,
            )
        ),
        "total_games_lost_along": fn.SUM(
            Case(
                None,
                [(((PlayerGame.is_win == False) & (OpponentGame.is_win == False)), 1)],
                0,
            )
        ),
        "total_games_won_along": fn.SUM(
            Case(
                None,
                [(((OpponentGame.is_win == True) & (PlayerGame.is_win == True)), 1)],
                0,
            )
        ),
    }
    return {k: v.alias(k) for k, v in obj.items()}


def get_params(group_by: str, Opponent):
    if group_by == "country":
        return [Zone.name, Zone.file_name]
    elif group_by == "uuid":
        return [Opponent.name]
    else:
        return []


def get_query(
    min_date: datetime,
    max_date: datetime,
    player: str,
    group_by: str,
    order_by: str,
    order: Union["desc", "asc"],
    page: int,
) -> list:
    Opponent = Player.alias("opponent")
    OpponentGame = PlayerGame.alias("pg2")
    player_counts = get_player_counts(Opponent, OpponentGame)
    order_by = player_counts[order_by]
    if order == "desc":
        order_by = order_by.desc()
    else:
        order_by = order_by.asc()

    if group_by == "country":

        def apply(x):
            return x.join(Zone, on=(Opponent.country == Zone.id))

    else:

        def apply(x):
            return x

    group_by_obj = Opponent.__getattr__(group_by)
    return (
        apply(
            Player.select(
                group_by_obj,
                *get_params(group_by, Opponent),
                *player_counts.values(),
            )
            .join(PlayerGame)
            .join(Game)
            .join(OpponentGame)
            .join(Opponent)
        )
        .where(
            Player.uuid == player,
            Opponent.uuid != player,
            Game.time >= min_date,
            Game.time <= max_date,
        )
        .group_by(group_by_obj)
        .order_by(order_by)
        .paginate(page, 10)
        .dicts()
    )
