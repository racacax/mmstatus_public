from src.player_views import PlayerAPIViews
from src.views import APIViews

routes = {**APIViews.routes(), **PlayerAPIViews.routes()}
