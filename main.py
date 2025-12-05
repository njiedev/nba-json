
from nba_api.stats.endpoints import commonplayerinfo
from nba_api.stats.endpoints import teaminfocommon
from nba_api.stats.static import players
import time


def main():

    activePlayers = players.get_active_players()
    for item in activePlayers:
        player_info = commonplayerinfo.CommonPlayerInfo(player_id= item["id"])
        print(player_info.common_player_info.get_dict()['data'])
        time.sleep(1.2)
    


main()