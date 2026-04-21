
from nba_api.stats.endpoints import commonplayerinfo
from nba_api.stats.endpoints import teaminfocommon
from nba_api.stats.static import players
import time
import json
def main():
    activePlayers = players.get_active_players()
    all_players = []
    for item in activePlayers:
        playerDict = {}
        index = 0
        player_info = commonplayerinfo.CommonPlayerInfo(player_id= item["id"])
        headers = player_info.common_player_info.get_dict()['headers']
        data = player_info.common_player_info.get_dict()['data'][0]
        while index < len(headers):
            playerDict[headers[index]] = data[index]
            index += 1
        all_players.append(playerDict)
        time.sleep(.8)
    with open('nba_players.json', 'w') as nbaplayerslist:
        json.dump(all_players, nbaplayerslist, indent = 2)
    print("Done!")
main()