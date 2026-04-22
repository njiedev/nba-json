"""
Combined NBA data pipeline:
  1. fetch_players() — pull every active player's common info, write nba_players.json
  2. fetch_career_paths() — for each player in nba_players.json, fetch their team-by-team career path,
     write player_career_paths.json (resumable; crash-safe writes).
"""

import json
import time
from pathlib import Path

from nba_api.stats.endpoints import commonplayerinfo, playercareerstats
from nba_api.stats.static import players
from requests.exceptions import ReadTimeout, ConnectionError

ROOT = Path(__file__).resolve().parent
PLAYERS_PATH = ROOT / "nba_players.json"
OUTPUT_PATH = ROOT / "player_career_paths.json"

REQUEST_DELAY_SECONDS = 0.6
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5
SAVE_EVERY = 25


def fetch_players():
    active_players = players.get_active_players()
    all_players = []
    for item in active_players:
        player_dict = {}
        player_info = commonplayerinfo.CommonPlayerInfo(player_id=item["id"])
        headers = player_info.common_player_info.get_dict()["headers"]
        data = player_info.common_player_info.get_dict()["data"][0]
        for index in range(len(headers)):
            player_dict[headers[index]] = data[index]
        all_players.append(player_dict)
        time.sleep(0.8)
    with open(PLAYERS_PATH, "w") as f:
        json.dump(all_players, f, indent=2)
    print(f"Done! wrote {len(all_players)} players to {PLAYERS_PATH}")


def load_players():
    with open(PLAYERS_PATH) as f:
        return json.load(f)


def load_existing_output():
    if OUTPUT_PATH.exists():
        with open(OUTPUT_PATH) as f:
            return json.load(f)
    return {}


def save_output(data):
    tmp = OUTPUT_PATH.with_suffix(".json.tmp")
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    tmp.replace(OUTPUT_PATH)


def fetch_path_for_player(person_id):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            stats = playercareerstats.PlayerCareerStats(player_id=person_id, timeout=30)
            rows = stats.get_normalized_dict().get("SeasonTotalsRegularSeason", [])
            return [
                {
                    "SEASON_ID": r.get("SEASON_ID"),
                    "TEAM_ID": r.get("TEAM_ID"),
                    "TEAM_ABBREVIATION": r.get("TEAM_ABBREVIATION"),
                }
                for r in rows
            ]
        except (ReadTimeout, ConnectionError) as e:
            print(f"  retry {attempt}/{MAX_RETRIES} after network error: {e}")
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)
        except Exception as e:
            print(f"  failed for {person_id}: {e}")
            return None
    return None


def fetch_career_paths():
    if not PLAYERS_PATH.exists():
        print(f"{PLAYERS_PATH} not found — running fetch_players() first")
        fetch_players()

    players_list = load_players()
    output = load_existing_output()

    to_fetch = [p for p in players_list if str(p["PERSON_ID"]) not in output]
    print(f"{len(players_list)} total players, {len(output)} already fetched, {len(to_fetch)} to go")

    for i, player in enumerate(to_fetch, start=1):
        pid = player["PERSON_ID"]
        name = player["DISPLAY_FIRST_LAST"]
        print(f"[{i}/{len(to_fetch)}] {name} ({pid})")

        path = fetch_path_for_player(pid)
        if path is None:
            continue

        output[str(pid)] = {
            "PERSON_ID": pid,
            "DISPLAY_FIRST_LAST": name,
            "PATH": path,
        }

        if i % SAVE_EVERY == 0:
            save_output(output)
            print(f"  checkpoint saved ({len(output)} players)")

        time.sleep(REQUEST_DELAY_SECONDS)

    save_output(output)
    print(f"done. wrote {len(output)} players to {OUTPUT_PATH}")


if __name__ == "__main__":
    fetch_career_paths()
