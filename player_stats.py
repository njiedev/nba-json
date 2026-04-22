"""
Fetch career per-game stats (ppg, rpg, apg, spg, bpg) for every player in
nba_players.json and write them to player_stats.json keyed by PERSON_ID.
"""

import json
import time
from pathlib import Path

from nba_api.stats.endpoints import playercareerstats
from requests.exceptions import ReadTimeout, ConnectionError

ROOT = Path(__file__).resolve().parent
PLAYERS_PATH = ROOT / "nba_players.json"
OUTPUT_PATH = ROOT / "player_stats.json"

REQUEST_DELAY_SECONDS = 0.6
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5
SAVE_EVERY = 25


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


def per_game(total, games):
    if not games:
        return 0.0
    return round(total / games, 1)


def fetch_stats_for_player(person_id):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            stats = playercareerstats.PlayerCareerStats(player_id=person_id, timeout=30)
            rows = stats.get_normalized_dict().get("CareerTotalsRegularSeason", [])
            if not rows:
                return None
            r = rows[0]
            gp = r.get("GP") or 0
            return {
                "ppg": per_game(r.get("PTS", 0), gp),
                "rpg": per_game(r.get("REB", 0), gp),
                "apg": per_game(r.get("AST", 0), gp),
                "spg": per_game(r.get("STL", 0), gp),
                "bpg": per_game(r.get("BLK", 0), gp),
            }
        except (ReadTimeout, ConnectionError) as e:
            print(f"  retry {attempt}/{MAX_RETRIES} after network error: {e}")
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)
        except Exception as e:
            print(f"  failed for {person_id}: {e}")
            return None
    return None


def fetch_all_stats():
    players_list = load_players()
    output = load_existing_output()

    to_fetch = [p for p in players_list if str(p["PERSON_ID"]) not in output]
    print(f"{len(players_list)} total players, {len(output)} already fetched, {len(to_fetch)} to go")

    for i, player in enumerate(to_fetch, start=1):
        pid = player["PERSON_ID"]
        name = player.get("DISPLAY_FIRST_LAST", "")
        print(f"[{i}/{len(to_fetch)}] {name} ({pid})")

        stats = fetch_stats_for_player(pid)
        if stats is None:
            continue

        output[str(pid)] = stats

        if i % SAVE_EVERY == 0:
            save_output(output)
            print(f"  checkpoint saved ({len(output)} players)")

        time.sleep(REQUEST_DELAY_SECONDS)

    save_output(output)
    print(f"done. wrote {len(output)} players to {OUTPUT_PATH}")


if __name__ == "__main__":
    fetch_all_stats()
