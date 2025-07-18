from flask import Flask, request, jsonify, make_response
from google.cloud import bigquery
import logging

# ---- Boilerplate ----
app = Flask(__name__)
client = bigquery.Client()
logging.basicConfig(level=logging.INFO)

def cors(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp

# ---- /results endpoint ----
@app.route("/results")
def get_results():
    team_id = request.args.get("team_id", type=int)
    if not team_id:
        return cors(make_response("team_id is required", 400))

    sql = f"""
    SELECT
      gameId             AS game_id,
      playDate           AS play_date,
      teams_home_caption AS home,
      teams_away_caption AS away,
      hall_caption       AS halle,
      hall_city          AS city,
      hall_plusCode      AS plus_code,
      referees,
      resultSummary      AS result,
      team_name,
      updated_at
    FROM `{client.project}.api_data.games_complete`
    WHERE wiedikon_team_id = @team_id
    ORDER BY playDate
    """
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("team_id", "INT64", team_id)]
        ),
    )
    rows = [dict(row) for row in job.result()]
    return cors(jsonify(rows))


# ---- /rankings endpoint ----
@app.route("/rankings")
def get_rankings():
    team_id = request.args.get("team_id", type=int)
    if not team_id:
        return cors(make_response("team_id is required", 400))

    sql = f"""
    SELECT
      leagueId       AS league_id,
      phaseId        AS phase_id,
      groupId        AS group_id,
      balls_lost,
      balls_won,
      defeats,
      defeats_clear,
      defeats_narrow,
      games_played,
      points,
      rank,
      sets_lost,
      sets_won,
      wins,
      wins_clear,
      wins_narrow,
      league_season,
      league_caption,
      phase_caption,
      group_caption,
      team_name,
      updated_at
    FROM `{client.project}.api_data.rankings_complete`
    WHERE wiedikon_team_id = @team_id
    ORDER BY rank
    """
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("team_id", "INT64", team_id)]
        ),
    )
    rows = [dict(row) for row in job.result()]
    return cors(jsonify(rows))


# ---- Run locally ----
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
