from flask import Flask, request, jsonify, make_response
from google.cloud import bigquery

app = Flask(__name__)
client = bigquery.Client()

def cors(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    return response

@app.route("/rankings")
def get_rankings():
    team_id = request.args.get("team_id", type=int)
    if not team_id:
        return cors(make_response("team_id is required", 400))

    # Query the rankings_complete view
    SQL = f"""
    SELECT
      leagueId           AS league_id,
      phaseId            AS phase_id,
      groupId            AS group_id,
      ranking.ballsLost  AS balls_lost,
      ranking.ballsWon   AS balls_won,
      ranking.defeats    AS defeats,
      ranking.defeatsClear   AS defeats_clear,
      ranking.defeatsNarrow AS defeats_narrow,
      ranking.games     AS games_played,
      ranking.points    AS points,
      ranking.rank      AS rank,
      ranking.setsLost  AS sets_lost,
      ranking.setsWon   AS sets_won,
      ranking.wins      AS wins,
      ranking.winsClear   AS wins_clear,
      ranking.winsNarrow AS wins_narrow,
      teamCaption       AS team_caption,
      teamId            AS team_id,
      updated_at        AS updated_at
    FROM `{client.project}.api_data.rankings_complete`
    WHERE wiedikon_team_id = @team_id
    ORDER BY ranking.rank
    """
    job = client.query(
        SQL,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("team_id", "INT64", team_id)
            ]
        )
    )
    rows = job.result()
    data = [dict(r) for r in rows]
    return cors(jsonify(data))

@app.route("/results")
def get_results():
    team_id = request.args.get("team_id", type=int)
    if not team_id:
        return cors(make_response("team_id is required", 400))

    # Query the games_complete view
    SQL = f"""
    SELECT
      gameId             AS game_id,
      playDate           AS play_date,
      gender,
      referees,
      setResults         AS set_results,
      resultSummary      AS result_summary,
      teams_home_caption AS home_team_caption,
      teams_away_caption AS away_team_caption,
      league_season,
      league_caption,
      phase_caption,
      group_caption,
      hall_caption,
      hall_street,
      hall_number,
      hall_zip,
      hall_city,
      hall_latitude,
      hall_longitude,
      hall_plusCode      AS hall_plus_code,
      updated_at         AS updated_at,
      team_name
    FROM `{client.project}.api_data.games_complete`
    WHERE home_team_id = @team_id OR away_team_id = @team_id
    ORDER BY playDate
    """
    job = client.query(
        SQL,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("team_id", "INT64", team_id)
            ]
        )
    )
    data = [dict(r) for r in job.result()]
    return cors(jsonify(data))

@app.route("/")
def root():
    return "Use /rankings?team_id=<id> or /results?team_id=<id>", 200
