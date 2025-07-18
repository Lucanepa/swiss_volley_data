from flask import Flask, request, jsonify, make_response
from google.cloud import bigquery
import logging

app = Flask(__name__)
client = bigquery.Client()

# configure root logger
target_level = logging.INFO
logging.basicConfig(level=target_level)


def cors(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    return response


@app.route("/rankings")
def get_rankings():
    team_id = request.args.get("team_id", type=int)
    if not team_id:
        return cors(make_response("team_id is required", 400))

    SQL = f"""
    SELECT
      league_id,
      phase_id,
      group_id,
      balls_lost,
      balls_won,
      defeats,
      defeats_clear,
      defeats_narrow,
      games,
      points,
      rank,
      sets_lost,
      sets_won,
      wins,
      wins_clear,
      wins_narrow,
      teamCaption     AS team_caption,
      updated_at
    FROM `{client.project}.api_data.rankings`
    WHERE wiedikon_team_id = @team_id
    ORDER BY rank
    """
    try:
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
    except Exception as e:
        logging.exception("Error running rankings query")
        return cors(make_response(f"Ranking query failed: {e}", 500))


@app.route("/results")
def get_results():
    team_id = request.args.get("team_id", type=int)
    if not team_id:
        return cors(make_response("team_id is required", 400))

    SQL = f"""
    SELECT
      gameId          AS game_id,
      playDate        AS play_date,
      gender,
      referees,
      setResults      AS set_results,
      resultSummary   AS result_summary,
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
      hall_plusCode   AS hall_plus_code,
      updated_at
    FROM `{client.project}.api_data.games_complete`
    WHERE wiedikon_team_id = @team_id
    ORDER BY play_date
    """
    try:
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
    except Exception as e:
        logging.exception("Error running results query")
        return cors(make_response(f"Results query failed: {e}", 500))


@app.route("/")
def root():
    return "Use /rankings?team_id=<id> or /results?team_id=<id>", 200
