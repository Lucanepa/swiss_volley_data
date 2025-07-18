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

    # Query rankings directly by the wistikon_team_id column
    SQL = f"""
    SELECT
      teamCaption AS Team,
      points      AS Pts,
      games       AS `# Matches`,
      wins        AS `Matches won`,
      defeats     AS `Matches lost`,
      setsWon     AS `Sets won`,
      setsLost    AS `Sets lost`,
      ballsWon    AS `Balls won`,
      ballsLost   AS `Balls lost`
    FROM
      `{client.project}.api_data.rankings`
    WHERE
      wiedikon_team_id = @team_id
    ORDER BY
      rank
    """
    job = client.query(
        SQL,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("team_id", "INT64", team_id)]
        )
    )
    data = [dict(row) for row in job.result()]
    return cors(jsonify(data))

@app.route("/results")
def get_results():
    team_id = request.args.get("team_id", type=int)
    if not team_id:
        return cors(make_response("team_id is required", 400))

    # Query games by team_id
    SQL = f"""
    SELECT
      FORMAT_TIMESTAMP('%d/%m/%Y, %H:%M', playDate, 'Europe/Zurich') AS Date,
      teams_home_caption  AS Home,
      teams_away_caption  AS Away,
      hall_caption        AS Halle,
      hall_city           AS City,
      hall_plusCode       AS PlusCode,
      referees            AS `Referee(s)`,
      CONCAT(
        CAST(set_1_home AS STRING), '-', CAST(set_1_away AS STRING), ' | ',
        CAST(set_2_home AS STRING), '-', CAST(set_2_away AS STRING)
      ) AS Result
    FROM
      `{client.project}.api_data.games`
    WHERE
      home_team_id = @team_id OR away_team_id = @team_id
    ORDER BY
      playDate
    """
    job = client.query(
        SQL,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("team_id", "INT64", team_id)]
        )
    )
    data = [dict(row) for row in job.result()]
    return cors(jsonify(data))

@app.route("/")
def root():
    return "Use /rankings?team_id=<id> or /results?team_id=<id>", 200
