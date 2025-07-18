from flask import Flask, request, jsonify, make_response
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

app = Flask(__name__)
client = bigquery.Client()

def cors(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    return response

@app.route("/rankings")
def get_rankings():
    # read ?team_id=<id>
    team_id = request.args.get("team_id", type=int)
    if not team_id:
        return cors(make_response("team_id is required", 400))

    # 1) find the groupId for that team
    q1 = """
    SELECT groupId
    FROM `swissvolleywiedikon.api_data.rankings`
    WHERE teamId = @team_id
    LIMIT 1
    """
    job1 = client.query(q1, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("team_id","INT64", team_id)]
    ))
    rows1 = list(job1.result())
    if not rows1:
        return cors(make_response("no such team in rankings", 404))
    group_id = rows1[0]["groupId"]

    # 2) fetch all rankings for that group
    q2 = f"""
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
      `swissvolleywiedikon.api_data.rankings`
    WHERE
      groupId = @group_id
    ORDER BY
      rank
    """
    job2 = client.query(q2, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("group_id","STRING", group_id)]
    ))
    data = [dict(row) for row in job2.result()]
    return cors(jsonify(data))


@app.route("/results")
def get_results():
    # read ?team_id=<id>
    team_id = request.args.get("team_id", type=int)
    if not team_id:
        return cors(make_response("team_id is required", 400))

    # Pull every game where that team was home or away
    q = """
    SELECT
      FORMAT_TIMESTAMP('%d/%m/%Y, %H:%M', playDate, 'Europe/Zurich') AS Date,
      teams_home_caption  AS Home,
      teams_away_caption  AS Away,
      hall_caption        AS Halle,
      hall_city           AS City,
      hall_plusCode       AS PlusCode,
      referees            AS `Referee(s)`,
      CONCAT(
        COALESCE(CAST(`set_1_home` AS STRING), ''), '-', 
        COALESCE(CAST(`set_1_away` AS STRING), ''), ' | ',
        COALESCE(CAST(`set_2_home` AS STRING), ''), '-', 
        COALESCE(CAST(`set_2_away` AS STRING), '')
        -- … you can expand to sets 3/4/5 if you like …
      ) AS Result
    FROM
      `swissvolleywiedikon.api_data.games`
    WHERE
      home_team_id = @team_id OR away_team_id = @team_id
    ORDER BY
      playDate
    """
    job = client.query(q, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("team_id","INT64", team_id)]
    ))
    data = [dict(row) for row in job.result()]
    return cors(jsonify(data))
