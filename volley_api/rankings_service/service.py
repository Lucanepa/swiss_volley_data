from flask import Flask, request, jsonify, make_response
from google.cloud import bigquery

app = Flask(__name__)
client = bigquery.Client()

def cors(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp

@app.route("/rankings")
def get_rankings():
    team_id = request.args.get("team_id", type=int)
    if not team_id:
        return cors(make_response("team_id is required", 400))

    sql = f"""
    SELECT
      r.leagueId               AS league_id,
      r.phaseId                AS phase_id,
      r.groupId                AS group_id,
      rm.ballsLost             AS balls_lost,
      rm.ballsWon              AS balls_won,
      rm.defeats               AS defeats,
      rm.defeatsClear          AS defeats_clear,
      rm.defeatsNarrow         AS defeats_narrow,
      rm.games                 AS games_played,
      rm.points                AS points,
      rm.rank                  AS rank,
      rm.setsLost              AS sets_lost,
      rm.setsWon               AS sets_won,
      rm.wins                  AS wins,
      rm.winsClear             AS wins_clear,
      rm.winsNarrow            AS wins_narrow,
      rm.teamCaption           AS team_name,
      r.updated_at
    FROM `{client.project}.api_data.rankings` AS r,
         UNNEST(r.ranking) AS rm
    WHERE rm.teamId = @team_id
    ORDER BY rm.rank
    """
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("team_id","INT64",team_id)]
        ),
    )
    rows = [dict(r) for r in job.result()]
    return cors(jsonify(rows))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
