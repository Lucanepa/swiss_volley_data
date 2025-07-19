from flask import Flask, request, jsonify, make_response
from google.cloud import bigquery

app = Flask(__name__)
client = bigquery.Client()

def cors(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp

@app.route("/results")
def get_results():
    team_id = request.args.get("team_id", type=int)
    if not team_id:
        return cors(make_response("team_id is required", 400))

    sql = f"""
    SELECT
      *
    FROM `{client.project}.api_data.games_complete`
    WHERE wiedikon_team_id = @team_id
    ORDER BY playDate
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
