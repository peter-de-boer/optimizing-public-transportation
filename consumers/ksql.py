"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"

#
# TODO: Complete the following KSQL statements.
# TODO: For the first statement, create a `turnstile` table from your turnstile topic.
#       Make sure to use 'avro' datatype!
# TODO: For the second statment, create a `turnstile_summary` table by selecting from the
#       `turnstile` table and grouping on station_id.
#       Make sure to cast the COUNT of station id to `count`
#       Make sure to set the value format to JSON

# TODO: producer: one topic for each station
#       consumer: one turnstile table, can consume only one topic
# either 1) produce only one topic
# or     2) multiple table and combine them
#
# 2): UNION?
# 1): probably better: turnstile events should write into same topic
# assume 1) 

KSQL_STATEMENT = """
CREATE TABLE turnstile (
    station_name VARCHAR,
    station_id INTEGER,
    line VARCHAR
) WITH (
    KAFKA_TOPIC='nd.project.opt.turnstile',
    VALUE_FORMAT='Avro',
    KEY='station_id'
);


CREATE TABLE turnstile_summary
WITH (
    VALUE_FORMAT='JSON'
) AS
  SELECT station_id, COUNT(station_id) AS count
  FROM turnstile
  GROUP BY station_id;

"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    # Ensure that a 2XX status code was returned
    
    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"failed creating turnstile_summary event: {json.dumps(resp.json(), indent=2)}")
        print(e)

if __name__ == "__main__":
    execute_statement()
