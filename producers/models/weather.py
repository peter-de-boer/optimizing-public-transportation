"""Methods pertaining to weather data"""
from enum import IntEnum
import json
import logging
from pathlib import Path
import random
import urllib.parse

import requests

from models.producer import Producer


logger = logging.getLogger(__name__)


class Weather(Producer):
    """Defines a simulated weather model"""

    status = IntEnum(
        "status", "sunny partly_cloudy cloudy windy precipitation", start=0
    )

    rest_proxy_url = "http://localhost:8082"

    key_schema = None
    value_schema = None

    winter_months = set((0, 1, 2, 3, 10, 11))
    summer_months = set((6, 7, 8))

    def __init__(self, month):
        #
        #
        # DONE: Complete the below by deciding on a topic name, number of partitions, and number of
        # replicas
        #
        #
        # it seems the parent Producer, avroProducer, etc. does not really affect the rest proxy
        super().__init__(
            "nd.project.opt.weather", # DONE: Come up with a better topic name
            key_schema=Weather.key_schema,
            value_schema=Weather.value_schema,
        #   num_partitions=10, # DONE
            num_replicas=3,    # DONE
        )

        self.topic_name = "nd.project.opt.weather"
        self.status = Weather.status.sunny
        self.temp = 70.0
        if month in Weather.winter_months:
            self.temp = 40.0
        elif month in Weather.summer_months:
            self.temp = 85.0

        if Weather.key_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_key.json") as f:
                Weather.key_schema = json.load(f)

        #
        # DONE: Define this value schema in `schemas/weather_value.json
        #
        if Weather.value_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_value.json") as f:
                Weather.value_schema = json.load(f)

    def _set_weather(self, month):
        """Returns the current weather"""
        mode = 0.0
        if month in Weather.winter_months:
            mode = -1.0
        elif month in Weather.summer_months:
            mode = 1.0
        self.temp += min(max(-20.0, random.triangular(-10.0, 10.0, mode)), 100.0)
        self.status = random.choice(list(Weather.status))

    def run(self, month):
        self._set_weather(month)

        #
        #
        # DONE: Complete the function by posting a weather event to REST Proxy. Make sure to
        # specify the Avro schemas and verify that you are using the correct Content-Type header.
        #
        #
        logger.info(f"posting weather event, temp: {self.temp:.2f}; status {self.status.name}")
        logger.debug(f"value_schema: {Weather.value_schema}")
        logger.debug(f"key_schema: {Weather.key_schema}")
        resp = requests.post(
            #
            #
            # DONE: What URL should be POSTed to?
            #
            #
            f"{Weather.rest_proxy_url}/topics/{self.topic_name}",
            #
            #
            # DONE: What Headers need to bet set?
            #
            #
            headers={"Content-Type": "application/vnd.kafka.avro.v2+json"},
            data=json.dumps(
                {
                    #
                    #
                    # DONE: Provide key schema, value schema, and records
                    #
                    #
                    "key_schema":   json.dumps(Weather.key_schema),
                    "value_schema": json.dumps(Weather.value_schema),
                    "records": [
                        {
                            "key":  {
                                "timestamp": self.time_millis()
                            },
                            "value": {
                                "temperature": self.temp,
                                "status": self.status.name
                            }
                        }
                    ]
                }
            ),
        )

        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f"failed creating weather event: {json.dumps(resp.json(), indent=2)}")
            print(e)

        logger.debug(
            "sent weather data to kafka, temp: %s, status: %s",
            self.temp,
            self.status.name,
        )
