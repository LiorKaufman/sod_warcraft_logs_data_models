from typing import Any, Dict
import requests
from pydantic import BaseModel
import json
import os
from pydantic_settings import BaseSettings
import logging


class WarcraftLogsAPISettings(BaseSettings):
    api_endpoint: str = "https://sod.warcraftlogs.com/api/v2/client"
    client_id: str
    client_secret: str
    token_url: str = "https://www.warcraftlogs.com/oauth/token"

    class Config:
        # Specifies that Pydantic should read environment variables from a .env file
        env_file = ".env"
        env_file_encoding = 'utf-8'


class WarcraftLogsAPI:
    def __init__(self):
        self.settings = WarcraftLogsAPISettings()
        self.headers = self.retrieve_headers()

    def read_token(self):
        try:
            with open(".credentials.json", mode="r+", encoding="utf-8") as f:
                access_token = json.load(f)
                return access_token.get("access_token")
        except OSError as e:
            print(e)
            return None

    def retrieve_headers(self) -> dict[str, str]:
        self.get_token()
        return {"Authorization": f"Bearer {self.read_token()}"}

    def get_token(self, store: bool = True):
        data = {"grant_type": "client_credentials"}
        auth = (os.environ.get("client_id"), os.environ.get("client_secret"))
        with requests.Session() as session:
            response = session.post(
                self.settings.token_url, data=data, auth=auth)
            if store and response.status_code == 200:
                self.store_token(response=response)
    # return response

    def store_token(response):
        try:
            with open(".credentials.json", mode="w+", encoding="utf-8") as f:
                json.dump(response.json(), f)
        except OSError as e:
            print(e)
        return None

    def get_report_data_by_report_id(self, report_id: str) -> Dict[str, Any]:
        query = """
        query ($report_id: String!) {
            reportData {
                report(code: $report_id) {
                    code
                    startTime
                    endTime
                    region {
                        id 
                        name
                        compactName
                    }
                    guild{
                        id
                        name 
                    }
                    fights (killType: Encounters){
                        id
                        encounterID
                        name
                        kill
                        endTime
                        startTime
                        size
                        originalEncounterID
                    }
                    rankedCharacters {
                        id
                        canonicalID
                        name
                        classID
                        level
                        
                    }
                    masterData(translate: true) {
                        actors(type: "Player") {
                            id
                            gameID
                            server
                            subType
                            name
                        }
                    }
                }
            }
        }
        """
        return self.get_data(query=query, report_id=report_id)

    def get_character_data_by_character_id(self, character_id: str) -> Dict[str, Any]:
        pass

    def get_data(self, query: str, **kwargs):
        data = {"query": query, "variables": kwargs}
        with requests.Session() as session:
            session.headers = self.headers
            # Use POST for GraphQL queries
            response = session.post(url=self.settings.api_endpoint, json=data)

            # Check if the response status code is not 200
            if response.status_code != 200:
                # Log an error message with the status code and returned message
                logging.error(f"""Failed to fetch data: HTTP {
                              response.status_code} - {response.text}""")
                # Decide how to handle errors, e.g., return None or a specific structure
                return None

            return response.json()
