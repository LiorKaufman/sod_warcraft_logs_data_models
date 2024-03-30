import requests
import os
import json

token_url = "https://www.warcraftlogs.com/oauth/token"


def get_token(store: bool = True):
    data = {"grant_type": "client_credentials"}
    auth = (os.environ.get("client_id"), os.environ.get("client_secret"))
    with requests.Session() as session:
        response = session.post(token_url, data=data, auth=auth)
        if store and response.status_code == 200:
            store_token(response=response)
    # return response


def store_token(response):
    try:
        with open(".credentials.json", mode="w+", encoding="utf-8") as f:
            json.dump(response.json(), f)
    except OSError as e:
        print(e)
        return None


def read_token():
    try:
        with open(".credentials.json", mode="r+", encoding="utf-8") as f:
            access_token = json.load(f)
            return access_token.get("access_token")
    except OSError as e:
        print(e)
        return None


def retrieve_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {read_token()}"}


def get_data(query: str, url: str, **kwargs):
    # headers = {"Authorization": f"Bearer {read_token()}"}
    data = {"query": query, "variables": kwargs}
    with requests.Session() as session:
        session.headers = retrieve_headers()
        response = session.get(url, json=data)

        return response.json()


def main():

    # get_token()
    query = """query($code:String){
                reportData{
                    report(code:$code){
                    fights(difficulty:3){
                        id
                        name
                        startTime
                        endTime
                    }
                    }
                }
    }"""

    char_data_query = """query {
  characterData {
    character(name: "Donalfonso", serverSlug: "wild-growth", serverRegion: "US") {
      id
      canonicalID
      classID
      gameData
      guildRank
      name
      zoneRankings
      server {
      name
      id 
      }
      }
  }
}

"""

    tt = """query {
    worldData {
        server(id: 10222) {
        id
        name
        slug
        region {
            id
            name
            compactName
        }
        }
    }
    }"""

    url = "https://www.warcraftlogs.com/api/v2/client"
    sod_url = "https://sod.warcraftlogs.com/api/v2/client"
    # print(response.json())
    # res = get_data(query=query, url=url, code="1yRfJ6aQCPXdGmnF")
    res = get_data(query=char_data_query, url=sod_url,
                   name="donalfonso", server="wild-growth", region="us")
    print(res)
    res = get_data(query=tt, url=sod_url,
                   )
    print(res)


if __name__ == "__main__":
    main()
