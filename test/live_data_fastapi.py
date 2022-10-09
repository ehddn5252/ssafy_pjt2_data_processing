from fastapi import FastAPI
import statsapi
import json
app = FastAPI()


@app.get("/")
async def root():
    dates = statsapi.get('schedule', {'sportId': 1})['dates']

    result = dict()
    result["games"] = []
    for date in dates:
        games = date['games']
        for game in games:
            oneResult = dict()
            data = statsapi.get('game', {'gamePk': game['gamePk']})
            gameData = data["gameData"]
            livdData = data["liveData"]
            gamePk = data["gamePk"]
            season = gameData["game"]["season"]
            date = gameData["datetime"]["originalDate"]
            time = gameData["datetime"]["time"]
            ampm = gameData["datetime"]["ampm"]
            abstractGameState = gameData["status"]["abstractGameState"]
            detailedState = gameData["status"]["detailedState"]
            away_id = gameData["teams"]["away"]["id"]
            away_name = gameData["teams"]["away"]["name"]
            home_id = gameData["teams"]["home"]["id"]
            home_name = gameData["teams"]["home"]["name"]
            league_id = gameData["teams"]["away"]["league"]["id"]
            league_name = gameData["teams"]["away"]["league"]["name"]
            divison_id = gameData["teams"]["away"]["division"]["id"]
            divison_name = gameData["teams"]["away"]["division"]["name"]
            venue_id = gameData["venue"]["id"]
            venue_name = gameData["venue"]["name"]
            venue_time_offset = gameData["venue"]["timeZone"]["offset"]
            weather_temp = gameData["weather"]["temp"]
            weather_wind = gameData["weather"]["wind"]
            oneResult["gamePk"] = gamePk
            oneResult["season"] = season
            oneResult["date"] = date
            oneResult["time"] = time
            oneResult["ampm"] = ampm
            oneResult["gamePk"] = gamePk
            oneResult["abstractGameState"] = abstractGameState
            oneResult["detailedState"] = detailedState
            oneResult["away_id"] = away_id
            oneResult["away_name"] = away_name
            oneResult["home_id"] = home_id
            oneResult["home_name"] = home_name
            oneResult["league_id"] = league_id
            oneResult["league_name"] = league_name
            oneResult["divison_id"] = divison_id
            oneResult["divison_name"] = divison_name
            oneResult["venue_id"] = venue_id
            oneResult["venue_name"] = venue_name
            oneResult["venue_time_offset"] = venue_time_offset
            oneResult["weather_temp"] = weather_temp
            oneResult["weather_wind"] = weather_wind
            allPlays = livdData["plays"]["allPlays"]
            plays = []
            for i in range(40):
                plays.append([])
            index=0
            for play in allPlays:
                print(len(allPlays))
                index = -1
                index += play["about"]["inning"]
                index *= 2
                if (play["about"]["halfInning"] == "bottom"):
                    index += 1

                plays[index].append(dict())
                try:
                    plays[index][len(plays[index]) - 1]["event"] = (play["result"]["event"])
                    plays[index][len(plays[index]) - 1]["eventType"] = (play["result"]["eventType"])
                    plays[index][len(plays[index]) - 1]["description"] = (play["result"]["description"])
                    plays[index][len(plays[index]) - 1]["rbi"] = (play["result"]["rbi"])
                except:
                    pass
                plays[index][len(plays[index]) - 1]["startTime"] = (play["about"]["startTime"])
                plays[index][len(plays[index]) - 1]["endTime"] = (play["about"]["endTime"])
                plays[index][len(plays[index]) - 1]["isComplete"] = (play["about"]["isComplete"])
                plays[index][len(plays[index]) - 1]["batterId"] = (play["matchup"]["batter"]["id"])
                plays[index][len(plays[index]) - 1]["batterName"] = (play["matchup"]["batter"]["fullName"])
                plays[index][len(plays[index]) - 1]["batSide"] = (play["matchup"]["batSide"]["description"])
                plays[index][len(plays[index]) - 1]["pitcherName"] = (play["matchup"]["pitcher"]["fullName"])
                plays[index][len(plays[index]) - 1]["pitcherId"] = (play["matchup"]["pitcher"]["id"])
                plays[index][len(plays[index]) - 1]["pitchHand"] = (play["matchup"]["pitchHand"]["description"])



                playEvents = play["playEvents"]
                plays[index][len(plays[index]) - 1]["playEvents"] = []
                for playEvent in playEvents:
                    plays[index][len(plays[index]) - 1]["playEvents"].append(dict())
                    plays[index][len(plays[index]) - 1]["playEvents"][
                        len(plays[index][len(plays[index]) - 1]["playEvents"]) - 1]["type"] = playEvent["type"]
                    try:
                        plays[index][len(plays[index]) - 1]["playEvents"][len(plays[index][len(plays[index]) - 1]["playEvents"]) - 1]["description"] = playEvent["details"]["description"]

                    except:
                        pass
                    try:
                        plays[index][len(plays[index]) - 1]["playEvents"][
                            len(plays[index][len(plays[index]) - 1]["playEvents"]) - 1]["event"] = playEvent["details"][
                            "event"]
                        plays[index][len(plays[index]) - 1]["playEvents"][
                            len(plays[index][len(plays[index]) - 1]["playEvents"]) - 1]["eventType"] = \
                        playEvent["details"]["eventType"]
                    except:
                        pass
                    try:
                        plays[index][len(plays[index]) - 1]["playEvents"][
                            len(plays[index][len(plays[index]) - 1]["playEvents"]) - 1]["code"] = playEvent["details"][
                            "code"]
                        plays[index][len(plays[index]) - 1]["playEvents"][
                            len(plays[index][len(plays[index]) - 1]["playEvents"]) - 1]["ballCode"] = \
                        playEvent["details"]["type"]["code"]
                        plays[index][len(plays[index]) - 1]["playEvents"][
                            len(plays[index][len(plays[index]) - 1]["playEvents"]) - 1]["ballDescription"] = \
                        playEvent["details"]["type"]["description"]
                        plays[index][len(plays[index]) - 1]["playEvents"][
                            len(plays[index][len(plays[index]) - 1]["playEvents"]) - 1]["ballSpeed"] = \
                        playEvent["pitchData"]["startSpeed"]
                    except:
                        pass
            oneResult["gameData"] = []
            for p in plays:
                oneResult["gameData"].append(p)
            result["games"].append(oneResult)

    return result

@app.get("/test")
async def test():
    message = f"Hello world! From FastAPI running on Uvicorn with Gunicorn. Using Python "
    return {"message": message}
