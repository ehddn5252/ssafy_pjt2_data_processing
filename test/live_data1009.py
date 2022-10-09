import statsapi
from pprint import pprint
dates = statsapi.get('schedule', {'sportId': 1})['dates']
for date in dates:
    games = date['games']
    for game in games:
        data = statsapi.get('game', {'gamePk': game['gamePk']})
        # pprint(data)
        livdData = data["liveData"]
        allPlays = livdData["plays"]["allPlays"]
        for play in allPlays:
            if play["about"]["inning"]==15:
                print(play["about"])
                exit(-1)
            # print(play["about"]["inning"])
