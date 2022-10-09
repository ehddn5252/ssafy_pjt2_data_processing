import statsapi
dates = statsapi.get('schedule', {'sportId': 1})['dates']
print(dates)
print("end")