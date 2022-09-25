# live data 분석

작성일자: 20220925

    # 프로세스
    '''
    0. 모든 rawdata를 가져온다. 
    1. 게임마다 gameData.datetime.officalDate 에서 날짜를 가져온다. 여기에서 투수, 타자의 시즌을 구할 수 있다. 
       시즌은 년도 전체(2022 1920 ...)로 한다
    2. live data의 allPlays[i]의 match up 에서 투수 id와 타자 id를 각각 들고온다.
    3. 들고온 선수 타자와 년도에 매칭되는 값이 batters에 없다면 batters에 만들고 그 row를 가져온다. (pitchers도 마찬가지)
    3.1 만약에 매칭되는 값이 있다면 그 row를 가져온다. 

    4. matchup_data['batSide']['code'] 타자면 투수의 손 코드, 투수면 타자의 주손 코드를 가져와 left 에 저장할 지 right 에 저장할 지 정한다.
    5. event 에서 해당하는 값을 database 에 매핑해서 해당하는 값을 하나 저장하고 commit한다.
    '''


event (1만건 기준) 기준 
```python
{'Balk': 2,
 'Batter Out': 56,
 'Bunt Groundout': 1559,
 'Bunt Pop Out': 595,
 'Catcher Interference': 59,
 'Caught Stealing 2B': 1232,
 'Caught Stealing 3B': 62,
 'Caught Stealing Home': 50,
 'Defensive Sub': 1,
 'Defensive Switch': 1,
 'Double': 35546,
 'Double Play': 2269,
 'Field Error': 7741,
 'Fielders Choice': 443,
 'Fielders Choice Out': 1289,
 'Flyout': 119247,
 'Forceout': 17932,
 'Grounded Into DP': 15404,
 'Groundout': 143150,
 'Hit By Pitch': 6870,
 'Home Run': 21970,
 'Intent Walk': 5254,
 'Lineout': 23957,
 'Passed Ball': 1,
 'Pickoff 1B': 186,
 'Pickoff 2B': 59,
 'Pickoff 3B': 7,
 'Pickoff Caught Stealing 2B': 334,
 'Pickoff Caught Stealing 3B': 14,
 'Pickoff Caught Stealing Home': 15,
 'Pickoff Error 3B': 1,
 'Pitching Substitution': 5,
 'Pop Out': 33096,
 'Runner Double Play': 1,
 'Runner Out': 98,
 'Sac Bunt': 6567,
 'Sac Bunt Double Play': 6,
 'Sac Fly': 5802,
 'Sac Fly Double Play': 62,
 'Single': 118509,
 'Stolen Base 2B': 7,
 'Strikeout': 127221,
 'Strikeout Double Play': 844,
 'Strikeout Triple Play': 1,
 'Triple': 3762,
 'Triple Play': 16,
 'Walk': 63751,
 'Wild Pitch': 19,
 'etc': 9
}
```

event_type (5만건 기준)
```python
{'balk': 4, # 보크
 'catcher_interf': 166, # 포수의 간섭으로 인한 1base 전진
 'caught_stealing_2b': 2731, # 2루로 가는 도루 저지
 'caught_stealing_3b': 134, # 3루로 가는 도루 저지
 'caught_stealing_home': 112, # 홈으로 가는 도루 저지
 'defensive_indiff': 1, # 수비적 무관심(도루를 시도해도 도루를 막지 않으려 함)
 'defensive_substitution': 1, # 수비 교체
 'defensive_switch': 2, # 수비 쉬프트
 'double': 85658, # 2루타
 'double_play': 5375, # 더블플레이
 'etc': 16, # 기타
 'field_error': 17463, # 수비 에러 
 'field_out': 776381, # 아웃
 'fielders_choice': 1860, # 야수 선택
 'fielders_choice_out': 3222, # 야수 선택 아웃
 'force_out': 42325, # 포스플레이 아웃(루상에 발을 밞아서 아웃시킴)
 'grounded_into_double_play': 37073, # 땅볼로 인한 병살
 'hit_by_pitch': 16907, # 공에 맞음
 'home_run': 50814, # 홈런
 'injury': 1, # 부상
 'intent_walk': 12712 , # 고의 사구
 'other_advance': 1,
 'other_out': 227,
 'passed_ball': 5, # 포수 책임으로 투수가 던진 볼을 못잡아 타자가 진루하는 경우
 'pickoff_1b': 459, # 1루 견제
 'pickoff_2b': 140, # 2루 견제
 'pickoff_3b': 18, # 3루 견제
 'pickoff_caught_stealing_2b': 728, # 2루 견제 아웃
 'pickoff_caught_stealing_3b': 30, # 3루 견제 아웃
 'pickoff_caught_stealing_home': 23, # 홈 견제 아웃
 'pickoff_error_1b': 1, # 1루 견제 에러
 'pickoff_error_2b': 1, # 2루 견제 에러
 'pickoff_error_3b': 1, # 3루 견제 아웃
 'pitching_substitution': 9, #
 'runner_double_play': 8, # 주자로 인한 더블플레이
 'sac_bunt': 16062, # 희생번트
 'sac_bunt_double_play': 14, # 희생번트로 인한 더블플레이
 'sac_fly': 13340, # 희생 플라이
 'sac_fly_double_play': 155, #희생 플레이로 인한 더블 플레이
 'single': 284502, # 안타
 'stolen_base_2b': 20, # 2루 도루
 'stolen_base_3b': 1, # 3루 도루
 'stolen_base_home': 2, # 홈 도루
 'strikeout': 302712, # 스트라이크 아웃
 'strikeout_double_play': 1804, # 스트라이크 아웃 + 도루 아웃
 'strikeout_triple_play': 2, # 스트라이크 아웃 + 도루 2번 다 아웃
 'triple': 9100, # 3루타
 'triple_play': 31, # 트리플 아웃
 'walk': 145666, # 볼넷
 'wild_pitch': 38 # 와일드 피치
 } 

dict_keys(['walk', 'single', 'force_out', 'field_out', 'strikeout', 'double', 'sac_bunt', 'home_run', 'field_error', 'hit_by_pitch', 'grounded_into_double_play', 'caught_stealing_2b', 'triple', 'intent_walk', 'fielders_choice_out', 'double_play', 'sac_fly', 'pickoff_caught_stealing_2b', 'strikeout_double_play', 'caught_stealing_home', 'fielders_choice', 'other_out', 'pickoff_1b', 'sac_fly_double_play', 'caught_stealing_3b', 'wild_pitch', 'pickoff_caught_stealing_3b', 'pickoff_2b', 'pickoff_3b', 'pickoff_caught_stealing_home', 'catcher_interf', 'pitching_substitution', 'stolen_base_2b', 'triple_play', 'defensive_switch', 'balk', 'defensive_substitution', 'runner_double_play', 'sac_bunt_double_play', 'pickoff_error_3b', 'etc', 'passed_ball', 'strikeout_triple_play', 'pickoff_error_2b', 'stolen_base_3b', 'pickoff_error_1b', 'defensive_indiff', 'injury', 'other_advance', 'stolen_base_home'])

 ```
 
 ## 이벤트에 따라 매칭되는 column
 ```python 
 {'balk': 2, # 보크
 'catcher_interf': 59, # 포수의 간섭으로 인한 1base 전진
 'caught_stealing_2b': 1232, # 2루로 가는 도루 저지
 'caught_stealing_3b': 62, # 3루로 가는 도루 저지
 'caught_stealing_home': 50, # 홈으로가는 도루 저지
 'defensive_substitution': 1, # 수비 교체
 'defensive_switch': 1, # 수비 쉬프트
 'double': 35546, # 
 'double_play': 2269, # 더블플레이
 'etc': 9, # 에러
 'field_error': 7741, # 필드 에러
 'field_out': 321660, # 필드 에러
 'fielders_choice': 443, # 야수 선택
 'fielders_choice_out': 1289, # 야수 선택 아웃
 'force_out': 17932,
 'grounded_into_double_play': 15404,
 'hit_by_pitch': 6870,
 'home_run': 21970,
 'intent_walk': 5254,
 'other_out': 98,
 'passed_ball': 1,
 'pickoff_1b': 186,
 'pickoff_2b': 59,
 'pickoff_3b': 7,
 'pickoff_caught_stealing_2b': 334,
 'pickoff_caught_stealing_3b': 14,
 'pickoff_caught_stealing_home': 15,
 'pickoff_error_3b': 1,
 'pitching_substitution': 5,
 'runner_double_play': 1,
 'sac_bunt': 6567,
 'sac_bunt_double_play': 6,
 'sac_fly': 5802,
 'sac_fly_double_play': 62,
 'single': 118509,
 'stolen_base_2b': 7,
 'strikeout': 127221,
 'strikeout_double_play': 844,
 'strikeout_triple_play': 1,
 'triple': 3762,
 'triple_play': 16,
 'walk': 63751,
 'wild_pitch': 19}
 ```
