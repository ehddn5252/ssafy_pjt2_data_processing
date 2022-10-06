# data manage
- 작성일: 2022.10.02
- 데이터를 쌓는 과정을 보여줍니다.

## 데이터 쌓기

### 크롤링
1. 크롤링을 통한 선수 이미지 데이터 쌓기
- baseball_player DB에서 player_uid에 맞는 img_url 을 저장하는 로직
- 3~5초에 하나 크롤링, 총 15만건, 60만초 -> 166.7시간 걸림

### statAPI를 통한 데이터 쌓기
1. 경기 schedule 데이터 쌓기
    - 21만건
2. teams data 쌓기
    - 651 건 
3. 선수 데이터 쌓기
    - 15만건
4. schedules 게임을 기반으로 매 경기의 모든 정보가 있는 raw_data 쌓기
    - 214,583건

5.  
    - 1초에 row 한개
    - 21만건 약 60시간
    - 스파크 사용결과는 비슷함 (DB 에 넣는 작업이라 sqoop을 사용해야 더 빨리 될 것이라 예상)

## 데이터 가공
5. raw_data 넣기 
    - 1초에 row 한개
    - 스파크 사용결과는 비슷함 (DB 에 넣는 작업이라 sqoop을 사용해야 더 빨리 될 것이라 예상)


5. raw_data 에서 event 에 따른 데이터 가공하여 events table 생성 및 데이터 쌓기
    - raw_data 12만건(100GB) -> 2340만건(3GB)의 events table 로 변환
    - 기존 방식: 약 16시간

6. events table의 데이터를 가공, 집계하여 event_pitchers와 event_batters (시즌별, 이밴트, 상대 손 별 타자 성적 DB)생성 및 데이터 쌓기
    - 2300만건 -> event_pitchers 약 110만건 evnet_batters 약 100만건 
    - 기존 방식: 약 1시간

7. event_pitchers와 event_batters에서 pitchers와 batters 테이블(시즌별 선수 게임 데이터 집계) 쌓기
   110만건 -> 43000건, 100만건 -> 4만건
    - 기존 방식: 3분 + 8시간


## 데이터 업데이트
### [5초마다 업데이트해야 할 정보] 
- 경기 실시간 중계 데이터는 5초마다 업데이트 합니다.

### [1분마다 업데이트해야 할 정보]
- 경기 실시간 현황을 보여주는 데이터는 1분에 한번씩 업데이트 합니다. (schedules)

### [매일 00:00분  업데이트해야 할 정보 ]
- 경기가 예정된 경기 정보(today + 14일후 경기 예정 정보까지 업데이트)
- 어제 경기 정보 (schedules 에 경기 결과 업데이트) 
- yesterday raw_data 를 pitchers and baseball 로 만들기
    - 어제 경기한 정보(win team,lose team, home_team score, away_team score)를 
     raw_data 에서 pitchers와 batters table로 데이터 업데이트 작업
    - 데이터 처리 flow는 다음과 같다. 
        - raw_data to tmp_event
        - tmp_event to tmp_event_pitchers and tmp_event to tmp_event_batters
        - event_pitchers to pitchers and event_batters to batters
    - update 로직은 pitchers 의 해당 시즌 정보를 가져와서 어제 경기에서 나온 것만큼 + 해서 다시 넣기


## 지금 해야 할 것
1. 데이터 처리 과정 파이썬으로 다 정리하기
2. 데이터 처리 과정 air flow 에 등록하기 
3. 등록 후 spark 로 데이터 처리 변경하기

