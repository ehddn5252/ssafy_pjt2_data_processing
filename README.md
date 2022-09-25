# Crawler

사용법
## 1. 가상환경 설정

1. 파이참에서 프로젝트를 엽니다.
2. Settings에 들어갑니다.

    ![image](https://user-images.githubusercontent.com/51036842/188261791-e21c8ff2-86b3-430c-95f9-7c693d30c75b.png)

3. 좌측에서 Project:프로젝트명으로 된 메뉴를 선택합니다.

4. 메뉴를 확장하면 보이는 Project Interpreter를 선택합니다.
    
5. Project Interpreter 경로 옆에 보이는 설정 버튼을 클릭합니다
    ![image](https://user-images.githubusercontent.com/51036842/188261863-81a8f3ef-8ec9-4512-96a7-2a2f3eed31ad.png)

6. venv를 create 해줍니다.

    ![image](https://user-images.githubusercontent.com/51036842/188261958-184f5813-54fc-4595-844b-751c74f3d73b.png)

7. venv의 Scripts에 가서 가상환경을 실행시켜줍니다.
- venv의 Scripts의 activate 를 실행해줍니다.
```
// 본인의  있는 곳 (crwaler)
cd venv\Scripts

# activate 파일 실행
activate
```
그러면 좌측에 가상환경이 생깁니다.
![image](https://user-images.githubusercontent.com/51036842/188262076-0a18eaf3-243d-4d0c-9e06-68c6898242f6.png)

## 2.  라이브러리 다운로드

crwaler 폴더의 venv 환경에서 다음 명령어를 실행합니다.
```
pip install -r requirements.txt
```

## 3. 자신의 이름에 맞는 부분의 주석을 풀어주세요
main.py
```python
    # 동우
    CrawlingGoogleImg.main(players[0:row_num1])
    # 찬호
    # CrawlingGoogleImg.main(players[row_num1:row_num2])
    # 예림
    # CrawlingGoogleImg.main(players[row_num2:row_num3])
```

## 4. 파일 실행하기
    ```
    python main.py
    ```