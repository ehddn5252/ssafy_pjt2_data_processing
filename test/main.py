from fastapi import FastAPI
from test.web_test import Crawler
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
app = FastAPI()


@app.get("/")
async def root(item):
    return {"message": f"Hello World {item}"}


@app.get("/address_list")
async def address_list():
    store_locations = jsonable_encoder(Crawler.store_location_crawler())
    return JSONResponse(content=store_locations)


@app.get("/avg_grade_list")
async def avg_data():
    avg_grades = jsonable_encoder(Crawler.avg_grade_crawler())
    return JSONResponse(content=avg_grades)

