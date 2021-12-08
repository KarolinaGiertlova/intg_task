import uvicorn
import asyncio
from fastapi import FastAPI
import httpx
from httpx import ConnectTimeout, ReadTimeout
import logging
import time


app = FastAPI()
logging.basicConfig(level=logging.DEBUG)
test_server_url = "https://exponea-engineering-assignment.appspot.com/api/work"


@app.get("/api/smart")
async def get_test_response(timeout: int):
    queue = asyncio.Queue(1)
    found_response = asyncio.Queue()

    start = time.perf_counter()
    async with httpx.AsyncClient() as client:
        main = asyncio.create_task(get_first_response(queue, timeout, start, found_response, client))

        try:
            return await asyncio.wait_for(queue.get(), timeout=timeout/1000)
        except asyncio.TimeoutError:
            return {'error': 'No successful response within this timeout.'}


async def get_first_response(queue, timeout, start, found_response, client):
    c1 = asyncio.create_task(send_test_request(queue, timeout, start, found_response, client))
    await asyncio.sleep(0.3)
    if not found_response.empty():
        logging.info("First task already finished.")
        return
    logging.info("Starting two new tasks.")
    c2 = asyncio.create_task(send_test_request(queue, timeout, start, found_response, client))
    c3 = asyncio.create_task(send_test_request(queue, timeout, start, found_response, client))


async def send_test_request(queue, timeout, start, found_response, client):
    try:
        response = await client.get(test_server_url, timeout=timeout/1000)
        json_response = response.json()
        logging.info(json_response)
        logging.info(f'actual time: {(time.perf_counter() - start)*1000}')
        if "time" in json_response:
            await found_response.put("yes")
            await queue.put(json_response)
    except ValueError:
        logging.warning("Not valid payload from the test server.")
    except (ConnectTimeout, ReadTimeout, RuntimeError):
        logging.warning("Request to test server timed out.")


if __name__ == '__main__':
    uvicorn.run("app_fastapi:app", port=8000, host='127.0.0.1', reload=True, debug=True)
