from flask import Flask, request, jsonify
import requests
import threading
from queue import Queue
import time
from requests.exceptions import ConnectTimeout, ReadTimeout
import logging

app = Flask(__name__)
app.logger.setLevel(logging.INFO)


@app.route('/api/smart')
def get_test_response():
    queue = Queue()
    if 'timeout' in request.args:
        try:
            timeout = int(request.args['timeout'])
        except ValueError:
            error_message = {'error': 'Timeout attribute is not an integer.'}
            return jsonify(error_message), 400

        start = time.time()
        main = threading.Thread(target=get_first_response_parallel, args=(queue, timeout, start))
        main.start()

        while time.time() - start < timeout/1000:
            if not queue.empty():
                return queue.get()

        error_message = {'error': 'No successful response within this timeout.'}
        return jsonify(error_message), 500
    else:
        error_message = {'error': 'Timeout was not specified.'}
        return jsonify(error_message), 400


def get_first_response_parallel(queue, timeout, start):
    t1 = threading.Thread(target=send_test_request, args=(queue, timeout, start))
    t1.start()

    t1.join(0.3)
    if not queue.empty():
        app.logger.info("First thread already finished.")
        return
    else:
        app.logger.info("Starting two new threads.")
        t2 = threading.Thread(target=send_test_request, args=(queue, timeout, start))
        t3 = threading.Thread(target=send_test_request, args=(queue, timeout, start))
        t2.start()
        t3.start()


def send_test_request(queue, timeout, start):
    try:
        response = requests.get(url="https://exponea-engineering-assignment.appspot.com/api/work", timeout=timeout/1000)
        app.logger.info(str(response.content) + f" actual time: {str(time.time() - start)}")

        json_response = response.json()
        if "time" in json_response:
            queue.put(json_response)
    except ValueError:
        app.logger.warning("Not valid payload from the test server.")
    except (ConnectTimeout, ReadTimeout):
        app.logger.warning("Request to test server timed out.")


if __name__ == '__main__':
    app.run(threaded=True)
