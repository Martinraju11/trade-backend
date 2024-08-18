from flask import Flask, jsonify
from celery import Celery
from protrader import main
import datetime
app = Flask(__name__)

# Configure Celery to use the in-memory broker and backend
app.config['CELERY_BROKER_URL'] = 'memory://'
app.config['CELERY_RESULT_BACKEND'] = 'cache+memory://'

def make_celery(app):
    celery = Celery(
        app.import_name,
        broker=app.config['CELERY_BROKER_URL'],
        backend=app.config['CELERY_RESULT_BACKEND']
    )
    celery.conf.update(app.config)
    return celery

celery = make_celery(app)

@celery.task
def run_protrader():
    print("Running ProTrader Task")
    print(datetime.datetime.now())  # Asynchronously start the task

    main()  # Assuming 'main' is your trading logic function

@app.route('/start_protrader', methods=['POST'])
def start_protrader():
    print("Starting ProTrader Task")
    run_protrader()
    print("2",datetime.datetime.now())  # Asynchronously start the task
    return jsonify({"status": "ProTrader started successfully!"})

if __name__ == '__main__':
    app.run(port=5001, debug=True)
