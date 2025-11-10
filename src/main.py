from flask import Flask
import logging

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)

@app.route('/')
def hello():
    app.logger.info('ok function')
    return 'Hello, World!'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)