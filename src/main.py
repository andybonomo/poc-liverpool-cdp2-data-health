from flask import Flask, request, jsonify
import logging

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)

@app.route('/', methods=['GET'])
def hello():
    app.logger.info('ok function')
    return 'Hello, World!'

@app.route('/test-avisco', methods=['POST'])
def test_avisco():
    app.logger.info('Received POST on /test-avisco')
    # Puedes acceder a los datos con request.form, request.json, etc. si lo necesitas.
    return jsonify({"message": "Aviso recibido", "status": "success"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)