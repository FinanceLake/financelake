from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit
from ingestion_simulator import simulator

app = Flask(__name__)
socketio = SocketIO(app)

@app.route('/')
def dashboard():
    return render_template('dashboard.html')

@app.route('/status')
def status():
    return jsonify(simulator.get_status())

@app.route('/start', methods=['POST'])
def start_ingestion():
    simulator.start()
    return jsonify({"message": "Ingestion started."})

@app.route('/pause', methods=['POST'])
def pause_ingestion():
    simulator.pause()
    return jsonify({"message": "Ingestion paused."})

# WebSocket real-time update
@socketio.on('request_status')
def handle_request_status():
    emit('status_update', simulator.get_status())

if __name__ == '__main__':
    socketio.run(app, debug=True)
