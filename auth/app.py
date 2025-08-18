from flask import Flask, jsonify, request, g, make_response
from flask_jwt_extended import (
    JWTManager, create_access_token, create_refresh_token,
    jwt_required, get_jwt_identity, decode_token, get_jwt
)
from jwt.exceptions import ExpiredSignatureError, InvalidTokenError
import pymysql
from config import Config
from werkzeug.security import generate_password_hash, check_password_hash
from kafka import KafkaConsumer

app = Flask(__name__)
app.config.from_object(Config)

# JWT configuration
jwt = JWTManager(app)

def get_db_connection():
    return pymysql.connect(
        host=app.config['MYSQL_HOST'],
        user=app.config['MYSQL_USER'],
        password=app.config['MYSQL_PASSWORD'],
        database=app.config['MYSQL_DB'],
        cursorclass=pymysql.cursors.DictCursor
    )

def create_tables():
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    username VARCHAR(50) UNIQUE NOT NULL,
                    password VARCHAR(255) NOT NULL
                )
            """)
            conn.commit()
    finally:
        conn.close()

@app.route('/register', methods=['POST'])
def register():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')

    if not username or not password:
        return jsonify({"msg": "Missing username or password"}), 400

    hashed_password = generate_password_hash(password)

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO users (username, password) VALUES (%s, %s)",
                (username, hashed_password)
            )
            conn.commit()
        return jsonify({"msg": "User created successfully"}), 201
    except pymysql.err.IntegrityError:
        return jsonify({"msg": "Username already exists"}), 400
    finally:
        conn.close()

@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT * FROM users WHERE username = %s", (username,)
            )
            user = cursor.fetchone()
            
        if user and check_password_hash(user['password'], password):
            access_token = create_access_token(identity=username)
            refresh_token = create_refresh_token(identity=username)
            
            response = jsonify({
                "access_token": access_token,
                "refresh_token": refresh_token
            })
            
            response.set_cookie(
                'access_token',
                value=access_token,
                httponly=True,
                max_age=15 * 60
            )
            response.set_cookie(
                'refresh_token',
                value=refresh_token,
                httponly=True,
                max_age=30 * 24 * 60 * 60
            )
            
            return response, 200
        else:
            return jsonify({"msg": "Invalid credentials"}), 401
    finally:
        conn.close()

@app.route('/refresh', methods=['POST'])
@jwt_required(refresh=True)
def refresh():
    current_user = get_jwt_identity()
    new_access_token = create_access_token(identity=current_user)
    
    response = jsonify({"access_token": new_access_token})
    response.set_cookie(
        'access_token',
        value=new_access_token,
        httponly=True,
        max_age=15 * 60
    )
    return response, 200

@app.before_request
def auto_refresh_token():
    if request.endpoint in ['register', 'login', 'refresh']:
        return

    refresh_token = request.cookies.get('refresh_token')
    
    try:
        if refresh_token:
            try:
                decoded_refresh = decode_token(refresh_token)
                identity = decoded_refresh['sub']
                new_access_token = create_access_token(identity=identity)
                g.jwt_identity = identity
                g.new_access_token = new_access_token
                response = make_response()
                response.set_cookie(
                'access_token',
                value=g.new_access_token,
                httponly=True,
                max_age=15 * 60
                )
                response.headers['X-New-Access-Token'] = g.new_access_token
                return
            except ExpiredSignatureError:
                return jsonify({"msg": "Refresh token expired"}), 401
            except InvalidTokenError:
                return jsonify({"msg": "Invalid refresh token"}), 401
        else:
            return jsonify({"msg": "Refresh token missing"}), 401
    except Exception as e:
        return jsonify({"msg": str(e)})
    

# @app.after_request
# def set_new_access_token(response):
#     if hasattr(g, 'new_access_token'):
#         response.set_cookie(
#             'access_token',
#             value=g.new_access_token,
#             httponly=True,
#             max_age=15 * 60
#         )
#         response.headers['X-New-Access-Token'] = g.new_access_token
#     return response

@app.route('/kafka/topics', methods=['GET'])
@jwt_required()
def get_kafka_topics():
    try:
        consumer = KafkaConsumer(bootstrap_servers=app.config["BOOTSTRAP_SERVERS"])
        topics = consumer.topics()

        topic_name = request.args.get('name')
        topic_metadata = []

        if topic_name:
            if topic_name in topics:
                partitions = consumer.partitions_for_topic(topic_name)
                partition_count = len(partitions) if partitions else 0
                topic_metadata.append({
                    "name": topic_name,
                    "partitions": partition_count
                })
            else:
                return jsonify({"msg": f"Topic '{topic_name}' not found"}), 404
        else:
            for topic in topics:
                partitions = consumer.partitions_for_topic(topic)
                partition_count = len(partitions) if partitions else 0
                topic_metadata.append({
                    "name": topic,
                    "partitions": partition_count
                })

        current_user = get_jwt_identity()

        return jsonify({
            "user": current_user,
            "kafka_data": {
                "topics": topic_metadata
            }
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    create_tables()
    app.run(debug=True)
