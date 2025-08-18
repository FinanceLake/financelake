# Flask Kafka API with JWT Authentication

This project is a secure Flask API that integrates with **Kafka** to fetch topic metadata, and with **MySQL** for user authentication. It uses **JWT** tokens stored in cookies to manage access and refresh tokens securely.

---

## 🚀 Features

- ✅ User registration and login
- 🔐 JWT-based authentication with access/refresh token cookies
- ♻️ Auto-refresh of access tokens using refresh tokens
- 🗂️ Retrieve all Kafka topics or a specific topic by name
- 🛡️ Protected endpoints using `@jwt_required()`

---

## 📦 Tech Stack

- **Flask**: Web framework
- **MySQL (via PyMySQL)**: User storage
- **Kafka (via kafka-python)**: Message broker integration
- **Flask-JWT-Extended**: JWT management
- **PyJWT**: For decoding and error handling
- **Werkzeug**: Secure password hashing

---

## 📁 Project Structure

project/
├── app.py # Main application file
├── config.py # Configuration class (not included here)
├── requirements.txt # Python dependencies
└── README.md # This file

---

## ⚙️ Setup

## ✅ Start the Flask Server
python app.py

## 🧪 API Endpoints

### 🔐 Authentication

#### Register

POST /register
Body:
json
{ "username": "user1", "password": "pass123" }

#### Login

POST /login
Body:
json
{ "username": "user1", "password": "pass123" }
Returns access_token and refresh_token as HTTP-only cookies.

#### Refresh Token

POST /refresh
Uses refresh_token from cookies to return a new access token.

#### Get All Topics

GET /kafka/topics

#### Get Topic By Name

GET /kafka/topics?name=your_topic_name

curl -X GET http://127.0.0.1:5000/kafka/topics \
  --cookie "access_token=YOUR_ACCESS_TOKEN"