from datetime import timedelta
import os

class Config:
    JWT_SECRET_KEY = os.environ.get("JWT_SECRET_KEY", "2b4fabbbcc38336a91666f74c784a505deecc70a8653d6924a426e0921a4b638")
    JWT_ACCESS_TOKEN_EXPIRES = timedelta(minutes=1)
    JWT_REFRESH_TOKEN_EXPIRES = timedelta(days=30)
    JWT_TOKEN_LOCATION = ["headers", "cookies"]
    JWT_COOKIE_SECURE = False
    JWT_COOKIE_CSRF_PROTECT = False
    JWT_ACCESS_COOKIE_NAME = "access_token"
    JWT_REFRESH_COOKIE_NAME = "refresh_token"
    MYSQL_HOST = "localhost"
    MYSQL_USER = "root"
    MYSQL_PASSWORD = ""
    MYSQL_DB = "financelake"
    BOOTSTRAP_SERVERS = "localhost:9092"
    ACCESS_EXPIRES = timedelta(seconds=60)
