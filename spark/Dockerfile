FROM apache/spark:3.5.1

# Installer Python3 + pip
USER root
RUN apt-get update && apt-get install -y python3 python3-pip && apt-get clean

# Installer libs Python 
RUN pip3 install --upgrade pip
RUN pip3 install pyspark delta-spark importlib-metadata

USER spark
WORKDIR /app