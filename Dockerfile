FROM apache/airflow:2.8.1
# FROM apache/airflow:2.8.1-python3.9


COPY requirements.txt /requirements.txt

RUN pip install --user --upgrade pip
RUN pip install --user --no-cache-dir -r /requirements.txt



# RUN apt update && \
#     apt-get install -y openjdk-17-jdk && \
#     apt-get install -y ant && \
#     apt-get clean;

# # Set JAVA_HOME
# ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
# RUN export JAVA_HOME


