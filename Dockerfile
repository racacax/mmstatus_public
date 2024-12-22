# ===== builder step =====
FROM python:3.11-buster AS builder

# set work directory
WORKDIR /app

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV DEBCONF_NOWARNINGS=yes
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Europe/Paris

# System layer
RUN apt update
# Javascript dependencies layer
ENV NVM_DIR="/root/.nvm"
ENV NODE_VERSION 22.11.0
RUN ["mkdir", "/install"]
RUN curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.2/install.sh | bash && . $NVM_DIR/nvm.sh \
    && nvm install $NODE_VERSION --latest-npm && npm install -g yarn
ENV NODE_PATH $NVM_DIR/v$NODE_VERSION/lib/node_modules
ENV PATH      $NVM_DIR/v$NODE_VERSION/bin:$PATH

# Python dependencies layer
COPY requirements.txt requirements.txt /app/
RUN pwd && ls -la && pip install --upgrade pip && \
    pip install -r requirements.txt

COPY package.json /install/
RUN . $NVM_DIR/nvm.sh && cd /install && yarn install

