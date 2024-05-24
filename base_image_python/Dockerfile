FROM ubuntu:20.04

# Set environment variables to avoid interactive prompts during installation
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=UTC

RUN apt-get update
RUN apt-get install -y software-properties-common
RUN add-apt-repository ppa:deadsnakes/ppa

# Update package lists and install basic utilities
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    tzdata \
    curl \
    unzip \
    wget \
    gnupg \
    ca-certificates \
    lsb-release \
    gettext \
    build-essential=12.8ubuntu1.1 \
    python3.12 \
    python3.12-dev

RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
RUN python3.12 get-pip.py

RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 1

# # Set the timezone (optional)
RUN ln -fs /usr/share/zoneinfo/$TZ /etc/localtime && dpkg-reconfigure -f noninteractive tzdata

WORKDIR /app

COPY requirements.txt .

# install python requirements
RUN python3 -m pip install -r ./requirements.txt