FROM ubuntu:20.04

# Set environment variables to avoid interactive prompts during installation
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=UTC
WORKDIR /app

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
    python3=3.8.2-0ubuntu2 \
    python3-dev=3.8.2-0ubuntu2 \
    python3-pip=20.0.2-5ubuntu1.10

# # Set the timezone (optional)
RUN ln -fs /usr/share/zoneinfo/$TZ /etc/localtime && dpkg-reconfigure -f noninteractive tzdata


COPY requirements.txt .

# install python requirements
RUN /usr/bin/pip install -r ./requirements.txt