# Description: Build the base image for the python application to not rebuild the requirements every time.
docker build -f ./Dockerfile -t base_image:latest .
