# Use an official Python runtime as a parent image
FROM python:3.7-alpine3.8

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

RUN mkdir /var/log/loaders

# Install necessery libs for psycopg2
RUN apk add postgresql-dev
RUN apk add gcc
RUN apk add musl-dev
RUN apk add g++

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt


# Run app.py when the container launches
CMD ["python", "main.py"]
