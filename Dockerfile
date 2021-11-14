FROM python:3.8

# set the working directory
WORKDIR /app

# copy all the files to the container
COPY . .

# install dependencies
RUN pip install -r requirements.txt

# run the command
#CMD ["python", "main.py"]