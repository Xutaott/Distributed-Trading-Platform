FROM python:3
ADD . /code
WORKDIR /code
RUN pip install flask
RUN pip install numpy pandas sqlalchemy
CMD python app.py