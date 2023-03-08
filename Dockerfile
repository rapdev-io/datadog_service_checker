FROM python:3.10

COPY . .

RUN pip install -r requirements.txt

CMD ["python3", "service_checker.py"]
