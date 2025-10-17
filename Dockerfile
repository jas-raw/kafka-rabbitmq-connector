FROM python:3.9-alpine
ENV PYTHONUNBUFFERED="1"

WORKDIR /app

COPY ./src/ .
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "-u", "main.py"]