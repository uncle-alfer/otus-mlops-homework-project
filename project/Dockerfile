FROM python:3.10.12 as base

ENV AWS_SHARED_CREDENTIALS_FILE /app/keys/credentials
ENV AWS_CONFIG_FILE /app/keys/config

WORKDIR /app

COPY .aws/ keys/

COPY requirements/requirements.txt requirements.txt
COPY src/ src/

RUN pip install --no-cache-dir -r requirements.txt

RUN useradd --user-group --shell /bin/false project  
USER project

EXPOSE 8000

CMD ["uvicorn", "src.app:app", "--host", "0.0.0.0"]