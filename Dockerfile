FROM python:3.10.12 as base

WORKDIR /app

COPY setup.py setup.py
COPY scripts/start_hw8.sh start.sh
COPY requirements/requirements-hw8.txt requirements.txt
COPY src/ src/
COPY .git/ .git/

RUN pip install --no-cache-dir -r requirements.txt

RUN pip install --no-cache-dir -e .

RUN chmod +x start.sh

RUN useradd --user-group --shell /bin/false predictor  
USER predictor

EXPOSE 8000

CMD ["start.sh"]