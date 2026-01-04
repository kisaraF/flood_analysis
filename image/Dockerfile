FROM python:3.12-slim

COPY src/ /app/

WORKDIR /app/

RUN pip install -r /app/requirements.txt

CMD ["fastapi", "run", "/app/backend.py", "--reload", "--port" , "9999"]
