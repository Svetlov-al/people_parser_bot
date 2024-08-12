FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    locales \
    && echo "ru_RU.UTF-8 UTF-8" > /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=ru_RU.UTF-8

COPY requirements.txt .
COPY pars.py .

COPY new_users.json .
COPY full_base.json .
COPY converted_new_users.txt .
COPY converted_full_base.txt .
COPY chats.json .
COPY channels.json .
COPY auto_parser_settings.json .
COPY acs_users.json .
COPY zakaz.session .
COPY images/ images/
COPY newFILES/ newFILES/

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "pars.py"]
