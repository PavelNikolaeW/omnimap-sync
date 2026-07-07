# OmniMap Sync

A **FastAPI + WebSocket** service for real-time synchronization between OmniMap
clients. One microservice in the OmniMap platform (see
[omnimap-back](https://github.com/PavelNikolaeW/omnimap-back) and
[omnimap-front](https://github.com/PavelNikolaeW/omnimap-front)).

## Stack

Python 3.11+ · FastAPI · WebSockets · Redis · RabbitMQ · JWT

## What it does

Fans out block/map changes to connected clients in real time and bridges to the rest
of the platform over RabbitMQ, with Redis for shared state and JWT for auth.

## Run

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 7999
pytest tests/ -v
```
