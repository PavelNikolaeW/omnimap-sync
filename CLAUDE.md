# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Git Workflow

**IMPORTANT: Разработка ведётся в отдельных ветках!**

1. **Создай ветку** для задачи:
   ```bash
   git checkout -b feature/название-задачи
   # или
   git checkout -b fix/описание-бага
   ```

2. **Работай в своей ветке** — никогда не коммить напрямую в `main`

3. **После завершения задачи** создай Pull Request:
   ```bash
   git push -u origin feature/название-задачи
   gh pr create --title "Описание" --body "Детали изменений"
   ```

4. **Дождись ревью** от Claude Code Action перед мержем

## Project Overview

OmniMap Sync is a FastAPI-based WebSocket service for real-time synchronization between clients. Part of the OmniMap platform microservices architecture.

**Stack:** Python 3.11+, FastAPI, WebSockets, Redis, RabbitMQ, JWT

## Common Commands

```bash
# Run development server
uvicorn app.main:app --reload --host 0.0.0.0 --port 7999

# Run tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=app --cov-report=term-missing
```

## Environment Variables

- `HOST` - Server host (default: 0.0.0.0)
- `PORT` - Server port (default: 7999)
- `REDIS_URL` - Redis connection URL
- `RABBITMQ_URL` - RabbitMQ connection URL
- `AUTH_SERVICE_URL` - Backend auth verification endpoint
- `JWT_SECRET_KEY` - JWT secret for token verification

## Architecture

### Project Structure

```
app/
├── main.py          # FastAPI application entry point
├── websocket.py     # WebSocket connection handling
├── auth.py          # JWT token verification
├── redis_client.py  # Redis pub/sub operations
└── rabbitmq.py      # RabbitMQ consumer for block updates
```

### Key Components

- **WebSocket Manager** - Handles client connections, subscriptions to blocks
- **Redis Pub/Sub** - Broadcasts updates to all connected clients
- **RabbitMQ Consumer** - Receives block update events from backend

## Testing

```bash
# Run all tests
pytest tests/ -v --tb=short

# Run specific test
pytest tests/test_websocket.py -v
```

## Workflow

1. Create a feature branch
2. Implement the feature
3. Write tests
4. Run ALL tests: `pytest tests/`
5. Fix any failures
6. Commit only when all tests pass
7. Push and create a Pull Request
