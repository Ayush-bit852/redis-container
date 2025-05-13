FROM python:3.11-slim AS build

WORKDIR /app
COPY pyproject.toml poetry.lock ./
RUN pip install poetry \
 && poetry config virtualenvs.create false \
 && poetry install --no-dev --no-root

FROM python:3.11-slim
WORKDIR /app
COPY --from=build /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY . .

RUN adduser --disabled-password appuser
USER appuser

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
