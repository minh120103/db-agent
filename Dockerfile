FROM python:3.11-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Copy only necessary files
COPY pyproject.toml .
COPY src/db_agent/ src/db_agent/

# Install dependencies using uv
RUN uv pip install --system --no-cache -e .

# Run the server
CMD ["python", "-m", "db_agent.server", "--transport", "http", "--host", "0.0.0.0", "--port", "9002"]
