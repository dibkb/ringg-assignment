# Ring Assignment

This project is a Python-based application with Docker support and performance testing capabilities using k6.

## Prerequisites

- Python 3.x
- Poetry (Python package manager)
- Docker and Docker Compose
- k6 (for performance testing)

## Project Structure

```
.
├── app/                    # Main application code
├── tests/                  # Test files
├── docker-compose.dev.yml  # Development Docker configuration
├── docker-compose.k6.yml   # k6 performance testing configuration
├── Dockerfile.mac         # Docker configuration for Mac
├── Dockerfile.k6          # k6 Docker configuration
├── k6.yml                 # k6 test configuration
├── grafana-dashboard.json # Grafana dashboard configuration
└── pyproject.toml         # Poetry project configuration
```

## Getting Started

1. Install dependencies using Poetry:

   ```bash
   poetry install
   ```

2. Run the application in development mode:
   ```bash
   docker-compose -f docker-compose.dev.yml up
   ```

## Performance Testing

To run performance tests using k6:

```bash
docker-compose -f docker-compose.dev.yml up --build
```

To test specific endpoints using k6:

```bash
docker-compose -f docker-compose.k6.yml run k6 run /tests/read/latency.js
```

The test file `latency.js` is located in the `/tests/read/` directory and contains the performance test scenarios for the endpoints.
