FROM python:3.10-slim

WORKDIR /app

# Install git (required by dbt for packages/dependencies)
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# Install python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code and dbt project
COPY get_strava_activities.py entrypoint.sh ./
COPY dbt/ ./dbt/

RUN chmod +x entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
