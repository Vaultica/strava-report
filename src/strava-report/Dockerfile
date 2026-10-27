FROM python:3.10-slim

WORKDIR /app

# Install git for dbt dependencies
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# Install python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy everything in src/strava-report into /app
# This includes dbt_project.yml, models/, profiles.yml, and data_pipeline/
COPY . .

RUN chmod +x entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
