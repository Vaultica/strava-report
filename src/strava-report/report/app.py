import os
from datetime import datetime, timedelta
from flask import Flask, render_template_string, request
from google.cloud import bigquery

app = Flask(__name__)
client = bigquery.Client(project="secret-compass-181513")

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Strava Performance Dashboard</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body class="bg-gray-900 text-gray-100 font-sans p-6">
    <div class="max-w-7xl mx-auto">
        <!-- Header -->
        <header class="mb-6 flex justify-between items-center border-b border-gray-800 pb-4">
            <h1 class="text-2xl font-bold tracking-tight text-orange-500">Strava Report Dashboard</h1>
            <a href="/" class="text-sm bg-gray-800 hover:bg-gray-700 text-gray-300 px-4 py-2 rounded-lg border border-gray-700 transition">Reset Filters</a>
        </header>

        <!-- Metrics Tile -->
        <div class="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
            <div class="bg-gray-800 p-5 rounded-xl border border-gray-700 shadow">
                <p class="text-xs font-medium uppercase tracking-wider text-gray-400">Total Kilometers</p>
                <p class="text-3xl font-extrabold mt-1 text-white">{{ "%.1f"|format(total_distance) }} km</p>
            </div>
            <div class="bg-gray-800 p-5 rounded-xl border border-gray-700 shadow">
                <p class="text-xs font-medium uppercase tracking-wider text-gray-400">Activities Count</p>
                <p class="text-3xl font-extrabold mt-1 text-white">{{ total_activities }}</p>
            </div>
        </div>

        <!-- Filters Form -->
        <form method="GET" action="/" class="bg-gray-800 p-4 rounded-xl border border-gray-700 mb-6 grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-4 items-end">
            <div>
                <label class="block text-xs font-medium text-gray-400 mb-1">Bike</label>
                <select name="bike" class="w-full bg-gray-900 border border-gray-700 rounded-lg p-2 text-sm text-gray-200">
                    <option value="">All Bikes</option>
                    {% for b in filter_options.bikes %}
                    <option value="{{ b }}" {% if selected_bike == b %}selected{% endif %}>{{ b }}</option>
                    {% endfor %}
                </select>
            </div>
            <div>
                <label class="block text-xs font-medium text-gray-400 mb-1">Activity Type</label>
                <select name="type" class="w-full bg-gray-900 border border-gray-700 rounded-lg p-2 text-sm text-gray-200">
                    <option value="">All Types</option>
                    {% for t in filter_options.types %}
                    <option value="{{ t }}" {% if selected_type == t %}selected{% endif %}>{{ t }}</option>
                    {% endfor %}
                </select>
            </div>
            <div>
                <label class="block text-xs font-medium text-gray-400 mb-1">Date Range</label>
                <select name="date_range" class="w-full bg-gray-900 border border-gray-700 rounded-lg p-2 text-sm text-gray-200">
                    <option value="this_month" {% if selected_date_range == 'this_month' %}selected{% endif %}>This Month</option>
                    <option value="last_30" {% if selected_date_range == 'last_30' %}selected{% endif %}>Last 30 Days</option>
                    <option value="this_year" {% if selected_date_range == 'this_year' %}selected{% endif %}>This Year</option>
                    <option value="all" {% if selected_date_range == 'all' %}selected{% endif %}>All Time</option>
                </select>
            </div>
            <div>
                <label class="block text-xs font-medium text-gray-400 mb-1">Min Distance (km): <span id="distVal">{{ selected_min_dist }}</span></label>
                <input type="range" name="min_dist" min="0" max="150" step="1" value="{{ selected_min_dist }}"
                       oninput="document.getElementById('distVal').innerText=this.value"
                       class="w-full accent-orange-500 bg-gray-900 cursor-pointer">
            </div>
            <div class="flex gap-2">
                <button type="submit" class="w-full bg-orange-600 hover:bg-orange-500 text-white font-medium py-2 px-4 rounded-lg text-sm transition">Apply Filter</button>
            </div>
        </form>

        <!-- Distance Chart Section -->
        <div class="bg-gray-800 p-6 rounded-xl border border-gray-700 shadow mb-6">
            <h2 class="text-xl font-semibold mb-4 text-gray-200">Distance Chart ({{ chart_mode | capitalize }})</h2>
            <div class="relative h-72">
                <canvas id="stravaChart"></canvas>
            </div>
        </div>

        <!-- Data Table Container -->
        <div class="bg-gray-800 rounded-xl border border-gray-700 overflow-hidden shadow mb-6">
            <!-- Table Header Toolbar (Search Box) -->
            <div class="p-4 border-b border-gray-700 flex justify-between items-center bg-gray-900/40">
                <h3 class="font-semibold text-gray-200">Activities</h3>
                <div class="w-72">
                    <input type="text" id="tableSearch" placeholder="Search ride name..."
                           class="w-full bg-gray-900 border border-gray-700 rounded-lg px-3 py-1.5 text-sm text-gray-200 focus:outline-none focus:border-orange-500"
                           onkeyup="filterTable()">
                </div>
            </div>

            <div class="overflow-x-auto">
                <table class="w-full text-left border-collapse text-sm" id="activitiesTable">
                    <thead>
                        <tr class="bg-gray-900/60 text-gray-400 border-b border-gray-700 text-xs uppercase tracking-wider">
                            <th class="p-3">Started At</th>
                            <th class="p-3">Ride Name</th>
                            <th class="p-3">Moving Time</th>
                            <th class="p-3">Elapsed Time</th>
                            <th class="p-3">Elevation (m)</th>
                            <th class="p-3">Bike</th>
                            <th class="p-3">Distance (km)</th>
                            <th class="p-3">Avg Speed (km/h)</th>
                        </tr>
                    </thead>
                    <tbody class="divide-y divide-gray-700/50">
                        {% for row in rows %}
                        <tr class="hover:bg-gray-700/30 transition">
                            <td class="p-3 text-gray-300">{{ row.start_date }}</td>
                            <td class="p-3 font-medium ride-name">
                                <a href="https://www.strava.com/activities/{{ row.id }}" target="_blank" class="text-orange-400 hover:text-orange-300 hover:underline flex items-center gap-1">
                                    {{ row.name }}
                                    <svg class="w-3.5 h-3.5 opacity-70" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14"></path></svg>
                                </a>
                            </td>
                            <td class="p-3 text-gray-300">{{ row.moving_time_formatted }}</td>
                            <td class="p-3 text-gray-300">{{ row.elapsed_time_formatted }}</td>
                            <td class="p-3 text-gray-300">{{ row.total_elevation_gain | int }}</td>
                            <td class="p-3 text-gray-300">{{ row.bike or 'N/A' }}</td>
                            <td class="p-3 text-gray-200">
                                <div class="flex items-center gap-2">
                                    <span class="w-12 text-right font-mono">{{ "%.1f"|format(row.distance_km) }}</span>
                                    <div class="w-24 bg-gray-700 rounded-full h-2 overflow-hidden">
                                        <div class="bg-orange-500 h-2 rounded-full" style="width: {{ [row.distance_km * 2, 100]|min }}%"></div>
                                    </div>
                                </div>
                            </td>
                            <td class="p-3 text-gray-300 font-mono">{{ "%.1f"|format(row.average_speed_kmh or 0) }}</td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>

            <!-- Pagination Footer -->
            <div class="p-4 border-t border-gray-700 bg-gray-900/40 flex justify-between items-center text-sm text-gray-400">
                <span>Showing page {{ page }} of {{ total_pages }} ({{ total_activities }} total activities)</span>
                <div class="flex gap-2">
                    {% if page > 1 %}
                    <a href="?bike={{ selected_bike }}&type={{ selected_type }}&date_range={{ selected_date_range }}&min_dist={{ selected_min_dist }}&page={{ page - 1 }}"
                       class="bg-gray-800 hover:bg-gray-700 text-gray-200 px-3 py-1.5 rounded-lg border border-gray-700 transition">Previous</a>
                    {% endif %}
                    {% if page < total_pages %}
                    <a href="?bike={{ selected_bike }}&type={{ selected_type }}&date_range={{ selected_date_range }}&min_dist={{ selected_min_dist }}&page={{ page + 1 }}"
                       class="bg-gray-800 hover:bg-gray-700 text-gray-200 px-3 py-1.5 rounded-lg border border-gray-700 transition">Next</a>
                    {% endif %}
                </div>
            </div>
        </div>
    </div>

    <script>
        const chartData = {{ chart_data | tojson }};
        const ctx = document.getElementById('stravaChart').getContext('2d');
        new Chart(ctx, {
            type: 'bar',
            data: {
                labels: chartData.labels,
                datasets: chartData.datasets
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: chartData.datasets.length > 1, labels: { color: '#9ca3af' } }
                },
                scales: {
                    x: { stacked: true, grid: { color: '#374151' }, ticks: { color: '#9ca3af' } },
                    y: { stacked: true, grid: { color: '#374151' }, ticks: { color: '#9ca3af' } }
                }
            }
        });

        function filterTable() {
            let input = document.getElementById('tableSearch');
            let filter = input.value.toLowerCase();
            let table = document.getElementById('activitiesTable');
            let tr = table.getElementsByTagName('tr');

            for (let i = 1; i < tr.length; i++) {
                let td = tr[i].getElementsByClassName('ride-name')[0];
                if (td) {
                    let txtValue = td.textContent || td.innerText;
                    if (txtValue.toLowerCase().indexOf(filter) > -1) {
                        tr[i].style.display = "";
                    } else {
                        tr[i].style.display = "none";
                    }
                }
            }
        }
    </script>
</body>
</html>
"""

def format_seconds(seconds):
    if not seconds:
        return "00:00:00"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

@app.route("/")
def index():
    selected_bike = request.args.get("bike", "")
    selected_type = request.args.get("type", "")
    selected_date_range = request.args.get("date_range", "this_month")
    selected_min_dist = request.args.get("min_dist", "0")
    page = int(request.args.get("page", 1))
    page_size = 50
    offset = (page - 1) * page_size

    bikes_query = "SELECT DISTINCT bike FROM `secret-compass-181513.strava.activities_features` WHERE bike IS NOT NULL ORDER BY bike"
    types_query = "SELECT DISTINCT type FROM `secret-compass-181513.strava.activities_features` WHERE type IS NOT NULL ORDER BY type"

    bikes = [r.bike for r in client.query(bikes_query)]
    types = [r.type for r in client.query(types_query)]

    if selected_date_range in ["this_month", "last_30"]:
        chart_mode = "weekly"
        date_trunc_unit = "WEEK"
    else:
        chart_mode = "monthly"
        date_trunc_unit = "MONTH"

    where_clauses = ["1=1"]
    query_params = []

    if selected_bike:
        where_clauses.append("bike = @bike")
        query_params.append(bigquery.ScalarQueryParameter("bike", "STRING", selected_bike))
    if selected_type:
        where_clauses.append("type = @type")
        query_params.append(bigquery.ScalarQueryParameter("type", "STRING", selected_type))
    if selected_min_dist and float(selected_min_dist) > 0:
        where_clauses.append("distance_km >= @min_dist")
        query_params.append(bigquery.ScalarQueryParameter("min_dist", "FLOAT", float(selected_min_dist)))

    now = datetime.utcnow()
    if selected_date_range == "this_month":
        start_date_val = datetime(now.year, now.month, 1).strftime("%Y-%m-%d")
        where_clauses.append("start_date >= @start_date")
        query_params.append(bigquery.ScalarQueryParameter("start_date", "STRING", start_date_val))
    elif selected_date_range == "last_30":
        start_date_val = (now - timedelta(days=30)).strftime("%Y-%m-%d")
        where_clauses.append("start_date >= @start_date")
        query_params.append(bigquery.ScalarQueryParameter("start_date", "STRING", start_date_val))
    elif selected_date_range == "this_year":
        start_date_val = datetime(now.year, 1, 1).strftime("%Y-%m-%d")
        where_clauses.append("start_date >= @start_date")
        query_params.append(bigquery.ScalarQueryParameter("start_date", "STRING", start_date_val))

    where_sql = " AND ".join(where_clauses)

    metrics_query = f"""
        SELECT COUNT(*) AS total_count, SUM(distance_km) AS total_dist
        FROM `secret-compass-181513.strava.activities_features`
        WHERE {where_sql}
    """
    metrics_job = client.query(metrics_query, job_config=bigquery.QueryJobConfig(query_parameters=query_params))
    metrics_result = list(metrics_job)[0]
    total_activities = metrics_result.total_count or 0
    total_distance = metrics_result.total_dist or 0
    total_pages = max(1, (total_activities + page_size - 1) // page_size)

    # Added `id` to the select list so we can construct the Strava URL link
    data_query = f"""
        SELECT
            id, start_date, name, moving_time, elapsed_time, total_elevation_gain, bike, distance_km,
            (average_speed * 3.6) AS average_speed_kmh
        FROM `secret-compass-181513.strava.activities_features`
        WHERE {where_sql}
        ORDER BY start_date DESC
        LIMIT {page_size} OFFSET {offset}
    """

    query_job = client.query(data_query, job_config=bigquery.QueryJobConfig(query_parameters=query_params))
    rows = []
    for row in query_job:
        row_dict = dict(row)
        row_dict["moving_time_formatted"] = format_seconds(row.moving_time)
        row_dict["elapsed_time_formatted"] = format_seconds(row.elapsed_time)
        rows.append(row_dict)

    chart_where = ["1=1"]
    chart_params = []
    if selected_type:
        chart_where.append("type = @type")
        chart_params.append(bigquery.ScalarQueryParameter("type", "STRING", selected_type))
    if selected_date_range in ["this_month", "last_30", "this_year"]:
        chart_where.append("start_date >= @chart_start_date")
        chart_params.append(bigquery.ScalarQueryParameter("chart_start_date", "STRING", start_date_val))

    chart_where_sql = " AND ".join(chart_where)
    chart_query = f"""
        SELECT
            FORMAT_DATE('%Y-%m-%d', DATE(TIMESTAMP_TRUNC(CAST(start_date AS TIMESTAMP), {date_trunc_unit}))) AS period,
            COALESCE(type, 'Unknown') AS activity_type,
            SUM(distance_km) AS total_dist
        FROM `secret-compass-181513.strava.activities_features`
        WHERE {chart_where_sql}
        GROUP BY period, activity_type
        ORDER BY period ASC
    """

    chart_rows = list(client.query(chart_query, job_config=bigquery.QueryJobConfig(query_parameters=chart_params)))
    periods_set = sorted(list({r.period for r in chart_rows}))
    activity_types = sorted(list({r.activity_type for r in chart_rows}))

    palette = ['#fc4c02', '#3b82f6', '#10b981', '#f59e0b', '#8b5cf6', '#ec4899']
    datasets = []
    for i, act_type in enumerate(activity_types):
        data_points = []
        for p in periods_set:
            matched_dist = next((r.total_dist for r in chart_rows if r.period == p and r.activity_type == act_type), 0)
            data_points.append(round(matched_dist, 1))
        datasets.append({
            "label": act_type,
            "data": data_points,
            "backgroundColor": palette[i % len(palette)],
            "borderRadius": 2
        })

    chart_data = {
        "labels": periods_set,
        "datasets": datasets
    }

    return render_template_string(
        HTML_TEMPLATE,
        rows=rows,
        total_distance=total_distance,
        total_activities=total_activities,
        filter_options={"bikes": bikes, "types": types},
        selected_bike=selected_bike,
        selected_type=selected_type,
        selected_date_range=selected_date_range,
        selected_min_dist=selected_min_dist,
        page=page,
        total_pages=total_pages,
        chart_mode=chart_mode,
        chart_data=chart_data
    )

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
