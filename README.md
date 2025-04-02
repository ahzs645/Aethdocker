🛰️ Aethalometer Data Processor (Modular Flask App)
A Dockerized, modular Flask-based application for processing and visualizing Aethalometer Black Carbon (BC) sensor data with optional weather correlation. Supports large datasets (500MB–1GB), real-time progress tracking, and the ONA noise reduction algorithm from EPA’s Hagler et al. paper.

🚀 Features
📥 Upload Aethalometer CSV files

🌤️ Upload optional Weather CSV files for correlation

🧠 Built-in ONA Algorithm (Optimized Noise-reduction Averaging)

📈 Interactive time-series & comparison visualizations (Plotly)

🧪 Raw vs. processed data comparisons

📦 Handles large datasets (chunked memory-safe processing)

🔧 NaN/empty value handling (JSON-safe output)

🌐 Runs locally in a Docker container

🎛️ Configurable web server port via environment variable

🗂 Directory Structure
bash
Copy
Edit
aethalometer_modular/
├── app/
│   ├── templates/          # index.html interface
│   ├── routes/             # Flask route logic
│   ├── processing/         # Core processing logic (ONA, weather, plots)
│   ├── utils/              # Status tracker, JSON encoder
│   └── __init__.py         # Flask app factory
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── run.py                  # Entry point for Flask app
🧪 Data Format
📄 Aethalometer CSV (Example Header)
css
Copy
Edit
Serial number,Time (UTC),...,[UV BC1],[UV BC2],[UV BCc],...,[IR BCc],Readable status
Important fields: UV BCc, Blue BCc, Green BCc, Red BCc, IR BCc, Time (UTC)

Missing/NaN/NA values are automatically handled

🌤️ Weather CSV (Optional)
sql
Copy
Edit
timestamp,temperature_c,dew_point_c,relative_humidity_percent,...
Timestamps must be parseable and ideally synchronized with the aethalometer data.

⚙️ How to Use
1. Place Data Files
Put your CSV files in the following directory:

bash
Copy
Edit
aethalometer_modular/app/data/
aethalometer_data.csv

weather_data.csv (optional)

2. Run the App
Default Port (5000)
bash
Copy
Edit
docker-compose up
Custom Port (e.g. 8080)
bash
Copy
Edit
PORT=8080 docker-compose up
Then open:
👉 http://localhost:5000
or
👉 http://localhost:8080 (if using a custom port)

🧠 ONA Algorithm
Based on Hagler et al. (2011), the Optimized Noise-reduction Averaging algorithm:

Groups data using ATN change thresholds

Applies adaptive averaging

Retains time resolution while reducing random noise

🛠 Developer Notes
All NaN, NA, and empty fields are converted to null in JSON via a custom encoder (json_encoder.py)

Processing progress is tracked and updated via /api/status (see status_tracker.py)

Visualizations are created using Plotly and returned dynamically via API routes

🐳 Docker Notes
Build (optional)
bash
Copy
Edit
docker-compose build
Port Explanation
The docker-compose.yml dynamically maps the internal and external port:

yaml
Copy
Edit
ports:
  - "${PORT:-5000}:${PORT:-5000}"
Use any free port by setting PORT=XXXX before docker-compose up.

🧑‍🔬 Reference
Hagler, G. S. W., Yelverton, T. L. B., Vedantham, R., Hansen, A. D. A., & Turner, J. R. (2011).
Post-processing Method to Reduce Noise while Preserving High Time Resolution in Aethalometer Real-time Black Carbon Data
J. Air & Waste Manage. Assoc. 61(4), 401–410.