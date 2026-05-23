# 📊 Big Data Project: Real-Time Sentiment Analysis Pipeline (Woosong University)

This project implements a real-time data streaming architecture to analyze the sentiment (positive, negative, neutral) of simulated tweets. It uses **Apache Kafka** for message ingestion and **Apache Spark Structured Streaming** for processing and analysis.

## 👥 Team
* Daniel Josias Cuellar Betancourt
* VANNEREAU Alexis Sylvain Noël
* BA Louqman

---

## 🛠️ 1. Prerequisites (Infrastructure)
Before running the Python code, make sure the basic infrastructure is installed and running on your local machine (Windows):
1. **Hadoop** (configured with the `HADOOP_HOME` environment variables, specifically `winutils.exe` for Windows).
2. **Apache Kafka** (Ensure the `tweets_stream` topic is created beforehand).
3. **Apache Spark** (version **3.5.8** configured with Hadoop and added to the `PATH`).
4. **Python** (version **3.11** is highly recommended. Do NOT use 3.14 as it is currently causing compatibility issues).

---

## 🚀 2. Clone the project (Git)

Open a terminal, clone this repository to your local machine, and navigate into the folder:
```bash
git clone https://github.com/YOUR-USERNAME/projet-big-data-woosong.git
cd projet-big-data-woosong
```

---

## 🛡️ 3. The .gitignore file (VERY IMPORTANT)
**Golden rule for the team:** Never push the virtual environment or large data files to GitHub!
Make sure a file named `.gitignore` (with no name before the dot) exists at the root of the project and contains this:

```text
# Python virtual environment
venv/

# Data files and logs
*.csv
logs/
__pycache__/
```

---

## 🐍 4. Create and activate the virtual environment
To avoid conflicts with other projects on your computer, everyone must create their own local virtual environment (it will not be pushed to GitHub).

**Step A: Create the bubble (to be done only once)**
```bash
python -m venv venv
```
*(This will create a folder named `venv` in the project).*

**Step B: Activate the bubble (to be done at each new work session!)**
* On Windows (PowerShell / VSCode):
  ```bash
  .\venv\Scripts\activate
  ```
* On Mac / Linux:
  ```bash
  source venv/bin/activate
  ```
*(Verification: You should see the `(venv)` prefix appear at the beginning of your command line in the terminal).*

---

## 📦 5. Install Python libraries
Once the `(venv)` environment is activated, install the exact package versions required for this project. 
*(Note: `pandas` and `pyarrow` are necessary for PySpark's `pandas_udf` to execute properly).*

```bash
pip install kafka-python==2.3.1 pyspark==3.5.8 vaderSentiment==3.3.2 pandas==3.0.3 pyarrow==24.0.0
```

*Alternatively, if a `requirements.txt` file is present, you can run:*
```bash
pip install -r requirements.txt
```

---

## ⚙️ 6. Execution Order of the Pipeline

The pipeline must be launched in a very specific order. You will need to open **3 separate terminals**.

### Terminal 1: Start the Kafka server (The mailbox)
Go to your Kafka installation folder and start the server. 
*(Depending on whether you use Zookeeper or KRaft, the command may vary. Below is an example for Zookeeper on Windows)*:
```bash
# Start Zookeeper (if not using KRaft)
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

# Start Kafka Server
.\bin\windows\kafka-server-start.bat .\config\server.properties
```
*(Wait for the message indicating the server has started. Leave this terminal running in the background).*

### Terminal 2: Start the Producer (Data Ingestion)
In the project folder in VSCode (with the `venv` environment activated), run the script that generates the simulated tweets:
```bash
python producer.py
```
*(You should see the tweets appearing one by one).*

### Terminal 3: Start the Consumer (Spark Processing)
In a third VSCode terminal (still with the `venv` environment activated), launch the analysis engine:
```bash
python consumer.py
```
*(Spark will connect to Kafka, analyze the sentiments with VADER using pandas UDFs, and display the aggregated result every 10 seconds).*
