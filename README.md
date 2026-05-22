# 📊 Big Data Project: Real-Time Sentiment Analysis Pipeline (Woosong University)

This project implements a real-time data streaming architecture to analyze the sentiment (positive, negative, neutral) of simulated tweets. It uses **Apache Kafka** for message ingestion and **Apache Spark Structured Streaming** for processing and analysis.

## 👥 Team
* Daniel Josias Cuellar Betancourt
* VANNEREAU Alexis Sylvain Noël
* BA Louqman

---

## 🛠️ 1. Prerequisites (Infrastructure)
Before running the Python code, make sure the basic infrastructure is installed and running on your local machine (Windows):
1. **Hadoop** (configured with the `HADOOP_HOME` environment variables).
2. **Apache Kafka** (version 4.2.0 in KRaft mode). The `tweets_stream` topic must be created beforehand.
3. **Apache Spark** (version 4.1.1 configured with Hadoop 3 and added to the `PATH`).
4. **Python** (version 3.8 or higher).

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
Once the `(venv)` environment is activated, install the required tools for the project:
```bash
pip install kafka-python pyspark vaderSentiment
```

---

## ⚙️ 6. Execution Order of the Pipeline

The pipeline must be launched in a very specific order. You will need to open **3 separate terminals**.

### Terminal 1: Start the Kafka server (The mailbox)
Go to your Kafka installation folder (e.g., `H:\BigData\kafka_2.13-4.2.0\`) and start the server:
```bash
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
*(Spark will connect to Kafka, analyze the sentiments with VADER, and display the aggregated result every 10 seconds).*
