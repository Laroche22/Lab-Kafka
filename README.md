# Kafka Lab

## Objective

Read a Project Gutenberg book line by line, send each line to Kafka, then consume, clean and parse the text and save the result to a file.

## Work split

- Member 1: environment setup, Kafka demonstration, new topic and documentation.
- Member 2: select and download the book and implement the book producer.
- Member 3: implement the book consumer, text processing and output file.
- Together: integrate the scripts and test the complete book pipeline.

## Current progress

The Kafka demonstration using the `timer` topic has been tested successfully.

The book pipeline has also been implemented and tested by all group members. The `book-lines` topic is created successfully, the producer reads `book.txt` line by line and sends non-empty lines to Kafka, and the consumer receives the messages, cleans and processes the text, and saves the result to `cleaned_book.txt`.

The complete pipeline has been tested successfully in each member's local environment. During testing, the consumer processed 6,730 messages and the generated output file was checked.

## Files

| File | Purpose |
| --- | --- |
| `admin.py` | Teacher's example: creates the `timer` topic. |
| `producer.py` | Teacher's example: sends the current time every second for five minutes. |
| `consumer.py` | Teacher's example: reads messages from `timer`. |
| `admin_book.py` | Creates `book-lines`, waits for Kafka's confirmation and handles an existing topic. |
| `requirements.txt` | Python dependency: `confluent-kafka==2.16.0`. |
| `.gitignore` | Excludes `.venv/`, `__pycache__/` and `*.pyc` from Git. |

## Shared configuration

- Book topic: `book-lines`
- Bootstrap server in the Python scripts: `localhost:9092`
- Topic partitions: `1`
- Replication factor: `1`
- Text encoding for book messages: UTF-8

GitHub shares the project files. Each member runs a local Kafka instance and creates the topic on their own machine with `admin_book.py`. `localhost` refers to the machine or network namespace where the script runs; it does not connect to another member's computer.

## Environment

This setup uses Windows PowerShell and Docker Desktop with Linux containers. Kafka runs in `apache/kafka-native:4.1.1`; Python runs in `python:3.11-slim`.

Python runs inside Docker because Windows Smart App Control blocked the native Kafka library on member 1's computer. The Windows security settings were kept enabled.

Open PowerShell in the project folder containing the Python scripts and `requirements.txt`. Docker Desktop must be running and port `9092` must be available.

### First setup on a new machine

Run these commands once to create the two containers. If they already exist, use the stop/restart instructions below.

Start Kafka:

```powershell
docker pull apache/kafka-native:4.1.1
docker run -d --name kafka-lab -p 9092:9092 apache/kafka-native:4.1.1
```

Start the Python container from the project folder:

```powershell
docker run -d --name kafka-python --network container:kafka-lab --mount "type=bind,source=$($PWD.Path),target=/app" -w /app python:3.11-slim sleep infinity
```

The project folder is mounted at `/app`, so saved edits are available inside the Python container. The Python container shares Kafka's network namespace, allowing the scripts to use `localhost:9092`.

Create a Linux Python environment inside the container and install the dependency:

```powershell
docker exec kafka-python python -m venv /opt/venv
docker exec kafka-python /opt/venv/bin/python -m pip install -r requirements.txt
docker ps
```

Both `kafka-lab` and `kafka-python` should show an `Up` status. The environment at `/opt/venv` is separate from the Windows `.venv` folder.

### Create the book topic

```powershell
docker exec kafka-python /opt/venv/bin/python -u admin_book.py
```

Expected output:

```text
Topic 'book-lines' created successfully.
```

Running the script again keeps the existing topic and prints:

```text
Topic 'book-lines' already exists.
```

### Run the teacher's demonstration

Create the example topic:

```powershell
docker exec kafka-python /opt/venv/bin/python -u admin.py
```

The original `admin.py` may finish without printing a topic because it does not wait for the topic creation request. Check the available topics directly if needed:

```powershell
docker exec kafka-python /opt/venv/bin/python -c "from confluent_kafka.admin import AdminClient; print(sorted(AdminClient({'bootstrap.servers': 'localhost:9092'}).list_topics(timeout=10).topics))"
```

Start the producer in one PowerShell terminal:

```powershell
docker exec kafka-python /opt/venv/bin/python -u producer.py
```

While it runs, open another PowerShell terminal and start the consumer:

```powershell
docker exec kafka-python /opt/venv/bin/python -u consumer.py
```

Expected consumer output:

```text
Rcvd message: The time is now 08:04:06
```

The producer stops after five minutes. The consumer stops after ten consecutive empty polls and prints `Closing: No new messages received.` This is normal.

Kafka retains messages after the producer stops, so the consumer can also read them afterwards. The original consumer uses group `foo` and resumes from saved offsets on later runs.

### Stop and restart

After the scripts have finished, stop Python first, then Kafka:

```powershell
docker stop kafka-python
docker stop kafka-lab
```

To resume, start Kafka first, then Python:

```powershell
docker start kafka-lab
docker start kafka-python
```

Keep the containers to reuse their installed environment and local Kafka data.

## Complete the book pipeline

Members 2 and 3 must use topic `book-lines` and the shared configuration above. Add their actual script filenames, book reference, execution commands and output format to this README when their work is available.

The final group test must confirm that the book lines are sent, consumed, cleaned and parsed, and that the expected output file is produced.
