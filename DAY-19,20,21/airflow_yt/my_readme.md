## architecture

1. Scheduler

    What it does: Triggers DAG runs based on schedules or external triggers.

    It queues up Task Instances that are ready to run.

2. Web Server

    What it does: Provides the UI to monitor DAGs, task runs, logs, and more.

    Runs as a Flask app.

    Useful for triggering DAGs manually and inspecting errors.

3. Metadata Database (Metastore)

    What it does: Stores the state of everything:

        DAGs

        Task instances

        DAG runs

        Logs, variables, connections, etc.

    Typically uses PostgreSQL or MySQL.

4. Workers

    What they do: Actually execute the tasks.

    Run in different modes:

        CeleryExecutor: Distributed workers (for production)

        LocalExecutor: Parallel tasks on one machine

        SequentialExecutor: One task at a time (default, dev only)

5. Executor

    What it does: Sits between Scheduler and Workers.

    Decides how and where to run tasks (e.g., Local, Celery, Kubernetes).

6. DAG Files

    Python files defining workflow logic.

    Placed in the DAGs folder and parsed by the Scheduler/Web Server.

⚙️ Optional Components

    Triggerer: Handles async tasks (e.g., waiting for a file to land).

    Logs: Can be stored in local files or remote (like S3/GCS).

🧭 How It Works Together

    DAG is loaded by the Scheduler and Web Server from DAGs folder.

    Scheduler checks when to trigger DAG runs (based on cron).

    For each DAG run, it creates Task Instances.

    Executor assigns tasks to Workers.

    Worker executes the task and updates status in DB.

    Web UI lets you monitor everything in real-time.

🔄 Typical Setup (Docker Compose or Production)

    Multiple Scheduler, Web Server, and Worker containers.

    Shared Postgres DB and volume for logs.

    If using Celery: add Redis or RabbitMQ as message broker.