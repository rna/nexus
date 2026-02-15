# Nexus | API-First Scraping Framework

Nexus is a production-grade, Dockerized, distributed web scraping framework. It is built on an **API-first** principle, using a browser only for initial discovery and a lightweight HTTP client for high-volume extraction. This architecture is designed for massive scale, cost-efficiency, and resilience against blocking.

## Core Principles

*   **API-First Scraping:** We don't scrape websites; we scrape their private APIs. A browser is used as a one-time tool to discover these APIs, while 99.9% of requests are made via a lightweight `httpx` client.
*   **Intelligent Proxy Management:** Proxies are treated as a managed pool, not a simple URL. Their health is actively tracked, and underperforming proxies are automatically cooled down and rotated.
*   **Adaptive Concurrency:** The framework monitors block rates and automatically throttles request concurrency up or down to maximize throughput without getting banned.
*   **Resilience & Idempotency:** A reliable queueing system ensures no task is lost on a crash. Idempotent database writes with version hashing prevent data duplication and corruption on re-runs.

## How It Works: The Two-Pipeline Architecture

### Pipeline 1: Discovery (Manual Intelligence Gathering)

This is a one-time setup step for each target website.

*   **Goal:** Find the private API endpoint a website uses to load product data.
*   **Tool:** `discovery/playwright_endpoint_mapper.py`
*   **Process:**
    1.  Run the script locally (`python discovery/playwright_endpoint_mapper.py`).
    2.  It opens a browser. You navigate to a product page and interact with it.
    3.  The script logs all potential API calls, headers, and tokens to your terminal.
    4.  You analyze this output to find the API endpoint (e.g., `https://api.nykaa.com/v2/products/12345`) which will be used by the main scraper.

### Pipeline 2: Extraction (The Scaled Worker)

This is the main, continuously running process that scrapes data at high volume.

*   **Tool:** The `scraper-worker` Docker service, which runs `workers/worker.py`.
*   **Process:**
    1.  A list of product API URLs is pushed to a Redis queue.
    2.  The worker pulls a URL, asks the `ProxyManager` for a healthy proxy, and acquires a concurrency slot from the `RateController`.
    3.  A direct request is made to the API using `httpx`.
    4.  The `BlockDetector` checks the response. Failures are reported, penalizing the proxy's health score.
    5.  On success, the raw JSON is sent to the `Normalizer`, which transforms it into a clean, standard schema.
    6.  The data is saved to PostgreSQL idempotently.

## How to Run Locally

### Prerequisites

1.  **Docker & Docker Compose:** Must be installed.
2.  **Python 3.11+:** For running the discovery script. Install dependencies with `pip install -r requirements.txt`.
3.  **Residential Proxy Service:** Required. You need a **comma-separated list** of proxy URLs.

### Step 1: Configure the Environment

*   Create a `.env` file in the project root.
*   Fill it with your database credentials and your comma-separated proxy list.

    ```
    # .env file
    POSTGRES_USER=user
    POSTGRES_PASSWORD=password
    POSTGRES_DB=nexus
    REDIS_URL=redis://redis:6379

    # Comma-separated list of proxy URLs from your provider
    PROXY_URLS=http://user:pass@host1:port,http://user:pass@host2:port
    ```

### Step 2: Run the Extraction Worker

*   From the project root, start the main application stack:
    ```bash
    docker-compose -f infra/docker-compose.yml up --build
    ```
*   The `scraper-worker` will start and wait for tasks. For initial testing, it will seed the queue with a hypothetical Sephora API endpoint if the queue is empty.

### Step 3: Populate the Queue

*   For real scraping, you need to populate the `scraping_queue` in Redis with the API URLs you found during the discovery phase. You would typically write a separate, simple "crawler" script for this that finds product IDs on category pages and formats them into API URLs.
