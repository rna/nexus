# Nexus | Advanced Web Scraping Framework

Nexus is a production-ready, Dockerized, distributed web scraping framework designed to bypass advanced anti-bot protections on e-commerce sites.

## Core Technologies

*   **Language:** Python 3.11+
*   **Browser:** Playwright with `playwright-stealth` and advanced fingerprint evasion.
*   **Database:** PostgreSQL (for product storage).
*   **Queue/Cache:** Redis (for task management and URL deduplication).
*   **Infrastructure:** Docker Compose.

## Features

*   **Advanced Stealth & Evasion:**
    *   **IP Rotation:** Integrates with professional residential proxy services.
    *   **Browser Fingerprint Evasion:** Randomizes WebGL, platform, user agents, and other navigator properties for each session.
    *   **Human Behavior Simulation:** Automatically handles cookie banners, scrolls pages realistically, and uses randomized delays to mimic user interaction.
*   **Modular & Extensible:**
    *   **Site-Specific Configurations:** Easily add new target websites by creating a simple JSON configuration file in the `sites/` directory. No code changes required.
    *   **Decoupled Architecture:** Core logic is separated into distinct modules for browser management, data extraction, and task orchestration, following SOLID principles.
*   **Resilient & Production-Ready:**
    *   **Asynchronous Operations:** Fully async database and network operations for high performance.
    *   **Dead Letter Queue (DLQ):** Persistently failing URLs are sent to a DLQ in Redis for later analysis.
    *   **12-Factor App Principles:** Aligns with best practices for configuration, logging, and deployment.
    *   **Structured Logging:** Outputs structured JSON logs for effective monitoring in a production environment.


## How to Run

### Prerequisites

1.  **Docker & Docker Compose:** You must have both installed on your local machine.
2.  **Residential Proxy Service:** This is **required** for effective scraping. The framework is designed to integrate with a proxy provider (e.g., Bright Data, Oxylabs, Smartproxy). You will need the proxy endpoint URL they provide. A standard VPN is not sufficient.

### Steps

1.  **Configure Environment:**
    *   Create a file named `.env` in the project root by copying the `.env.example` file.
    *   Open the `.env` file and set the `PROXY_URL` to the one provided by your proxy service. It should look like `http://username:password@proxy.host:port`.

2.  **Build and Run Services:**
    *   Open a terminal in the project root and run:
        ```bash
        docker-compose up --build
        ```
    *   This will build the scraper's Docker image and start the Postgres, Redis, and `scraper-worker` containers. The worker will automatically start and begin listening for URLs in the Redis queue.

3.  **Add URLs to Scrape:**
    *   The worker pulls URLs from the `scraping_queue` in Redis. You can add URLs using any Redis client or by running a separate Python script. For testing purposes, the `scraper.py` script will add a sample Sephora URL on startup if the queue is empty.

## How to Add a New Target Site (e.g., `example.com`)

1.  **Create a Config File:**
    *   In the `sites/` directory, create a new file named `example.com.json`.

2.  **Define Selectors:**
    *   Inspect a product page on `example.com` to find the CSS selectors for the data you need. Prioritize checking for `application/ld+json` structured data first, as the scraper will always prefer that. The CSS selectors are a fallback.
    *   Populate your `example.com.json` file with the selectors:
        ```json
        {
          "selectors": {
            "brand": ".brand-class-name",
            "product_name": "#product-title-id",
            "price_amount": ".price-selector"
          }
        }
        ```

3.  **Add a URL:**
    *   Push a product URL from `example.com` to the `scraping_queue` in Redis. The scraper will automatically detect the domain, load your new configuration, and begin scraping.
