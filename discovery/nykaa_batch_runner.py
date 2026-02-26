import asyncio
import importlib
import json
import os
import time
from dataclasses import asdict, dataclass
from typing import Any, Mapping

from logger import get_logger


logger = get_logger(__name__)


@dataclass
class QueueMetrics:
    pending: int
    processing: int
    dlq: int
    seen: int
    done: int


@dataclass
class RunStats:
    attempts: int = 0
    successes: int = 0
    failures: int = 0
    empty_polls: int = 0

    def record(self, status: str) -> None:
        if status == "empty":
            self.empty_polls += 1
            return
        self.attempts += 1
        if status == "success":
            self.successes += 1
        elif status == "failed":
            self.failures += 1


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return int(value)


def build_queue_namespace_env(prefix: str) -> dict[str, str]:
    clean = (prefix or "").strip()
    if not clean:
        return {}
    clean = clean.replace(" ", "_")
    return {
        "SCRAPING_QUEUE": f"{clean}_scraping_queue",
        "PROCESSING_QUEUE": f"{clean}_scraping_processing",
        "DLQ_QUEUE": f"{clean}_scraping_dlq",
        "SEEN_URLS_SET": f"{clean}_scraping_seen",
        "DONE_URLS_SET": f"{clean}_scraping_done",
    }


def apply_queue_namespace(prefix: str) -> dict[str, str]:
    overrides = build_queue_namespace_env(prefix)
    if overrides:
        os.environ.update(overrides)
    return overrides


def get_queue_metrics(tasks_module: Any) -> QueueMetrics:
    client = getattr(tasks_module, "r", None)
    if client is None:
        raise RuntimeError("Redis client is unavailable.")
    return QueueMetrics(
        pending=int(client.llen(tasks_module.SCRAPING_QUEUE)),
        processing=int(client.llen(tasks_module.PROCESSING_QUEUE)),
        dlq=int(client.llen(tasks_module.DLQ_QUEUE)),
        seen=int(client.scard(tasks_module.SEEN_URLS_SET)),
        done=int(client.scard(tasks_module.DONE_URLS_SET)),
    )


def should_seed(*, enabled: bool, only_if_queue_empty: bool, queue_metrics: QueueMetrics) -> bool:
    if not enabled:
        return False
    if not only_if_queue_empty:
        return True
    return (queue_metrics.pending + queue_metrics.processing) == 0


async def count_products(models_module: Any, source_site: str = "nykaa") -> int:
    from sqlalchemy import func, select
    from sqlmodel.ext.asyncio.session import AsyncSession

    stmt = select(func.count()).select_from(models_module.Product)
    if source_site:
        stmt = stmt.where(models_module.Product.source_site == source_site)

    async with AsyncSession(models_module.engine) as session:
        result = await session.execute(stmt)
        return int(result.scalar_one())


def maybe_apply_seed_aliases() -> None:
    if os.getenv("NYKAA_RUN_SEED_MAX_PRODUCTS") and not os.getenv("NYKAA_SITEMAP_MAX_PRODUCTS"):
        os.environ["NYKAA_SITEMAP_MAX_PRODUCTS"] = os.environ["NYKAA_RUN_SEED_MAX_PRODUCTS"]
    if os.getenv("NYKAA_RUN_SEED_MAX_FILES") and not os.getenv("NYKAA_SITEMAP_MAX_FILES"):
        os.environ["NYKAA_SITEMAP_MAX_FILES"] = os.environ["NYKAA_RUN_SEED_MAX_FILES"]


async def run_nykaa_seeder() -> None:
    from discovery import nykaa_sitemap_seed

    seeder = importlib.reload(nykaa_sitemap_seed)
    await seeder.main()


async def log_checkpoint(
    *,
    label: str,
    tasks_module: Any,
    models_module: Any,
    stats: RunStats,
    start_db_count: int,
    started_at: float,
    source_site: str,
) -> dict[str, Any]:
    queue_metrics = get_queue_metrics(tasks_module)
    db_count = await count_products(models_module, source_site=source_site)
    elapsed = max(time.monotonic() - started_at, 0.001)
    payload = {
        "label": label,
        "elapsed_seconds": round(elapsed, 2),
        "attempts": stats.attempts,
        "successes": stats.successes,
        "failures": stats.failures,
        "empty_polls": stats.empty_polls,
        "throughput_success_per_sec": round(stats.successes / elapsed, 3),
        "db_count_start": start_db_count,
        "db_count_current": db_count,
        "db_count_delta": db_count - start_db_count,
        "queue": asdict(queue_metrics),
    }
    logger.info("Nykaa batch runner checkpoint: %s", json.dumps(payload, sort_keys=True))
    return payload


async def main() -> None:
    queue_namespace = os.getenv("NYKAA_RUN_QUEUE_NAMESPACE", "").strip()
    queue_overrides = apply_queue_namespace(queue_namespace)
    maybe_apply_seed_aliases()

    seed_enabled = env_bool("NYKAA_RUN_SEED", True)
    seed_only_if_queue_empty = env_bool("NYKAA_RUN_SEED_ONLY_IF_QUEUE_EMPTY", True)
    requeue_inflight = env_bool("NYKAA_RUN_REQUEUE_INFLIGHT", True)
    target_successes = env_int("NYKAA_RUN_TARGET_SUCCESS", 1000)
    max_attempts = env_int("NYKAA_RUN_MAX_ATTEMPTS", max(target_successes * 2, 1000))
    progress_every = max(env_int("NYKAA_RUN_PROGRESS_EVERY", 100), 1)
    source_site = os.getenv("NYKAA_RUN_SOURCE_SITE", "nykaa")

    logger.info(
        "Starting Nykaa batch runner with queue_namespace=%s target_successes=%s max_attempts=%s progress_every=%s",
        queue_namespace or "<default>",
        target_successes,
        max_attempts,
        progress_every,
    )
    if queue_overrides:
        logger.info("Applied queue key overrides: %s", json.dumps(queue_overrides, sort_keys=True))

    import tasks as tasks_mod
    import models as models_mod
    from core.api_scraper import ApiScraper
    from core.proxy_manager import ProxyManager
    from core.rate_controller import AdaptiveRateController
    from workers import worker as worker_mod

    tasks_mod = importlib.reload(tasks_mod)

    await models_mod.create_db_and_tables()

    if requeue_inflight:
        recovered = tasks_mod.requeue_inflight_urls()
        if recovered:
            logger.info("Recovered %s in-flight URLs before starting.", recovered)

    initial_queue_metrics = get_queue_metrics(tasks_mod)
    started_at = time.monotonic()
    start_db_count = await count_products(models_mod, source_site=source_site)

    logger.info(
        "Initial state: queue=%s db_count(%s)=%s",
        json.dumps(asdict(initial_queue_metrics), sort_keys=True),
        source_site,
        start_db_count,
    )

    if should_seed(
        enabled=seed_enabled,
        only_if_queue_empty=seed_only_if_queue_empty,
        queue_metrics=initial_queue_metrics,
    ):
        logger.info("Running Nykaa sitemap seeder before extraction.")
        await run_nykaa_seeder()
        await log_checkpoint(
            label="post_seed",
            tasks_module=tasks_mod,
            models_module=models_mod,
            stats=RunStats(),
            start_db_count=start_db_count,
            started_at=started_at,
            source_site=source_site,
        )
    else:
        logger.info("Skipping seeding (seed_enabled=%s, queue not empty=%s).", seed_enabled, initial_queue_metrics.pending > 0)

    proxy_manager = ProxyManager()
    rate_controller = AdaptiveRateController()
    api_scraper = ApiScraper(proxy_manager)
    asyncio.create_task(rate_controller.adjust_rate())

    stats = RunStats()
    stop_reason = "unknown"

    while True:
        if target_successes > 0 and stats.successes >= target_successes:
            stop_reason = "target_successes_reached"
            break
        if max_attempts > 0 and stats.attempts >= max_attempts:
            stop_reason = "max_attempts_reached"
            break

        result = await worker_mod.process_next_queue_item(
            api_scraper,
            rate_controller,
            get_next_url=tasks_mod.get_url_for_processing,
            mark_done_fn=tasks_mod.mark_url_as_done,
            push_dlq_fn=tasks_mod.push_to_dlq,
            db_engine=models_mod.engine,
            poll_when_empty=False,
        )
        status = result.get("status", "unknown")
        stats.record(status)

        if status == "empty":
            stop_reason = "queue_empty"
            break

        if stats.attempts % progress_every == 0:
            await log_checkpoint(
                label=f"progress_{stats.attempts}",
                tasks_module=tasks_mod,
                models_module=models_mod,
                stats=stats,
                start_db_count=start_db_count,
                started_at=started_at,
                source_site=source_site,
            )

    final_payload = await log_checkpoint(
        label="final",
        tasks_module=tasks_mod,
        models_module=models_mod,
        stats=stats,
        start_db_count=start_db_count,
        started_at=started_at,
        source_site=source_site,
    )
    final_payload["stop_reason"] = stop_reason
    final_payload["queue_namespace"] = queue_namespace or None
    logger.info("Nykaa batch runner finished: %s", json.dumps(final_payload, sort_keys=True))
    print(json.dumps(final_payload, sort_keys=True))


if __name__ == "__main__":
    asyncio.run(main())
