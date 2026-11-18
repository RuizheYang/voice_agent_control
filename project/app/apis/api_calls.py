import logging
import asyncio
from typing import Any, Optional, Callable, Awaitable, Union, List
import aiohttp


logger = logging.getLogger(__name__)

_session: Optional[aiohttp.ClientSession] = None

def get_session() -> aiohttp.ClientSession:
    global _session
    try:
        current_loop = asyncio.get_running_loop()
    except RuntimeError:
        current_loop = None

    if _session is None or _session.closed or (current_loop and getattr(_session, "_loop", None) and _session._loop != current_loop):
        _session = aiohttp.ClientSession(trust_env=True)
    return _session

async def close_session() -> None:
    global _session
    if _session and not _session.closed:
        await _session.close()
        _session = None

async def do_post(
    url: str,
    headers: dict[str, str],
    payload: dict[str, Any], 
    clue_id: str = None,
    timeout_seconds: Union[int, float] = 5,
    retry_times: int = 3,
    retry_interval_seconds: Union[int, float, List[float]] = 5,
    should_retry_fn: Optional[Callable[[aiohttp.ClientResponse], Awaitable[bool]]] = None
) -> tuple[int, str, Optional[dict[str, Any]]]:

    timeout = aiohttp.ClientTimeout(total=timeout_seconds)

    session = get_session()

    for attempt in range(1, retry_times + 1):
        if isinstance(retry_interval_seconds, (list, tuple)):
            idx = attempt - 1
            interval = retry_interval_seconds[idx] if idx < len(retry_interval_seconds) else retry_interval_seconds[-1]
        else:
            interval = float(retry_interval_seconds)

        try:
            # TCP连接池复用，timeout需在单次请求里精确指定
            async with session.post(url, headers=headers, json=payload, timeout=timeout) as response:
                text = await response.text()
                try:
                    data = await response.json(content_type=None)
                except Exception:
                    data = None

                if response.status >= 500 and attempt < retry_times:
                    logger.warning(f"{url} clueid {clue_id} post json got {response.status}, retrying ({attempt}/{retry_times})")
                    await asyncio.sleep(interval)
                    continue
                
                if should_retry_fn and await should_retry_fn(response) and attempt < retry_times:
                    logger.warning(f"{url} clueid {clue_id} got business error, retrying ({attempt}/{retry_times})")
                    await asyncio.sleep(interval)
                    continue

                return response.status, text, data
            
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt >= retry_times:
                raise
            logger.warning(f"{url} clueid {clue_id} post json failed (Error: {type(e).__name__}), retrying ({attempt}/{retry_times})")
            await asyncio.sleep(interval)
