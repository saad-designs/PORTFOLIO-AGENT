import asyncio
import contextvars
import threading
import time
from contextlib import asynccontextmanager, contextmanager
from functools import wraps
from langchain_google_genai import ChatGoogleGenerativeAI, GoogleGenerativeAIEmbeddings

# Context flag marking user-initiated interactive requests (e.g. /api/dashboard/chat)
# so they bypass background sweep queues (YouTube / news / sentinel) immediately.
USER_PRIORITY_CTX: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "gemini_user_priority", default=False
)


@contextmanager
def user_priority_context():
    """Marks all Gemini calls inside this block (and spawned coroutines/threads) as high-priority user chat."""
    token = USER_PRIORITY_CTX.set(True)
    try:
        yield
    finally:
        USER_PRIORITY_CTX.reset(token)


def is_user_priority() -> bool:
    return bool(USER_PRIORITY_CTX.get())


class UnifiedRateLimiter:
    def __init__(self, max_concurrent: int, rpm_limit: int):
        self.rpm_limit = rpm_limit
        self.period = 60.0
        self.calls = []

        # Background concurrency constraints (YouTube, news, sentinel, embeddings)
        self.max_concurrent = max_concurrent
        self.async_semaphore = None
        self._async_semaphores = {}
        self.sync_semaphore = threading.Semaphore(max_concurrent)

        # Dedicated high-priority lane for interactive user chat requests
        self.priority_max_concurrent = max(2, max_concurrent)
        self._priority_async_semaphores = {}
        self.priority_sync_semaphore = threading.Semaphore(self.priority_max_concurrent)
        self.priority_waiters = 0
        self.priority_active = 0

        self._lock = threading.Lock()

    def _get_async_semaphore(self):
        loop = asyncio.get_running_loop()
        lid = id(loop)
        sem = self._async_semaphores.get(lid)
        if sem is None:
            sem = asyncio.Semaphore(self.max_concurrent)
            self._async_semaphores[lid] = sem
            self.async_semaphore = sem
        return sem

    def _get_priority_async_semaphore(self):
        loop = asyncio.get_running_loop()
        lid = id(loop)
        sem = self._priority_async_semaphores.get(lid)
        if sem is None:
            sem = asyncio.Semaphore(self.priority_max_concurrent)
            self._priority_async_semaphores[lid] = sem
        return sem

    @asynccontextmanager
    async def async_slot(self):
        if is_user_priority():
            sem = self._get_priority_async_semaphore()
            with self._lock:
                self.priority_waiters += 1
            try:
                await sem.acquire()
            finally:
                with self._lock:
                    self.priority_waiters = max(0, self.priority_waiters - 1)

            with self._lock:
                self.priority_active += 1
                now = time.time()
                self.calls = [t for t in self.calls if now - t < self.period]
                self.calls.append(now)
            try:
                yield
            finally:
                with self._lock:
                    self.priority_active = max(0, self.priority_active - 1)
                sem.release()
        else:
            sem = self._get_async_semaphore()
            await sem.acquire()
            try:
                while True:
                    sleep_time = 0.0
                    with self._lock:
                        # Yield immediately while any user-initiated chat call is waiting or active
                        if self.priority_waiters > 0 or self.priority_active > 0:
                            sleep_time = 0.25
                        else:
                            now = time.time()
                            self.calls = [t for t in self.calls if now - t < self.period]
                            if len(self.calls) < self.rpm_limit:
                                self.calls.append(now)
                                break
                            sleep_time = self.period - (now - self.calls[0])

                    if sleep_time > 0:
                        await asyncio.sleep(sleep_time)
                yield
            finally:
                sem.release()

    @contextmanager
    def sync_slot(self):
        if is_user_priority():
            with self._lock:
                self.priority_waiters += 1
            try:
                self.priority_sync_semaphore.acquire()
            finally:
                with self._lock:
                    self.priority_waiters = max(0, self.priority_waiters - 1)

            with self._lock:
                self.priority_active += 1
                now = time.time()
                self.calls = [t for t in self.calls if now - t < self.period]
                self.calls.append(now)
            try:
                yield
            finally:
                with self._lock:
                    self.priority_active = max(0, self.priority_active - 1)
                self.priority_sync_semaphore.release()
        else:
            self.sync_semaphore.acquire()
            try:
                while True:
                    sleep_time = 0.0
                    with self._lock:
                        if self.priority_waiters > 0 or self.priority_active > 0:
                            sleep_time = 0.25
                        else:
                            now = time.time()
                            self.calls = [t for t in self.calls if now - t < self.period]
                            if len(self.calls) < self.rpm_limit:
                                self.calls.append(now)
                                break
                            sleep_time = self.period - (now - self.calls[0])

                    if sleep_time > 0:
                        time.sleep(sleep_time)
                yield
            finally:
                self.sync_semaphore.release()

    async def async_acquire(self):
        await self._get_async_semaphore().acquire()
        try:
            while True:
                with self._lock:
                    now = time.time()
                    self.calls = [t for t in self.calls if now - t < self.period]
                    if len(self.calls) < self.rpm_limit:
                        self.calls.append(now)
                        return
                    sleep_time = self.period - (now - self.calls[0])
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
        except BaseException:
            self._get_async_semaphore().release()
            raise

    def async_release(self):
        self._get_async_semaphore().release()

    def sync_acquire(self):
        self.sync_semaphore.acquire()
        try:
            while True:
                with self._lock:
                    now = time.time()
                    self.calls = [t for t in self.calls if now - t < self.period]
                    if len(self.calls) < self.rpm_limit:
                        self.calls.append(now)
                        return
                    sleep_time = self.period - (now - self.calls[0])
                if sleep_time > 0:
                    time.sleep(sleep_time)
        except BaseException:
            self.sync_semaphore.release()
            raise

    def sync_release(self):
        self.sync_semaphore.release()


_limiter = None


def get_limiter():
    global _limiter
    if _limiter is None:
        # 2 concurrent background requests max, 14 requests per minute limit (leaves 1 RPM as safe buffer)
        _limiter = UnifiedRateLimiter(max_concurrent=2, rpm_limit=14)
    return _limiter


def apply_global_rate_limits():
    """
    Monkey-patches langchain_google_genai and google.genai classes to throttle background API calls
    using a global Semaphore & RPM Limiter, while giving user-initiated chat priority queueing.
    """
    print("Applying global rate limits (Concurrency=2, RPM=14, UserChat=Priority) to all Gemini SDK calls...")

    limiter = get_limiter()

    # --- 1. LangChain Wrappers ---
    original_agenerate = ChatGoogleGenerativeAI._agenerate
    original_generate = ChatGoogleGenerativeAI._generate

    original_aembed_documents = GoogleGenerativeAIEmbeddings.aembed_documents
    original_embed_documents = GoogleGenerativeAIEmbeddings.embed_documents

    original_aembed_query = GoogleGenerativeAIEmbeddings.aembed_query
    original_embed_query = GoogleGenerativeAIEmbeddings.embed_query

    @wraps(original_agenerate)
    async def throttled_agenerate(*args, **kwargs):
        async with limiter.async_slot():
            return await original_agenerate(*args, **kwargs)

    @wraps(original_generate)
    def throttled_generate(*args, **kwargs):
        with limiter.sync_slot():
            return original_generate(*args, **kwargs)

    @wraps(original_aembed_documents)
    async def throttled_aembed_documents(*args, **kwargs):
        async with limiter.async_slot():
            return await original_aembed_documents(*args, **kwargs)

    @wraps(original_embed_documents)
    def throttled_embed_documents(*args, **kwargs):
        with limiter.sync_slot():
            return original_embed_documents(*args, **kwargs)

    @wraps(original_aembed_query)
    async def throttled_aembed_query(*args, **kwargs):
        async with limiter.async_slot():
            return await original_aembed_query(*args, **kwargs)

    @wraps(original_embed_query)
    def throttled_embed_query(*args, **kwargs):
        with limiter.sync_slot():
            return original_embed_query(*args, **kwargs)

    ChatGoogleGenerativeAI._agenerate = throttled_agenerate
    ChatGoogleGenerativeAI._generate = throttled_generate
    GoogleGenerativeAIEmbeddings.aembed_documents = throttled_aembed_documents
    GoogleGenerativeAIEmbeddings.embed_documents = throttled_embed_documents
    GoogleGenerativeAIEmbeddings.aembed_query = throttled_aembed_query
    GoogleGenerativeAIEmbeddings.embed_query = throttled_embed_query

    # --- 2. Direct google.genai SDK Calls ---
    try:
        from google.genai._api_client import BaseApiClient

        original_init = BaseApiClient.__init__

        @wraps(original_init)
        def patched_init(self, *args, **kwargs):
            # Preserve explicit short timeouts on priority user chat; default background sweeps to 300,000 ms
            prio = is_user_priority()
            default_ms = 28000 if prio else 300000
            if 'http_options' not in kwargs or kwargs['http_options'] is None:
                kwargs['http_options'] = {'timeout': default_ms}
            elif isinstance(kwargs['http_options'], dict):
                if kwargs['http_options'].get('timeout') is None:
                    kwargs['http_options']['timeout'] = default_ms
                elif not prio:
                    kwargs['http_options']['timeout'] = max(int(kwargs['http_options']['timeout']), 300000)
            elif hasattr(kwargs['http_options'], 'timeout'):
                current_t = getattr(kwargs['http_options'], 'timeout')
                if current_t is None:
                    setattr(kwargs['http_options'], 'timeout', default_ms)
                elif not prio and isinstance(current_t, (int, float)) and current_t < 300000:
                    setattr(kwargs['http_options'], 'timeout', 300000)
            original_init(self, *args, **kwargs)

        BaseApiClient.__init__ = patched_init

        original_async_request_once = BaseApiClient._async_request_once
        original_request_once = BaseApiClient._request_once

        def is_bypass_request(http_request):
            url_str = str(getattr(http_request, 'url', '')).lower()
            return any(kw in url_str for kw in ['upload', '/files', 'media', 'download'])

        @wraps(original_async_request_once)
        async def throttled_async_request_once(self, http_request, *args, **kwargs):
            prio = is_user_priority()
            if hasattr(http_request, 'timeout'):
                if prio:
                    if http_request.timeout is None or (isinstance(http_request.timeout, (int, float)) and http_request.timeout > 30.0):
                        http_request.timeout = 28.0
                else:
                    if http_request.timeout is None or (isinstance(http_request.timeout, (int, float)) and http_request.timeout < 300.0):
                        http_request.timeout = 300.0

            if is_bypass_request(http_request):
                return await original_async_request_once(self, http_request, *args, **kwargs)

            async with limiter.async_slot():
                return await original_async_request_once(self, http_request, *args, **kwargs)

        @wraps(original_request_once)
        def throttled_request_once(self, http_request, *args, **kwargs):
            prio = is_user_priority()
            if hasattr(http_request, 'timeout'):
                if prio:
                    if http_request.timeout is None or (isinstance(http_request.timeout, (int, float)) and http_request.timeout > 30.0):
                        http_request.timeout = 28.0
                else:
                    if http_request.timeout is None or (isinstance(http_request.timeout, (int, float)) and http_request.timeout < 300.0):
                        http_request.timeout = 300.0

            if is_bypass_request(http_request):
                return original_request_once(self, http_request, *args, **kwargs)

            with limiter.sync_slot():
                return original_request_once(self, http_request, *args, **kwargs)

        BaseApiClient._async_request_once = throttled_async_request_once
        BaseApiClient._request_once = throttled_request_once
        print("Patched native google.genai HTTP clients successfully.")
    except ImportError:
        pass

