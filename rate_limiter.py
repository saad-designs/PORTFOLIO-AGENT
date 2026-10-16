import asyncio
import threading
import time
from functools import wraps
from langchain_google_genai import ChatGoogleGenerativeAI, GoogleGenerativeAIEmbeddings

class UnifiedRateLimiter:
    def __init__(self, max_concurrent: int, rpm_limit: int):
        self.rpm_limit = rpm_limit
        self.period = 60.0
        self.calls = []
        
        # Concurrency constraints
        self.max_concurrent = max_concurrent
        self.async_semaphore = None  # Lazily instantiated to bind to correct event loop
        self.sync_semaphore = threading.Semaphore(max_concurrent)
        
        self._lock = threading.Lock()
        
    def _get_async_semaphore(self):
        if self.async_semaphore is None:
            self.async_semaphore = asyncio.Semaphore(self.max_concurrent)
        return self.async_semaphore

    async def async_acquire(self):
        # 1. Enforce Concurrency
        await self._get_async_semaphore().acquire()
        try:
            # 2. Enforce RPM limit
            while True:
                with self._lock:
                    now = time.time()
                    self.calls = [t for t in self.calls if now - t < self.period]
                    if len(self.calls) < self.rpm_limit:
                        self.calls.append(now)
                        return
                    else:
                        sleep_time = self.period - (now - self.calls[0])
                
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
        except Exception:
            self._get_async_semaphore().release()
            raise
            
    def async_release(self):
        if self.async_semaphore:
            self.async_semaphore.release()

    def sync_acquire(self):
        # 1. Enforce Concurrency
        self.sync_semaphore.acquire()
        try:
            # 2. Enforce RPM limit
            while True:
                with self._lock:
                    now = time.time()
                    self.calls = [t for t in self.calls if now - t < self.period]
                    if len(self.calls) < self.rpm_limit:
                        self.calls.append(now)
                        return
                    else:
                        sleep_time = self.period - (now - self.calls[0])
                
                if sleep_time > 0:
                    time.sleep(sleep_time)
        except Exception:
            self.sync_semaphore.release()
            raise

    def sync_release(self):
        self.sync_semaphore.release()


_limiter = None

def get_limiter():
    global _limiter
    if _limiter is None:
        # 2 concurrent requests max, 14 requests per minute limit (leaves 1 RPM as safe buffer)
        _limiter = UnifiedRateLimiter(max_concurrent=2, rpm_limit=14)
    return _limiter

def apply_global_rate_limits():
    """
    Monkey-patches langchain_google_genai and google.genai classes to throttle API calls using a global Semaphore & RPM Limiter.
    This resolves the 429 RESOURCE_EXHAUSTED errors on Gemini Free Tier.
    """
    print("Applying global rate limits (Concurrency=2, RPM=14) to all Gemini SDK calls...")
    
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
        await limiter.async_acquire()
        try:
            return await original_agenerate(*args, **kwargs)
        finally:
            limiter.async_release()

    @wraps(original_generate)
    def throttled_generate(*args, **kwargs):
        limiter.sync_acquire()
        try:
            return original_generate(*args, **kwargs)
        finally:
            limiter.sync_release()

    @wraps(original_aembed_documents)
    async def throttled_aembed_documents(*args, **kwargs):
        await limiter.async_acquire()
        try:
            return await original_aembed_documents(*args, **kwargs)
        finally:
            limiter.async_release()

    @wraps(original_embed_documents)
    def throttled_embed_documents(*args, **kwargs):
        limiter.sync_acquire()
        try:
            return original_embed_documents(*args, **kwargs)
        finally:
            limiter.sync_release()

    @wraps(original_aembed_query)
    async def throttled_aembed_query(*args, **kwargs):
        await limiter.async_acquire()
        try:
            return await original_aembed_query(*args, **kwargs)
        finally:
            limiter.async_release()

    @wraps(original_embed_query)
    def throttled_embed_query(*args, **kwargs):
        limiter.sync_acquire()
        try:
            return original_embed_query(*args, **kwargs)
        finally:
            limiter.sync_release()

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
            # Ensure generous default timeout of at least 300,000 ms (300 seconds)
            if 'http_options' not in kwargs or kwargs['http_options'] is None:
                kwargs['http_options'] = {'timeout': 300000}
            elif isinstance(kwargs['http_options'], dict):
                if kwargs['http_options'].get('timeout') is None:
                    kwargs['http_options']['timeout'] = 300000
                else:
                    kwargs['http_options']['timeout'] = max(int(kwargs['http_options']['timeout']), 300000)
            elif hasattr(kwargs['http_options'], 'timeout'):
                current_t = getattr(kwargs['http_options'], 'timeout')
                if current_t is None or (isinstance(current_t, (int, float)) and current_t < 300000):
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
            if hasattr(http_request, 'timeout'):
                if http_request.timeout is None or (isinstance(http_request.timeout, (int, float)) and http_request.timeout < 300.0):
                    http_request.timeout = 300.0
            
            if is_bypass_request(http_request):
                return await original_async_request_once(self, http_request, *args, **kwargs)

            await limiter.async_acquire()
            try:
                return await original_async_request_once(self, http_request, *args, **kwargs)
            finally:
                limiter.async_release()
                
        @wraps(original_request_once)
        def throttled_request_once(self, http_request, *args, **kwargs):
            if hasattr(http_request, 'timeout'):
                if http_request.timeout is None or (isinstance(http_request.timeout, (int, float)) and http_request.timeout < 300.0):
                    http_request.timeout = 300.0
            
            if is_bypass_request(http_request):
                return original_request_once(self, http_request, *args, **kwargs)

            limiter.sync_acquire()
            try:
                return original_request_once(self, http_request, *args, **kwargs)
            finally:
                limiter.sync_release()
                
        BaseApiClient._async_request_once = throttled_async_request_once
        BaseApiClient._request_once = throttled_request_once
        print("Patched native google.genai HTTP clients successfully.")
    except ImportError:
        pass

