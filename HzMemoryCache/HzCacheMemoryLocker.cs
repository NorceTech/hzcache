using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;

namespace HzCache
{
    public class HzCacheMemoryLockerOptions
    {
        public int lockPoolSize { get; set; } = 7872;
    }

    public class HzCacheMemoryLocker
    {
        private readonly MemoryCache lockCache = new(new MemoryCacheOptions());
        private readonly object[] lockPool;
        private readonly HzCacheMemoryLockerOptions options;
        private readonly TimeSpan slidingExpiration = TimeSpan.FromMinutes(5);

        public HzCacheMemoryLocker(HzCacheMemoryLockerOptions options)
        {
            this.options = options;
            lockPool = new object[options.lockPoolSize];
            for (var i = 0; i < lockPool.Length; i++)
            {
                lockPool[i] = new object();
            }
        }

        private uint GetLockIndex(string key)
        {
            return unchecked((uint)key.GetHashCode()) % (uint)options.lockPoolSize;
        }

        private SemaphoreSlim GetSemaphore(string cacheName, string cacheInstanceId, string key, ILogger? logger)
        {
            object? _semaphore;

            if (lockCache.TryGetValue(key, out _semaphore))
            {
                return (SemaphoreSlim)_semaphore!;
            }

            lock (lockPool[GetLockIndex(key)])
            {
                if (lockCache.TryGetValue(key, out _semaphore))
                {
                    return (SemaphoreSlim)_semaphore!;
                }

                _semaphore = new SemaphoreSlim(1, 1);

                using var entry = lockCache.CreateEntry(key);
                entry.Value = _semaphore;
                entry.SlidingExpiration = slidingExpiration;
                entry.RegisterPostEvictionCallback((key, value, _, _) =>
                {
                    try
                    {
                        ((SemaphoreSlim?)value)?.Dispose();
                    }
                    catch (Exception exc)
                    {
                        if (logger?.IsEnabled(LogLevel.Warning) ?? false)
                        {
                            logger.Log(LogLevel.Warning, exc,
                                "FUSION [N={CacheName} I={CacheInstanceId}] (K={CacheKey}): an error occurred while trying to dispose a SemaphoreSlim in the memory locker",
                                cacheName, cacheInstanceId, key);
                        }
                    }
                });

                return (SemaphoreSlim)_semaphore;
            }
        }

        /// <inheritdoc />
        public async ValueTask<object> AcquireLockAsync(string cacheName, string cacheInstanceId, string operationId, string key, TimeSpan timeout, ILogger? logger,
            CancellationToken token)
        {
            var semaphore = GetSemaphore(cacheName, cacheInstanceId, key, logger);

            if (logger?.IsEnabled(LogLevel.Trace) ?? false)
            {
                logger.Log(LogLevel.Trace, "FUSION [N={CacheName} I={CacheInstanceId}] (O={CacheOperationId} K={CacheKey}): waiting to acquire the LOCK", cacheName,
                    cacheInstanceId, operationId, key);
            }

            var acquired = await semaphore.WaitAsync(timeout, token).ConfigureAwait(false);

            if (acquired)
            {
                // LOCK ACQUIRED
                if (logger?.IsEnabled(LogLevel.Trace) ?? false)
                {
                    logger.Log(LogLevel.Trace, "FUSION [N={CacheName} I={CacheInstanceId}] (O={CacheOperationId} K={CacheKey}): LOCK acquired", cacheName, cacheInstanceId,
                        operationId, key);
                }
            }
            else
            {
                // LOCK TIMEOUT
                if (logger?.IsEnabled(LogLevel.Trace) ?? false)
                {
                    logger.Log(LogLevel.Trace, "FUSION [N={CacheName} I={CacheInstanceId}] (O={CacheOperationId} K={CacheKey}): LOCK timeout", cacheName, cacheInstanceId,
                        operationId, key);
                }
            }

            return acquired ? semaphore : null;
        }

        /// <inheritdoc />
        public object? AcquireLock(string cacheName, string cacheInstanceId, string operationId, string key, TimeSpan timeout, ILogger? logger, CancellationToken token)
        {
            var semaphore = GetSemaphore(cacheName, cacheInstanceId, key, logger);

            if (logger?.IsEnabled(LogLevel.Trace) ?? false)
            {
                logger.Log(LogLevel.Trace, "FUSION [N={CacheName} I={CacheInstanceId}] (O={CacheOperationId} K={CacheKey}): waiting to acquire the LOCK", cacheName,
                    cacheInstanceId, operationId, key);
            }

            var acquired = semaphore.Wait(timeout, token);

            if (acquired)
            {
                // LOCK ACQUIRED
                if (logger?.IsEnabled(LogLevel.Trace) ?? false)
                {
                    logger.Log(LogLevel.Trace, "FUSION [N={CacheName} I={CacheInstanceId}] (O={CacheOperationId} K={CacheKey}): LOCK acquired", cacheName, cacheInstanceId,
                        operationId, key);
                }
            }
            else
            {
                // LOCK TIMEOUT
                if (logger?.IsEnabled(LogLevel.Trace) ?? false)
                {
                    logger.Log(LogLevel.Trace, "FUSION [N={CacheName} I={CacheInstanceId}] (O={CacheOperationId} K={CacheKey}): LOCK timeout", cacheName, cacheInstanceId,
                        operationId, key);
                }
            }

            return acquired ? semaphore : null;
        }

        /// <inheritdoc />
        public void ReleaseLock(string cacheName, string cacheInstanceId, string operationId, string key, object? lockObj, ILogger? logger)
        {
            if (lockObj is null)
            {
                return;
            }

            try
            {
                ((SemaphoreSlim)lockObj).Release();
            }
            catch (Exception exc)
            {
                if (logger?.IsEnabled(LogLevel.Warning) ?? false)
                {
                    logger.Log(LogLevel.Warning, exc,
                        "FUSION [N={CacheName} I={CacheInstanceId}] (O={CacheOperationId} K={CacheKey}): an error occurred while trying to release a SemaphoreSlim in the memory locker",
                        cacheName, cacheInstanceId, operationId, key);
                }
            }
        }
    }
}
