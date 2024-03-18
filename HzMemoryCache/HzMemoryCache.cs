#nullable enable
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace hzcache
{
    /// <summary>
    ///     The type of change that occurred to a cache item. Note it's not possible to distinguish between "add" and "update"
    ///     for performance reasons since it will require an additional lookup.
    /// </summary>

    /// <summary>
    ///     Simple MemoryCache alternative. Basically a concurrent dictionary with expiration and cache value change
    ///     notifications.
    /// </summary>
    public sealed class HzMemoryCache : IDisposable, IDetailedHzCache
    {
        private static readonly SemaphoreSlim globalStaticLock = new(1);
        private readonly BlockingCollection<TTLValue> checksumAndNotifyQueue = new();
        private readonly Timer checksumAndNotifyTimer;
        private readonly Timer cleanUpTimer;
        private readonly ConcurrentDictionary<string, TTLValue> dictionary = new();
        private readonly HzCacheOptions options;

        //IDispisable members
        private bool _disposedValue;

        /// <summary>
        ///     Initializes a new empty instance of <see cref="HzMemoryCache" />
        /// </summary>
        /// <param name="options">Options for the cache</param>
        public HzMemoryCache(HzCacheOptions? options = null)
        {
            this.options = options ?? new HzCacheOptions();
            cleanUpTimer = new Timer(s => { _ = ProcessExpiredEviction(); }, null, this.options.cleanupJobInterval, this.options.cleanupJobInterval);
            checksumAndNotifyTimer = new Timer(UpdateChecksumAndNotify, null, 20, 20);
        }

        /// <summary>
        ///     The number of items in the memory cache
        /// </summary>
        public int Count => dictionary.Count;

        /// <summary>
        ///     @see <see cref="IHzCache" />
        /// </summary>
        /// <param name="re"></param>
        /// <param name="sendNotification"></param>
        public void RemoveByRegex(Regex re, bool sendNotification = true)
        {
            var victims = dictionary.Keys.Where(k => re.IsMatch(k)).ToList();
            victims.ForEach(key => RemoveItem(key, CacheItemChangeType.Remove, false));
            if (sendNotification)
            {
                NotifyItemChange(re.ToString(), CacheItemChangeType.Remove, null, 0, true);
            }
        }

        /// <summary>
        ///     @see <see cref="IDetailedHzCache" />
        /// </summary>
        public void EvictExpired()
        {
            if (Monitor.TryEnter(cleanUpTimer)) //use the timer-object for our lock, it's local, private and instance-type, so its ok
            {
                try
                {
                    foreach (var p in dictionary.Where(p => p.Value.IsExpired()))
                    {
                        RemoveItem(p.Key, CacheItemChangeType.Expire, true);
                    }
                }
                finally
                {
                    Monitor.Exit(cleanUpTimer);
                }
            }
        }

        /// <summary>
        ///     @see <see cref="IDetailedHzCache" />
        /// </summary>
        public void Clear()
        {
            var kvps = dictionary.ToArray();
            dictionary.Clear();
            foreach (var kv in kvps)
            {
                NotifyItemChange(kv.Key, kv.Value.IsExpired() ? CacheItemChangeType.Expire : CacheItemChangeType.Remove, kv.Value.checksum, kv.Value.timestampCreated);
            }
        }

        /// <summary>
        ///     @see <see cref="IHzCache" />
        /// </summary>
        public T? Get<T>(string key)
        {
            var defaultValue = default(T);

            if (!dictionary.TryGetValue(key, out var ttlValue))
            {
                return defaultValue;
            }

            if (ttlValue.IsExpired()) //found but expired
            {
                return defaultValue;
            }

            if (options.evictionPolicy == EvictionPolicy.LRU)
            {
                ttlValue.UpdateTimeToKill();
            }

            if (ttlValue.value is T o)
            {
                return o;
            }

            return default;
        }

        /// <summary>
        ///     @see <see cref="IHzCache" />
        /// </summary>
        public void Set<T>(string key, T? value)
        {
            Set(key, value, options.defaultTTL);
        }

        /// <summary>
        ///     @see <see cref="IHzCache" />
        /// </summary>
        public void Set<T>(string key, T? value, TimeSpan ttl)
        {
            Action<TTLValue>? valueChangeListener = options.valueChangeListener != null
                ? tv => NotifyItemChange(key, CacheItemChangeType.AddOrUpdate, tv.checksum, tv.timestampCreated)
                : null;
            var v = new TTLValue(value, ttl, checksumAndNotifyQueue, options.asyncNotifications, valueChangeListener);
            dictionary[key] = v;
        }

        /// <summary>
        ///     @see <see cref="IHzCache" />
        /// </summary>
        public T? GetOrSet<T>(string key, Func<string, T> valueFactory, TimeSpan ttl)
        {
            var value = Get<T>(key);
            if (!IsNullOrDefault(value))
            {
                return value;
            }

            value = valueFactory(key);
            Action<TTLValue>? valueChangeListener = options.valueChangeListener != null
                ? tv => NotifyItemChange(key, CacheItemChangeType.AddOrUpdate, tv.checksum, tv.timestampCreated)
                : null;
            var ttlValue = new TTLValue(value, ttl, checksumAndNotifyQueue, options.asyncNotifications, valueChangeListener);
            dictionary[key] = ttlValue;
            return value;
        }

        /// <summary>
        ///     @see <see cref="IHzCache" />
        /// </summary>
        public bool Remove(string key)
        {
            return Remove(key, true);
        }


        /// <summary>
        ///     @see <see cref="IDetailedHzCache" />
        /// </summary>
        public bool Remove(string key, bool sendBackplaneNotification, Func<string?, bool>? checksumEqualsFunc = null)
        {
            return RemoveItem(key, CacheItemChangeType.Remove, sendBackplaneNotification, checksumEqualsFunc);
        }

        public CacheStatistics GetStatistics()
        {
            return new CacheStatistics {Counts = this.Count, SizeInBytes = this.dictionary.Values.Sum(v => v.objectSize)};
        }

        /// <summary>
        ///     Dispose.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
        }

        private void UpdateChecksumAndNotify(object state)
        {
            while (checksumAndNotifyQueue.TryTake(out var ttlValue))
            {
                ttlValue.UpdateChecksum();
            }
        }

        private async Task ProcessExpiredEviction()
        {
            await globalStaticLock.WaitAsync().ConfigureAwait(false);
            try
            {
                EvictExpired();
            }
            finally { globalStaticLock.Release(); }
        }

        private bool RemoveItem(string key, CacheItemChangeType changeType, bool sendNotification, Func<string?, bool>? checksumEqualsFunc = null)
        {
            var valueExists = dictionary.TryGetValue(key, out TTLValue ttlValue);
            if (!valueExists)
            {
                return false;
            }

            if (checksumEqualsFunc?.Invoke(ttlValue.checksum) ?? false)
            {
                return false;
            }

            var result = dictionary.TryRemove(key, out ttlValue);
            if (result)
            {
                if (sendNotification)
                {
                    NotifyItemChange(key, changeType, ttlValue.checksum, ttlValue.timestampCreated);
                }

                return !ttlValue.IsExpired();
            }

            return false;
        }

        private void NotifyItemChange(string key, CacheItemChangeType changeType, string? checksum, long timestamp, bool isRegexp = false)
        {
            options.valueChangeListener?.Invoke(key, changeType, checksum, timestamp, isRegexp);
        }

        private void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    cleanUpTimer.Dispose();
                    checksumAndNotifyTimer.Dispose();
                }

                _disposedValue = true;
            }
        }
        
        public static bool IsNullOrDefault<T>(T argument)
        {
            // deal with normal scenarios
            if (argument == null) return true;
            if (object.Equals(argument, default(T))) return true;

            // deal with non-null nullables
            Type methodType = typeof(T);
            if (Nullable.GetUnderlyingType(methodType) != null) return false;

            // deal with boxed value types
            Type argumentType = argument.GetType();
            if (argumentType.IsValueType && argumentType != methodType) 
            {
                object obj = Activator.CreateInstance(argument.GetType());
                return obj.Equals(argument);
            }

            return false;
        }
    }
}
