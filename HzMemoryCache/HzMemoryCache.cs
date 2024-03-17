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
    public enum CacheItemChangeType
    {
        /// <summary>
        ///     A cache item was added or updated
        /// </summary>
        AddOrUpdate, 
        /// <summary>
        ///     A cache item was removed
        /// </summary>
        Remove, 
        /// <summary>
        ///     A cache item expired
        /// </summary>
        Expire
    }

    /// <summary>
    ///     The eviction policy to use for the cache.
    ///     LRU is "Least Recently Used" and FIFO is "First In First Out". Which is almost true.
    ///     LRU means that the item expiry TTL is extended on read, and FIFO means that the item
    ///     expiry TTL is set on write. So if all items have the same expiry, then it's LRU and FIFO.
    /// </summary>
    public enum EvictionPolicy
    {
        /// <summary>
        ///     LRU means that the item expiry TTL is set on write and extended on read.
        /// </summary>
        LRU, 
        /// <summary>
        ///     FIFO means that the item expiry TTL is set on write never updated, thus being evicted
        ///     when TTL expires independently on how often it's read.
        /// </summary>
        FIFO
    }

    /// <summary>
    ///     Configuration options for hzcache.
    /// </summary>
    public class HzCacheOptions
    {
        /// <summary>
        ///     How frequently the cache should clean up expired items. Defaults to 1 second.
        /// </summary>
        public int cleanupJobInterval { get; set; } = 1000;

        /// <summary>
        ///     The default TTL for items added to the cache. Defaults to 5 minutes.
        /// </summary>
        public TimeSpan defaultTTL { get; set; } = TimeSpan.FromMinutes(5);

        /// <summary>
        ///     The listener for value changes in the cache. The first parameter is the key, the second is the change type,
        ///     the third is the checksum of the value, the fourth is the insert timestamp (Unix Time in ms) of the item,
        ///     and the fifth is a boolean indicating if the key is a regex pattern.
        /// </summary>
        public Action<string, CacheItemChangeType, string?, long, bool>? valueChangeListener { get; set; }

        /// <summary>
        ///     Whether or not to send notifications asynchronously. Defaults to true.
        /// </summary>
        public bool asyncNotifications { get; set; } = true;

        /// <summary>
        ///     Eviction policy to use for the cache. Defaults to LRU.
        /// </summary>
        public EvictionPolicy evictionPolicy { get; set; } = EvictionPolicy.LRU;
    }

    public interface IHzCache
    {
        /// <summary>
        ///     Removes cache keys based on a regex pattern.
        /// </summary>
        /// <param name="re"></param>
        /// <param name="sendNotification"></param>
        void RemoveByRegex(Regex re, bool sendNotification = true);

        /// <summary>
        ///     Attempts to get a value by key
        /// </summary>
        /// <param name="key">The key to get</param>
        /// <returns>True if value exists, otherwise false</returns>
        T? Get<T>(string key) where T : class;

        /// <summary>
        ///     Attempts to add a key/value item
        /// </summary>
        /// <param name="key">The key to add</param>
        /// <param name="value">The value to add</param>
        /// <returns>True if value was added, otherwise false (already exists)</returns>
        void Set<T>(string key, T? value) where T : class;

        /// <summary>
        ///     Adds a key/value pair. This method could potentially be optimized, but not sure as of now.
        ///     The initial "TryGetValue" likely costs a bit, which adds time and makes it slower than
        ///     a MemoryCache.Set. However, the "TryGetValue" is required to be able to send the correct
        ///     message type to the listener.
        /// </summary>
        /// <param name="key">The key to add</param>
        /// <param name="value">The value to add</param>
        /// <param name="ttl">TTL of the item</param>
        /// <returns>True if value was added, otherwise false (already exists)</returns>
        void Set<T>(string key, T? value, TimeSpan ttl) where T : class;

        /// <summary>
        ///     Adds a key/value pair by using the specified function if the key does not already exist, or returns the existing
        ///     value if the key exists.
        /// </summary>
        /// <param name="key">The key to add</param>
        /// <param name="valueFactory">The factory function used to generate the item for the key</param>
        /// <param name="ttl">TTL of the item</param>
        T? GetOrSet<T>(string key, Func<string, T> valueFactory, TimeSpan ttl) where T : class;

        /// <summary>
        ///     Tries to remove item with the specified key, also returns the object removed in an "out" var
        /// </summary>
        /// <param name="key">The key of the element to remove</param>
        bool Remove(string key);
    }

    public interface IDetailedHzCache : IHzCache
    {
        /// <summary>
        ///     Cleans up expired items (dont' wait for the background job)
        ///     There's rarely a need to execute this method, b/c getting an item checks TTL anyway.
        /// </summary>
        void EvictExpired();

        /// <summary>
        ///     Removes all items from the cache
        /// </summary>
        void Clear();


        /// <summary>
        ///     Tries to remove item with the specified key, also returns the object removed in an "out" var
        /// </summary>
        /// <param name="key">The key of the element to remove</param>
        /// <param name="sendBackplaneNotification">Send backplane notification or not</param>
        /// <param name="checksumEqualsFunc">If function returns true, skip removing the entry</param>
        bool Remove(string key, bool sendBackplaneNotification = true, Func<string?, bool>? checksumEqualsFunc = null);
    }

    /// <summary>
    ///     Simple MemoryCache alternative. Basically a concurrent dictionary with expiration and cache value change
    ///     notifications.
    /// </summary>
    public class HzMemoryCache : IDisposable, IDetailedHzCache
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
        public T? Get<T>(string key) where T : class
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

            return null;
        }

        /// <summary>
        ///     @see <see cref="IHzCache" />
        /// </summary>
        public void Set<T>(string key, T? value) where T : class
        {
            Set(key, value, options.defaultTTL);
        }

        /// <summary>
        ///     @see <see cref="IHzCache" />
        /// </summary>
        public void Set<T>(string key, T? value, TimeSpan ttl) where T : class
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
        public T? GetOrSet<T>(string key, Func<string, T> valueFactory, TimeSpan ttl) where T : class
        {
            var value = Get<T>(key);
            if (value != null)
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

        protected virtual void Dispose(bool disposing)
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
    }
}
