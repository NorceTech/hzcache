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
    /// The type of change that occurred to a cache item. Note it's not possible to distinguish between "add" and "update"
    /// for performance reasons since it will require an additional lookup.
    /// </summary>
    public enum CacheItemChangeType
    {
        AddOrUpdate, Remove, Expire
    }

    public enum EvictionPolicy
    {
        LRU, FIFO
    }

    public class HzCacheOptions
    {
        public int cleanupJobInterval { get; set; } = 1000;
        public TimeSpan defaultTTL { get; set; } = TimeSpan.FromMinutes(5);
        public Action<string, CacheItemChangeType, string?, long, bool> valueChangeListener { get; set; } = (_, _, _, _, _) => { };
        public bool asyncNotifications { get; set; }
        public EvictionPolicy evictionPolicy { get; set; } = EvictionPolicy.LRU;
    }

    public interface IHzCache
    {
        /// <summary>
        /// Removes cache keys based on a regex pattern.
        /// </summary>
        /// <param name="re"></param>
        /// <param name="sendNotification"></param>
        void RemoveByRegex(Regex re, bool sendNotification = true);

        /// <summary>
        /// Attempts to get a value by key
        /// </summary>
        /// <param name="key">The key to get</param>
        /// <returns>True if value exists, otherwise false</returns>
        T? Get<T>(string key) where T : class;

        /// <summary>
        /// Attempts to add a key/value item
        /// </summary>
        /// <param name="key">The key to add</param>
        /// <param name="value">The value to add</param>
        /// <returns>True if value was added, otherwise false (already exists)</returns>
        void Set<T>(string key, T? value) where T : class;

        /// <summary>
        /// Adds a key/value pair. This method could potentially be optimized, but not sure as of now.
        /// The initial "TryGetValue" likely costs a bit, which adds time and makes it slower than
        /// a MemoryCache.Set. However, the "TryGetValue" is required to be able to send the correct
        /// message type to the listener.
        /// </summary>
        /// <param name="key">The key to add</param>
        /// <param name="value">The value to add</param>
        /// <param name="ttl">TTL of the item</param>
        /// <returns>True if value was added, otherwise false (already exists)</returns>
        void Set<T>(string key, T? value, TimeSpan ttl) where T : class;

        /// <summary>
        /// Adds a key/value pair by using the specified function if the key does not already exist, or returns the existing value if the key exists.
        /// </summary>
        /// <param name="key">The key to add</param>
        /// <param name="valueFactory">The factory function used to generate the item for the key</param>
        /// <param name="ttl">TTL of the item</param>
        T? GetOrSet<T>(string key, Func<string, T> valueFactory, TimeSpan ttl) where T : class;

        /// <summary>
        /// Tries to remove item with the specified key, also returns the object removed in an "out" var
        /// </summary>
        /// <param name="key">The key of the element to remove</param>
        bool Remove(string key);
    }

    public interface IDetailedHzCache : IHzCache
    {
        /// <summary>
        /// Cleans up expired items (dont' wait for the background job)
        /// There's rarely a need to execute this method, b/c getting an item checks TTL anyway.
        /// </summary>
        void EvictExpired();

        /// <summary>
        /// Removes all items from the cache
        /// </summary>
        void Clear();


        /// <summary>
        /// Tries to remove item with the specified key, also returns the object removed in an "out" var
        /// </summary>
        /// <param name="key">The key of the element to remove</param>
        /// <param name="sendBackplaneNotification">Send backplane notification or not</param>
        /// <param name="skipRemoveIfEqualFunc">If function returns true, skip removing the entry</param>
        bool Remove(string key, bool sendBackplaneNotification = true, Func<string?, bool>? skipRemoveIfEqualFunc = null);
    }

    /// <summary>
    /// Simple MemoryCache alternative. Basically a concurrent dictionary with expiration and cache value change notifications.
    /// </summary>
    public class HzMemoryCache : IDisposable, IDetailedHzCache
    {
        private readonly ConcurrentDictionary<string, TTLValue> dictionary = new();
        private readonly HzCacheOptions options;
        private readonly Timer cleanUpTimer;
        private readonly Timer checksumAndNotifyTimer;
        private readonly BlockingCollection<TTLValue> checksumAndNotifyQueue = new();

        /// <summary>
        /// Initializes a new empty instance of <see cref="HzMemoryCache"/>
        /// </summary>
        /// <param name="options">Options for the cache</param>
        public HzMemoryCache(HzCacheOptions? options = null)
        {
            this.options = options ?? new HzCacheOptions();
            cleanUpTimer = new Timer(s => { _ = ProcessExpiredEviction(); }, null, this.options.cleanupJobInterval, this.options.cleanupJobInterval);
            checksumAndNotifyTimer = new Timer(UpdateChecksumAndNotify, null, 20, 20);
        }

        private static readonly SemaphoreSlim globalStaticLock = new(1);

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

        public void RemoveByRegex(Regex re, bool sendNotification = true)
        {
            var victims = dictionary.Keys.Where(k => re.IsMatch(k)).ToList();
            victims.ForEach(key => RemoveItem(key, CacheItemChangeType.Remove, false));
            if (sendNotification)
            {
                NotifyItemChange(re.ToString(), CacheItemChangeType.Remove, null, 0, true);
            }
        }

        private bool RemoveItem(string key, CacheItemChangeType changeType, bool sendNotification, Func<string?, bool>? skipRemoveIfEqualFunc = null)
        {
            if (!dictionary.TryGetValue(key, out TTLValue ttlValue) || (skipRemoveIfEqualFunc != null && skipRemoveIfEqualFunc.Invoke(ttlValue.checksum)))
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
            options.valueChangeListener.Invoke(key, changeType, checksum, timestamp, isRegexp);
        }

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

        public int Count => dictionary.Count;

        public void Clear()
        {
            var kvps = dictionary.ToArray();
            dictionary.Clear();
            foreach (var kv in kvps)
            {
                NotifyItemChange(kv.Key, kv.Value.IsExpired() ? CacheItemChangeType.Expire : CacheItemChangeType.Remove, kv.Value.checksum, kv.Value.timestampCreated);
            }
        }

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

        public void Set<T>(string key, T? value) where T : class
        {
            Set(key, value, options.defaultTTL);
        }

        public void Set<T>(string key, T? value, TimeSpan ttl) where T : class
        {
            var v = new TTLValue(value, ttl, checksumAndNotifyQueue, options.asyncNotifications,
                tv => NotifyItemChange(key, CacheItemChangeType.AddOrUpdate, tv.checksum, tv.timestampCreated));
            dictionary[key] = v;
        }

        public T? GetOrSet<T>(string key, Func<string, T> valueFactory, TimeSpan ttl) where T : class
        {
            var value = Get<T>(key);
            if (value != null)
                return value;

            value = valueFactory(key);
            var ttlValue = new TTLValue(value, ttl, checksumAndNotifyQueue, options.asyncNotifications, tv =>
            {
                NotifyItemChange(key, CacheItemChangeType.AddOrUpdate, tv.checksum, tv.timestampCreated);
            });
            dictionary[key] = ttlValue;
            return value;
        }

        public bool Remove(string key)
        {
            return Remove(key, true);
        }


        public bool Remove(string key, bool sendBackplaneNotification, Func<string?, bool>? skipRemoveIfEqualFunc = null)
        {
            return RemoveItem(key, CacheItemChangeType.Remove, sendBackplaneNotification, skipRemoveIfEqualFunc);
        }

        //IDispisable members
        private bool _disposedValue;
        public void Dispose() => Dispose(true);

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
