#nullable enable
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace hzcache
{
    public enum CacheItemChangeType
    {
        AddOrUpdate, Remove, Expire
    }

    public enum EvictionPolicy
    {
        LRU, FIFO
    }

    public enum NotificationType
    {
        Async, Sync, None
    }

    public class HzCacheOptions
    {
        public string instanceId { get; set; } = Guid.NewGuid().ToString();
        public int cleanupJobInterval { get; set; } = 1000;
        public TimeSpan defaultTTL { get; set; } = TimeSpan.FromMinutes(5);
        public Action<string, CacheItemChangeType, TTLValue, byte[]?, bool?> valueChangeListener { get; set; } = (_, _, _, _, _) => { };
        public NotificationType notificationType { get; set; }
        public EvictionPolicy evictionPolicy { get; set; } = EvictionPolicy.LRU;
    }

    public interface IHzCache
    {
        /// <summary>
        ///     Removes cache keys based on a pattern (`*` - asterisk as wildcard).
        /// </summary>
        /// <param name="re"></param>
        /// <param name="sendNotification"></param>
        void RemoveByPattern(string pattern, bool sendNotification = true);

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
        /// <param name="skipRemoveIfEqualFunc">If function returns true, skip removing the entry</param>
        bool Remove(string key, bool sendBackplaneNotification = true, Func<string, bool>? skipRemoveIfEqualFunc = null);
    }

    /// <summary>
    ///     Simple MemoryCache alternative. Basically a concurrent dictionary with expiration and cache value change
    ///     notifications.
    /// </summary>
    public class HzMemoryCache : IEnumerable<KeyValuePair<string, object>>, IDisposable, IDetailedHzCache
    {
        private static readonly IPropagatorBlock<TTLValue, IList<TTLValue>> checksumAndNotifyQueue = CreateBuffer<TTLValue>(TimeSpan.FromMilliseconds(100), 100);

        private static readonly SemaphoreSlim globalStaticLock = new(1);
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
            StartUpdateChecksumAndNotify();
        }

        public int Count => dictionary.Count;

        public void RemoveByPattern(string pattern, bool sendNotification = true)
        {
            var myPattern = pattern;
            if (pattern[0] != '*')
            {
                myPattern = "^" + pattern;
            }

            var re = new Regex(myPattern.Replace("*", ".*"));
            var victims = dictionary.Keys.Where(k => re.IsMatch(k)).ToList();
            victims.ForEach(key =>
            {
                RemoveItem(key, CacheItemChangeType.Remove, false);
            });
            if (sendNotification)
            {
                NotifyItemChange(pattern, CacheItemChangeType.Remove, null, null, true);
            }
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

        public void Clear()
        {
            var kvps = dictionary.ToArray();
            dictionary.Clear();
            foreach (var kv in kvps)
            {
                NotifyItemChange("*", CacheItemChangeType.Remove, null, null, true);
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

            return (T)ttlValue.value;
        }

        public void Set<T>(string key, T? value) where T : class
        {
            Set(key, value, options.defaultTTL);
        }

        public void Set<T>(string key, T? value, TimeSpan ttl) where T : class
        {
            var v = new TTLValue(key, value, ttl, checksumAndNotifyQueue, options.notificationType,
                (tv, objectData) => NotifyItemChange(key, CacheItemChangeType.AddOrUpdate, tv, objectData));
            dictionary[key] = v;
        }

        public T? GetOrSet<T>(string key, Func<string, T> valueFactory, TimeSpan ttl) where T : class
        {
            var value = Get<T>(key);
            if (value != null)
            {
                return value;
            }

            value = valueFactory(key);
            var ttlValue = new TTLValue(key, value, ttl, checksumAndNotifyQueue, options.notificationType, (tv, objectData) =>
            {
                NotifyItemChange(key, CacheItemChangeType.AddOrUpdate, tv, objectData);
            });
            dictionary[key] = ttlValue;
            return value;
        }

        public bool Remove(string key)
        {
            return Remove(key, options.notificationType != NotificationType.None);
        }


        public bool Remove(string key, bool sendBackplaneNotification = true, Func<string, bool>? skipRemoveIfEqualFunc = null)
        {
            return RemoveItem(key, CacheItemChangeType.Remove, sendBackplaneNotification, skipRemoveIfEqualFunc);
        }

        public void Dispose()
        {
            Dispose(true);
        }

        public IEnumerator<KeyValuePair<string, object>> GetEnumerator()
        {
            foreach (var kvp in dictionary)
            {
                if (!kvp.Value.IsExpired())
                {
                    yield return new KeyValuePair<string, object>(kvp.Key, kvp.Value.value);
                }
            }
        }


        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public static IPropagatorBlock<TIn, IList<TIn>> CreateBuffer<TIn>(TimeSpan timeSpan, int count)
        {
            var inBlock = new BufferBlock<TIn>();
            var outBlock = new BufferBlock<IList<TIn>>();

            var outObserver = outBlock.AsObserver();
            inBlock.AsObservable()
                .Buffer(timeSpan, count)
                .ObserveOn(TaskPoolScheduler.Default)
                .Subscribe(outObserver);

            return DataflowBlock.Encapsulate(inBlock, outBlock);
        }

        private void StartUpdateChecksumAndNotify()
        {
            var options = new DataflowLinkOptions {PropagateCompletion = true};
            var actionBlock = new ActionBlock<IList<TTLValue>>(ttlValues =>
            {
                ttlValues.AsParallel().ForAll(ttlValue => ttlValue.UpdateChecksum());
            });

            checksumAndNotifyQueue.LinkTo(actionBlock, options);
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

        private bool RemoveItem(string key, CacheItemChangeType changeType, bool sendNotification, Func<string, bool>? areEqualFunc = null)
        {
            var result = !(!dictionary.TryGetValue(key, out TTLValue ttlValue) || (areEqualFunc != null && areEqualFunc.Invoke(ttlValue.checksum)));

            if (result)
            {
                result = dictionary.TryRemove(key, out ttlValue);
                if (result)
                {
                    result = !ttlValue.IsExpired();
                }
            }

            if (sendNotification)
            {
                NotifyItemChange(key, changeType, ttlValue);
            }

            return result;
        }

        private void NotifyItemChange(string key, CacheItemChangeType changeType, TTLValue ttlValue, byte[]? objectData = null, bool isPattern = false)
        {
            options.valueChangeListener.Invoke(key, changeType, ttlValue, objectData, isPattern);
        }

        public void SetRaw<T>(string key, TTLValue value) where T : class
        {
            dictionary[key] = value;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    cleanUpTimer.Dispose();
                }

                _disposedValue = true;
            }
        }

        public IEnumerator<KeyValuePair<string, T>> GetEnumerator<T>()
        {
            foreach (var kvp in dictionary)
            {
                if (!kvp.Value.IsExpired())
                {
                    yield return new KeyValuePair<string, T>(kvp.Key, (T)kvp.Value.value);
                }
            }
        }
    }
}
