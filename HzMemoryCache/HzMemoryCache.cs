#nullable enable
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Joins;
using System.Reactive.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using HzCache.Diagnostics;
using Microsoft.Extensions.Logging;

namespace HzCache
{
    /// <summary>
    ///     Simple MemoryCache alternative. Basically a concurrent dictionary with expiration and cache value change
    ///     notifications.
    /// </summary>
    public partial class HzMemoryCache : IEnumerable<KeyValuePair<string, object>>, IDisposable, IDetailedHzCache
    {
        private static readonly IPropagatorBlock<TTLValue, IList<TTLValue>> updateChecksumAndSerializeQueue = CreateBuffer<TTLValue>(TimeSpan.FromMilliseconds(35), 100);
        private static readonly SemaphoreSlim globalStaticLock = new(1);
        private readonly Timer cleanUpTimer;
        private readonly ConcurrentDictionary<string, TTLValue> dictionary = new();
        private readonly HzCacheMemoryLocker memoryLocker = new(new HzCacheMemoryLockerOptions());
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
        public long SizeInBytes => dictionary.Values.Sum(ttlv => ttlv.sizeInBytes);


        public void RemoveByPattern(string pattern, bool sendNotification = true)
        {
            using var activity = Activities.Source.StartActivityWithCommonTags(Activities.Names.RemoveByPattern, Activities.Project.HzMemoryCache, pattern: pattern, sendNotification: sendNotification);

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
            var v = new TTLValue(key, value, ttl, updateChecksumAndSerializeQueue, options.notificationType,
                (tv, objectData) => NotifyItemChange(key, CacheItemChangeType.AddOrUpdate, tv, objectData));
            dictionary[key] = v;
        }

        /// <summary>
        ///     @see <see cref="IHzCache" />
        /// </summary>
        public T? GetOrSet<T>(string key, Func<string, T> valueFactory, TimeSpan ttl, long maxMsToWaitForFactory = 10000)
        {
            using var activity = Activities.Source.StartActivityWithCommonTags(Activities.Names.GetOrSet, Activities.Project.HzMemoryCache, key: key);

            var value = Get<T>(key);
            if (!IsNullOrDefault(value))
            {
                return value;
            }

            options.logger?.LogDebug("Cache miss for key {Key}, calling value factory", key);

            var factoryLock = memoryLocker.AcquireLock(options.applicationCachePrefix, options.instanceId, "GET", key, TimeSpan.FromMilliseconds(maxMsToWaitForFactory),
                options.logger, CancellationToken.None);
            if (factoryLock is null)
            {
                options.logger?.LogDebug("Could not acquire lock for key {Key}, returning default value", key);
                throw new Exception($"Could not acquire lock for key {key}");
            }

            try
            {
                value = valueFactory(key);
                var ttlValue = new TTLValue(key, value, ttl, updateChecksumAndSerializeQueue, options.notificationType, (tv, objectData) =>
                {
                    NotifyItemChange(key, CacheItemChangeType.AddOrUpdate, tv, objectData);
                });
                dictionary[key] = ttlValue;
            }
            finally
            {
                ReleaseLock(factoryLock, "GET", key);
            }

            return value;
        }

        public IList<T> GetOrSetBatch<T>(IList<string> keys, Func<IList<string>, List<KeyValuePair<string, T>>> valueFactory)
        {
            return GetOrSetBatch(keys, valueFactory, options.defaultTTL);
        }

        public IList<T> GetOrSetBatch<T>(IList<string> keys, Func<IList<string>, List<KeyValuePair<string, T>>> valueFactory, TimeSpan ttl)
        {
            var cachedItems = keys.Select(key => new KeyValuePair<string, T?>(key, Get<T>(key)));
            var missingKeys = cachedItems.Where(kvp => IsNullOrDefault(kvp.Value)).Select(kvp => kvp.Key).ToList();
            var factoryRetrievedItems = valueFactory(missingKeys).ToDictionary(kv => kv.Key, kv => kv.Value);

            return cachedItems.Select(kv =>
            {
                T? value = default;
                if (kv.Value != null)
                {
                    value = kv.Value;
                }

                if (kv.Value == null)
                {
                    factoryRetrievedItems.TryGetValue(kv.Key, out value);
                    Set(kv.Key, value, ttl);
                }

                return new KeyValuePair<string, T?>(kv.Key, value);
            }).Where(v => v.Value is not null).Select(kv => kv.Value).ToList();
        }


        /// <summary>
        ///     @see <see cref="IHzCache" />
        /// </summary>
        public bool Remove(string key)
        {
            return Remove(key, options.notificationType != NotificationType.None);
        }


        /// <summary>
        ///     @see <see cref="IDetailedHzCache" />
        /// </summary>
        public bool Remove(string key, bool sendBackplaneNotification, Func<string, bool>? skipRemoveIfEqualFunc = null)
        {
            return RemoveItem(key, CacheItemChangeType.Remove, sendBackplaneNotification, skipRemoveIfEqualFunc);
        }

        public CacheStatistics GetStatistics()
        {
            return new CacheStatistics {Counts = Count, SizeInBytes = SizeInBytes};
        }

        /// <summary>
        ///     Dispose.
        /// </summary>
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

        private object? AcquireLock(string operation, string key)
        {
            return memoryLocker.AcquireLock(options.applicationCachePrefix, options.instanceId, operation, key, TimeSpan.FromSeconds(10), options.logger, CancellationToken.None);
        }

        private void ReleaseLock(object? memoryLock, string? operation, string key)
        {
            memoryLocker.ReleaseLock(options.applicationCachePrefix, options.instanceId, operation, key, memoryLock, options.logger);
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
                try
                {
                    ttlValues.AsParallel().ForAll(ttlValue => ttlValue.UpdateChecksum());
                }
                catch (Exception e)
                {
                    this.options.logger?.LogCritical(e, "Unable to calculate checksum and serialize TTLValue");
                }
            });

            updateChecksumAndSerializeQueue.LinkTo(actionBlock, options);
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
            using var activity = Activities.Source.StartActivityWithCommonTags(Activities.Names.NotifyItemChange, Activities.Project.HzMemoryCache, key: key);

            options.valueChangeListener(key, changeType, ttlValue, objectData, isPattern);
        }

        public void SetRaw(string key, TTLValue value)
        {
            dictionary[key] = value;
        }

        private void Dispose(bool disposing)
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

        public static bool IsNullOrDefault<T>(T argument)
        {
            // deal with normal scenarios
            if (argument == null)
            {
                return true;
            }

            if (Equals(argument, default(T)))
            {
                return true;
            }

            // deal with non-null nullables
            var methodType = typeof(T);
            if (Nullable.GetUnderlyingType(methodType) != null)
            {
                return false;
            }

            // deal with boxed value types
            var argumentType = argument.GetType();
            if (argumentType.IsValueType && argumentType != methodType)
            {
                var obj = Activator.CreateInstance(argument.GetType());
                return obj.Equals(argument);
            }

            return false;
        }
    }
}
