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
using Microsoft.Extensions.Logging;

namespace hzcache
{
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
            // Console.WriteLine($"[{options.instanceId}] Set {key} with TTL {ttl} and value {value}");
            var v = new TTLValue(key, value, ttl, checksumAndNotifyQueue, options.notificationType,
                (tv, objectData) => NotifyItemChange(key, CacheItemChangeType.AddOrUpdate, tv, objectData));
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
            var ttlValue = new TTLValue(key, value, ttl, checksumAndNotifyQueue, options.notificationType, (tv, objectData) =>
            {
                NotifyItemChange(key, CacheItemChangeType.AddOrUpdate, tv, objectData);
            });
            dictionary[key] = ttlValue;
            return value;
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
        public bool Remove(string key, bool sendBackplaneNotification = true, Func<string, bool>? skipRemoveIfEqualFunc = null)
        {
            return RemoveItem(key, CacheItemChangeType.Remove, sendBackplaneNotification, skipRemoveIfEqualFunc);
        }

        public CacheStatistics GetStatistics()
        {
            return new CacheStatistics {Counts = Count, SizeInBytes = 0};
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
            // Console.WriteLine($"[{options.instanceId}] Delete {key}");
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
            // Console.WriteLine($"Publishing {changeType} for {key} and pattern {isPattern}");
            options.valueChangeListener.Invoke(key, changeType, ttlValue, objectData, isPattern);
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
