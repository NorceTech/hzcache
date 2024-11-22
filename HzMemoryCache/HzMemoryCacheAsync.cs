using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace HzCache
{
    public partial class HzMemoryCache
    {
        public Task SetAsync<T>(string key, T? value)
        {
            return SetAsync(key, value, options.defaultTTL);
        }

        public Task SetAsync<T>(string key, T? value, TimeSpan ttl)
        {
            Set(key, value, ttl);
            return Task.CompletedTask;
        }

        public async Task<T?> GetOrSetAsync<T>(string key, Func<string, Task<T>> valueFactory, TimeSpan ttl, long maxMsToWaitForFactory = 10000)
        {
            var value = Get<T>(key);
            if (!IsNullOrDefault(value))
            {
                return value;
            }

            options.logger?.LogDebug("Cache miss for key {Key}, calling value factory", key);

            var factoryLock = await memoryLocker.AcquireLockAsync(options.applicationCachePrefix, options.instanceId, "GET", key, TimeSpan.FromMilliseconds(maxMsToWaitForFactory),
                options.logger, CancellationToken.None);
            if (factoryLock is null)
            {
                options.logger?.LogDebug("Could not acquire lock for key {Key}, returning default value", key);
                throw new Exception($"Could not acquire lock for key {key}");
            }

            try
            {
                if ((value = Get<T>(key)) is not null)
                {
                    return value;
                }
                value = await valueFactory(key);
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


        public async Task<IList<T>> GetOrSetBatchAsync<T>(IList<string> keys, Func<IList<string>, Task<List<KeyValuePair<string, T>>>> valueFactory)
        {
            return await GetOrSetBatchAsync(keys, valueFactory, options.defaultTTL);
        }

        public async Task<IList<T>> GetOrSetBatchAsync<T>(IList<string> keys, Func<IList<string>, Task<List<KeyValuePair<string, T>>>> valueFactory, TimeSpan ttl)
        {
            var cachedItems = keys.Select(key => new KeyValuePair<string, T?>(key, Get<T>(key)));
            var missingKeys = cachedItems.Where(kvp => IsNullOrDefault(kvp.Value)).Select(kvp => kvp.Key).ToList();
            var factoryRetrievedItems = (await valueFactory(missingKeys)).ToDictionary(kv => kv.Key, kv => kv.Value);

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

        public async Task ClearAsync()
        {
            var kvps = dictionary.ToArray();
            dictionary.Clear();
            foreach (var kv in kvps)
            {
                NotifyItemChange("*", CacheItemChangeType.Remove, null, null, true);
            }
        }

        public async Task<bool> RemoveAsync(string key)
        {
            return await RemoveAsync(key, options.notificationType != NotificationType.None);
        }

        public async Task RemoveByPatternAsync(string pattern, bool sendNotification = true)
        {
            var myPattern = pattern;
            if (pattern[0] != '*')
            {
                myPattern = "^" + pattern;
            }

            var re = new Regex(myPattern.Replace("*", ".*"));
            var victims = dictionary.Keys.Where(k => re.IsMatch(k)).ToList();
            victims.AsParallel().ForAll(async key =>
            {
                await RemoveItemAsync(key, CacheItemChangeType.Remove, false);
            });
            if (sendNotification)
            {
                NotifyItemChange(pattern, CacheItemChangeType.Remove, null, null, true);
            }
        }

        public async Task<T> GetAsync<T>(string key)
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

        public async Task<bool> RemoveAsync(string key, bool sendBackplaneNotification = true, Func<string, bool>? skipRemoveIfEqualFunc = null)
        {
            return await RemoveItemAsync(key, CacheItemChangeType.Remove, sendBackplaneNotification, skipRemoveIfEqualFunc);
        }

        private async Task<bool> RemoveItemAsync(string key, CacheItemChangeType changeType, bool sendNotification, Func<string, bool>? areEqualFunc = null)
        {
            var result = !(!dictionary.TryGetValue(key, out var ttlValue) || (areEqualFunc != null && areEqualFunc.Invoke(ttlValue.checksum)));

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
    }
}
