using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using HzCache.Diagnostics;
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
            using var activity = Activities.Source.StartActivityWithCommonTags(Activities.Names.Set, Activities.Project.HzMemoryCache, async: true, key: key);
            Set(key, value, ttl);
            return Task.CompletedTask;
        }

        public async Task<T?> GetOrSetAsync<T>(string key, Func<string, Task<T>> valueFactory, TimeSpan ttl, long maxMsToWaitForFactory = 10000)
        {
            using var activity = Activities.Source.StartActivityWithCommonTags(Activities.Names.GetOrSet, Activities.Project.HzMemoryCache, async: true, key: key);

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
            using var activity = Activities.Source.StartActivityWithCommonTags(Activities.Names.GetOrSetBatch, Activities.Project.HzMemoryCache, async: true, key: string.Join(",", keys ?? new List<string>()));

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
            using var activity = Activities.Source.StartActivityWithCommonTags(Activities.Names.Clear, Activities.Project.HzMemoryCache, async: true);
            var kvps = dictionary.ToArray();
            dictionary.Clear();
            foreach (var kv in kvps)
            {
                NotifyItemChange("*", CacheItemChangeType.Remove, null, null, true);
            }
        }

        public async Task<bool> RemoveAsync(string key)
        {
            using var activity = Activities.Source.StartActivityWithCommonTags(Activities.Names.Remove, Activities.Project.HzMemoryCache, async: true, key: key);
            return await RemoveAsync(key, options.notificationType != NotificationType.None);
        }

        public async Task RemoveByPatternAsync(string pattern, bool sendNotification = true)
        {
            using var activity = Activities.Source.StartActivityWithCommonTags(Activities.Names.RemoveByPattern, Activities.Project.HzMemoryCache, async: true, pattern: pattern,sendNotification:sendNotification);
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
            using var activity = Activities.Source.StartActivityWithCommonTags(Activities.Names.Get, Activities.Project.HzMemoryCache, async: true, key: key);
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
            using var activity = Activities.Source.StartActivityWithCommonTags(Activities.Names.Remove, Activities.Project.HzMemoryCache, async: true, key: key);
            return await RemoveItemAsync(key, CacheItemChangeType.Remove, sendBackplaneNotification, skipRemoveIfEqualFunc);
        }

        private async Task<bool> RemoveItemAsync(string key, CacheItemChangeType changeType, bool sendNotification, Func<string, bool>? areEqualFunc = null)
        {
            using var activity = Activities.Source.StartActivityWithCommonTags(Activities.Names.RemoveItem, Activities.Project.HzMemoryCache, async: true, key: key);
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
