using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reactive.Joins;
using System.Text;
using System.Threading.Tasks;
using HzCache.Diagnostics;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace HzCache
{
    public partial class RedisBackedHzCache
    {
        public async Task RemoveByPatternAsync(string pattern, bool sendNotification = true)
        {
            using var activity = Activities.Source.StartActivityWithCommonTags(Activities.Names.RemoveByPattern, Activities.Area.RedisBackedHzCache,async:true, pattern: pattern,sendNotification:sendNotification);
            await hzCache.RemoveByPatternAsync(pattern, sendNotification);
        }

        public async Task<T> GetAsync<T>(string key)
        {
            using var activity = Activities.Source.StartActivityWithCommonTags(Activities.Names.Get, Activities.Area.RedisBackedHzCache, async: true, key: key);
            var value = await hzCache.GetAsync<T>(key);
            if (value == null && options.useRedisAs2ndLevelCache)
            {
                var stopwatch = Stopwatch.StartNew();
                var redisValue = await GetRedisValueAsync(key);
                options.logger?.LogTrace("Redis get for key {Key} took {Elapsed} ms", key, stopwatch.ElapsedMilliseconds);
                stopwatch.Restart();
                if (!redisValue.IsNull)
                {
                    var ttlValue = await TTLValue.FromRedisValueAsync<T>(Encoding.ASCII.GetBytes(redisValue.ToString()));
                    options.logger?.LogTrace("Deerialize {Key} took {Elapsed} ms", key, stopwatch.ElapsedMilliseconds);
                    stopwatch.Restart();
                    hzCache.SetRaw(key, ttlValue);
                    return (T)ttlValue.value;
                }
            }

            return value;
        }

        private async Task<RedisValue> GetRedisValueAsync(string key)
        {
            using var activity = Activities.Source.StartActivityWithCommonTags(Activities.Names.GetRedis, Activities.Area.RedisBackedHzCache, async: true, key: key);
            return await redisDb.StringGetAsync(GetRedisKey(key));
        }

        public async Task SetAsync<T>(string key, T value)
        {
            using var activity = Activities.Source.StartActivityWithCommonTags(Activities.Names.Set, Activities.Area.RedisBackedHzCache, async: true, key: key);
            await hzCache.SetAsync(key, value);
        }

        public async Task SetAsync<T>(string key, T value, TimeSpan ttl)
        {
            using var activity = Activities.Source.StartActivityWithCommonTags(Activities.Names.Set, Activities.Area.RedisBackedHzCache, async: true, key: key);
            await hzCache.SetAsync(key, value, ttl);
        }

        public async Task<T> GetOrSetAsync<T>(string key, Func<string, Task<T>> valueFactory, TimeSpan ttl, long maxMsToWaitForFactory = 10000)
        {
            using var activity = Activities.Source.StartActivityWithCommonTags(Activities.Names.GetOrSet, Activities.Area.RedisBackedHzCache, async: true, key: key);
            return await hzCache.GetOrSetAsync(key, valueFactory, ttl, maxMsToWaitForFactory);
        }

        public async Task<IList<T>> GetOrSetBatchAsync<T>(IList<string> keys, Func<IList<string>, Task<List<KeyValuePair<string, T>>>> valueFactory)
        {
            return await GetOrSetBatchAsync(keys, valueFactory, options.defaultTTL);
        }

        public async Task<IList<T>> GetOrSetBatchAsync<T>(IList<string> keys, Func<IList<string>, Task<List<KeyValuePair<string, T>>>> valueFactory, TimeSpan ttl)
        {
            using var activity = Activities.Source.StartActivityWithCommonTags(Activities.Names.GetOrSetBatch, Activities.Area.RedisBackedHzCache, async: true, key: string.Join(",",keys??new List<string>()));
            Func<IList<string>, Task<List<KeyValuePair<string, T>>>> redisFactory = async idList =>
            {
                // Create a list of redis keys from the list of cache keys
                var redisKeyList = idList.Select(GetRedisKey).Select(k => new RedisKey(k)).ToArray();

                // Get all values from redis, non-existing values are returned as RedisValue where HasValue == false;
                var redisBatchResult = await RedisBatchResultAsync<T>(redisKeyList);

                // Create a list of key-value pairs from the redis key list and the redis batch result. Values not found will still have HasValue == false
                var redisKeyValueBatchResult = redisKeyList.Select((id, i) => new KeyValuePair<string, RedisValue>(id, redisBatchResult[i])).ToList();

                // Create a list of cache keys for which the value factory should be called
                var idsForFactoryCall = redisKeyValueBatchResult.Where(rb => !rb.Value.HasValue).Select(rb => rb.Key.ToString()).ToList();

                // Call the value factory with the list of cache keys missing in redis and create a Dictionary for lookup.
                var factoryRetrievedValues =
                    (await valueFactory.Invoke(idsForFactoryCall.Select(CacheKeyFromRedisKey).ToList())).ToDictionary(pair => pair.Key, pair => pair.Value);

                // Merge factory-retrieved values with the redis values
                return redisKeyValueBatchResult.Select(kv =>
                {
                    var cacheKey = CacheKeyFromRedisKey(kv.Key);
                    T value;
                    if (kv.Value.HasValue)
                    {
                        var ttlValue = TTLValue.FromRedisValue<T>(Encoding.UTF8.GetBytes(kv.Value));
                        hzCache.SetRaw(cacheKey, ttlValue);
                        value = (T)ttlValue.value;
                    }
                    else if (factoryRetrievedValues.TryGetValue(cacheKey, out var factoryValue))
                    {
                        value = factoryValue;
                    }
                    else
                    {
                        value = default;
                    }

                    return new KeyValuePair<string, T>(cacheKey, value);
                }).ToList();
            };
            return await hzCache.GetOrSetBatchAsync(keys, redisFactory, ttl);
        }

        private Task<RedisValue[]> RedisBatchResultAsync<T>(RedisKey[] redisKeyList)
        {
            using var activity = Activities.Source.StartActivityWithCommonTags(Activities.Names.GetBatchRedis, Activities.Area.Redis, async: true);
            return redisDb.StringGetAsync(redisKeyList);
        }

        public async Task ClearAsync()
        {
            using var activity = Activities.Source.StartActivityWithCommonTags(Activities.Names.Clear, Activities.Area.RedisBackedHzCache, async: true);
            await hzCache.ClearAsync();
        }

        public async Task<bool> RemoveAsync(string key)
        {
            using var activity = Activities.Source.StartActivityWithCommonTags(Activities.Names.Remove, Activities.Area.RedisBackedHzCache, async: true, key: key);
            return await hzCache.RemoveAsync(key);
        }
    }
}
