using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace HzCache
{
    public partial class RedisBackedHzCache
    {
        public async Task RemoveByPatternAsync(string pattern, bool sendNotification = true)
        {
            await hzCache.RemoveByPatternAsync(pattern, sendNotification);
        }

        public async Task<T> GetAsync<T>(string key)
        {
            var value = await hzCache.GetAsync<T>(key);
            if (value == null && options.useRedisAs2ndLevelCache)
            {
                var stopwatch = Stopwatch.StartNew();
                var redisValue = await redisDb.StringGetAsync(GetRedisKey(key));
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

        public async Task SetAsync<T>(string key, T value)
        {
            await hzCache.SetAsync(key, value);
        }

        public async Task SetAsync<T>(string key, T value, TimeSpan ttl)
        {
            await hzCache.SetAsync(key, value, ttl);
        }

        public async Task<T> GetOrSetAsync<T>(string key, Func<string, Task<T>> valueFactory, TimeSpan ttl, long maxMsToWaitForFactory = 10000)
        {
            return await hzCache.GetOrSetAsync(key, valueFactory, ttl, maxMsToWaitForFactory);
        }

        public async Task<IList<T>> GetOrSetBatchAsync<T>(IList<string> keys, Func<IList<string>, Task<List<KeyValuePair<string, T>>>> valueFactory)
        {
            return await GetOrSetBatchAsync(keys, valueFactory, options.defaultTTL);
        }

        public async Task<IList<T>> GetOrSetBatchAsync<T>(IList<string> keys, Func<IList<string>, Task<List<KeyValuePair<string, T>>>> valueFactory, TimeSpan ttl)
        {
            Func<IList<string>, Task<List<KeyValuePair<string, T>>>> redisFactory = async idList =>
            {
                // Create a list of redis keys from the list of cache keys
                var redisKeyList = idList.Select(GetRedisKey).Select(k => new RedisKey(k)).ToArray();

                // Get all values from redis, non-existing values are returned as RedisValue where HasValue == false;
                var redisBatchResult = redisDb.StringGet(redisKeyList);

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

        public async Task ClearAsync()
        {
            await hzCache.ClearAsync();
        }

        public async Task<bool> RemoveAsync(string key)
        {
            return await hzCache.RemoveAsync(key);
        }
    }
}
