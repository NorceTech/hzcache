using HzCache.Diagnostics;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Utf8Json;

namespace HzCache
{
    public class RedisBackedHzCacheOptions : HzCacheOptions
    {
        public string redisConnectionString { get; set; }
        public bool useRedisAs2ndLevelCache { get; set; }
    }

    public partial class RedisBackedHzCache : IDetailedHzCache
    {
        public IDatabase Database => redisDb;

        private readonly HzMemoryCache hzCache;
        private readonly string instanceId = Guid.NewGuid().ToString();
        private readonly RedisBackedHzCacheOptions options;
        private readonly IDatabase redisDb;

        public RedisBackedHzCache(RedisBackedHzCacheOptions options)
        {
            this.options = options;
            if (this.options.redisConnectionString != null)
            {
                this.options.notificationType = NotificationType.Async;
            }

            if (!string.IsNullOrWhiteSpace(options.instanceId))
            {
                instanceId = options.instanceId;
            }

            var redis = ConnectionMultiplexer.Connect(options.redisConnectionString);
            redisDb = redis.GetDatabase();
            hzCache = new HzMemoryCache(new HzCacheOptions
            {
                instanceId = this.options.instanceId,
                evictionPolicy = options.evictionPolicy,
                notificationType = options.notificationType,
                cleanupJobInterval = options.cleanupJobInterval,
                compressionThreshold = options.compressionThreshold,
                valueChangeListener = (key, changeType, ttlValue, objectData, isPattern) =>
                {
                    using var activity = HzActivities.Source.StartActivityWithCommonTags(HzActivities.Names.ValueChanged, HzActivities.Area.RedisBackedHzCache, key: key);
                    options.valueChangeListener?.Invoke(key, changeType, ttlValue, objectData, isPattern);
                    var redisChannel = new RedisChannel(options.applicationCachePrefix, RedisChannel.PatternMode.Auto);
                    var messageObject = new RedisInvalidationMessage(this.options.applicationCachePrefix, instanceId, key, ttlValue?.checksum, ttlValue?.timestampCreated,
                        isPattern);

                    redis.GetSubscriber().Publish(redisChannel, new RedisValue(JsonSerializer.ToJsonString(messageObject)));
                    var redisKey = GetRedisKey(key);
                    if (changeType == CacheItemChangeType.AddOrUpdate)
                    {
                        if (options.useRedisAs2ndLevelCache && objectData != null && ttlValue != null)
                        {
                            try
                            {
                                var stopwatch = Stopwatch.StartNew();
                                RedisSet(redisKey, objectData, ttlValue);
                                options.logger?.LogTrace("Writing value for key {Key} in redis took {Elapsed} ms", key, stopwatch.ElapsedMilliseconds);
                            }
                            catch (Exception e)
                            {
                                this.options.logger?.LogCritical(e, "Failed to set value in redis");
                            }
                        }
                    }
                    else
                    {
                        if (isPattern.HasValue && isPattern.Value)
                        {
                            RemoveByPattern(key, false);
                            if (options.useRedisAs2ndLevelCache)
                            {
                                this.options.logger?.LogTrace("Removing keys by pattern {Pattern} in redis", key);
                                RedisRemoveByPattern(redisKey);
                            }
                        }
                        else
                        {
                            Remove(key, false);
                        }

                        if (options.useRedisAs2ndLevelCache)
                        {
                            this.options.logger?.LogTrace("Removing value for key {Key} in redis", key);
                            RedisRemove(redisKey);
                        }
                    }
                },
                defaultTTL = options.defaultTTL
            });

            if (string.IsNullOrWhiteSpace(options.redisConnectionString))
            {
                throw new ArgumentException("Redis connection string is required");
            }

            if (string.IsNullOrWhiteSpace(options.applicationCachePrefix))
            {
                throw new ArgumentException("Application cache prefix is required");
            }

            if (!string.IsNullOrWhiteSpace(options.instanceId))
            {
                instanceId = options.instanceId;
            }

            // Messages from other instances through redis.
            redis.GetSubscriber().Subscribe(options.applicationCachePrefix, (_, message) =>
            {
                using var activity = HzActivities.Source.StartActivityWithCommonTags(HzActivities.Names.Subscribe, HzActivities.Area.RedisBackedHzCache);
                var invalidationMessage = JsonSerializer.Deserialize<RedisInvalidationMessage>(message.ToString());
                if (invalidationMessage.applicationCachePrefix != options.applicationCachePrefix)
                {
                    return;
                }

                if (invalidationMessage.instanceId != instanceId)
                {
                    if (invalidationMessage.isPattern.HasValue && invalidationMessage.isPattern.Value)
                    {
                        hzCache.RemoveByPattern(invalidationMessage.key, false);
                    }
                    else
                    {
                        hzCache.Remove(invalidationMessage.key, false, chksum => chksum == invalidationMessage.checksum);
                    }
                }
            });
        }

        private void RedisRemove(string redisKey)
        {
            using var activity = HzActivities.Source.StartActivityWithCommonTags(HzActivities.Names.RemoveRedis, HzActivities.Area.Redis, key: redisKey);
            redisDb.KeyDelete(redisKey);
        }

        private void RedisRemoveByPattern(string redisKey)
        {
            using var activity = HzActivities.Source.StartActivityWithCommonTags(HzActivities.Names.RemoveByPatternRedis, HzActivities.Area.Redis, key: redisKey);
            redisDb.Execute("EVAL", $"for i, name in ipairs(redis.call(\"KEYS\", \"{redisKey}\")) do redis.call(\"UNLINK\", name); end", "0");
        }

        private void RedisSet(string redisKey, byte[] objectData, TTLValue ttlValue)
        {
            using var activity = HzActivities.Source.StartActivityWithCommonTags(HzActivities.Names.SetRedis, HzActivities.Area.Redis, key:redisKey);
            redisDb.StringSet(redisKey, objectData,
                TimeSpan.FromMilliseconds(ttlValue.absoluteExpireTime - DateTimeOffset.Now.ToUnixTimeMilliseconds()));
        }

        public void RemoveByPattern(string pattern, bool sendNotification = true)
        {
            hzCache.RemoveByPattern(pattern, sendNotification);
        }

        public void EvictExpired()
        {
            hzCache.EvictExpired();
        }

        public void Clear()
        {
            hzCache.Clear();
        }

        public bool Remove(string key, bool sendBackplaneNotification, Func<string, bool> skipRemoveIfEqualFunc = null)
        {
            return hzCache.Remove(key, sendBackplaneNotification, skipRemoveIfEqualFunc);
        }

        public CacheStatistics GetStatistics()
        {
            return hzCache.GetStatistics();
        }

        public T Get<T>(string key)
        {
            using var activity = HzActivities.Source.StartActivityWithCommonTags(HzActivities.Names.Get, HzActivities.Area.RedisBackedHzCache, key: key); 
            var value = hzCache.Get<T>(key);
            if (value == null && options.useRedisAs2ndLevelCache)
            {
                var stopwatch = Stopwatch.StartNew();
                var redisValue = GetRedisValue(key);
                options.logger?.LogTrace("Reading value for key {Key} in redis took {Elapsed} ms", key, stopwatch.ElapsedMilliseconds);
                if (!redisValue.IsNull)
                {
                    var ttlValue = TTLValue.FromRedisValue<T>(Encoding.ASCII.GetBytes(redisValue.ToString()));
                    hzCache.SetRaw(key, ttlValue);
                    return (T)ttlValue.value;
                }
            }

            return value;
        }

        private RedisValue GetRedisValue(string key)
        {
            using var activity = HzActivities.Source.StartActivityWithCommonTags(HzActivities.Names.GetRedis, HzActivities.Area.Redis, key: key);
            return redisDb.StringGet(GetRedisKey(key));
        }

        public void Set<T>(string key, T value)
        {
            hzCache.Set(key, value);
        }

        public void Set<T>(string key, T value, TimeSpan ttl)
        {
            hzCache.Set(key, value, ttl);
        }

        public T GetOrSet<T>(string key, Func<string, T> valueFactory, TimeSpan ttl, long maxMsToWaitForFactory = 10000)
        {
            return hzCache.GetOrSet(key, valueFactory, ttl, maxMsToWaitForFactory);
        }

        public IList<T> GetOrSetBatch<T>(IList<string> keys, Func<IList<string>, List<KeyValuePair<string, T>>> valueFactory)
        {
            return GetOrSetBatch(keys, valueFactory, options.defaultTTL);
        }

        public IList<T> GetOrSetBatch<T>(IList<string> keys, Func<IList<string>, List<KeyValuePair<string, T>>> valueFactory, TimeSpan ttl)
        {
            using var activity = HzActivities.Source.StartActivityWithCommonTags(HzActivities.Names.GetOrSetBatch, HzActivities.Area.RedisBackedHzCache, key: string.Join(",",keys??new List<string>()));
            Func<IList<string>, List<KeyValuePair<string, T>>> redisFactory = idList =>
            {
                // Create a list of redis keys from the list of cache keys
                var redisKeyList = idList.Select(GetRedisKey).Select(k => new RedisKey(k)).ToArray();

                // Get all values from redis, non-existing values are returned as RedisValue where HasValue == false;
                var redisBatchResult = RedisBatchResult<T>(redisKeyList);

                // Create a list of key-value pairs from the redis key list and the redis batch result. Values not found will still have HasValue == false
                var redisKeyValueBatchResult = redisKeyList.Select((id, i) => new KeyValuePair<string, RedisValue>(id, redisBatchResult[i])).ToList();

                // Create a list of cache keys for which the value factory should be called
                var idsForFactoryCall = redisKeyValueBatchResult.Where(rb => !rb.Value.HasValue).Select(rb => rb.Key.ToString()).ToList();

                // Call the value factory with the list of cache keys missing in redis and create a Dictionary for lookup.
                var factoryRetrievedValues = valueFactory.Invoke(idsForFactoryCall.Select(CacheKeyFromRedisKey).ToList()).ToDictionary(pair => pair.Key, pair => pair.Value);

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
            return hzCache.GetOrSetBatch(keys, redisFactory, ttl);
        }

        private RedisValue[] RedisBatchResult<T>(RedisKey[] redisKeyList)
        {
            using var activity = HzActivities.Source.StartActivityWithCommonTags(HzActivities.Names.GetBatchRedis, HzActivities.Area.Redis);

            return redisDb.StringGet(redisKeyList);
        }

        public bool Remove(string key)
        {
            return hzCache.Remove(key);
        }

        private string GetRedisKey(string cacheKey)
        {
            return $"{options.applicationCachePrefix}:{cacheKey}";
        }

        private string CacheKeyFromRedisKey(string redisKey)
        {
            return redisKey.Substring(options.applicationCachePrefix.Length + 1);
        }
    }
}
