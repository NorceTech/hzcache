using System;
using System.Text;
using hzcache;
using StackExchange.Redis;
using Utf8Json;

namespace RedisBackplane
{
    public class RedisBackplanceMemoryMemoryCacheOptions : HzCacheOptions
    {
        public string redisConnectionString { get; set; }
        public string applicationCachePrefix { get; set; }
        public string instanceId { get; set; }
        public bool useRedisAs2ndLevelCache => redisConnectionString != null;
    }

    public class RedisBackplaneHzCache : IDetailedHzCache
    {
        private readonly HzMemoryCache hzCache;
        private readonly string instanceId = Guid.NewGuid().ToString();
        private readonly RedisBackplanceMemoryMemoryCacheOptions options;
        private readonly ConnectionMultiplexer redis;

        public RedisBackplaneHzCache(RedisBackplanceMemoryMemoryCacheOptions options)
        {
            this.options = options;
            if (this.options.useRedisAs2ndLevelCache)
            {
                this.options.notificationType = NotificationType.Async;
            }

            if (!string.IsNullOrWhiteSpace(options.instanceId))
            {
                instanceId = options.instanceId;
            }

            redis = ConnectionMultiplexer.Connect(options.redisConnectionString);
            hzCache = new HzMemoryCache(new HzCacheOptions
            {
                instanceId = instanceId,
                evictionPolicy = options.evictionPolicy,
                notificationType = options.notificationType,
                cleanupJobInterval = options.cleanupJobInterval,
                valueChangeListener = (key, changeType, ttlValue, objectData, isPattern) =>
                {
                    options.valueChangeListener?.Invoke(key, changeType, ttlValue, objectData, isPattern);
                    var redisChannel = new RedisChannel(options.applicationCachePrefix, RedisChannel.PatternMode.Auto);
                    var messageObject = new RedisInvalidationMessage(instanceId, key, ttlValue?.checksum, ttlValue?.timestampCreated, isPattern);
                    redis.GetSubscriber().PublishAsync(redisChannel, new RedisValue(JsonSerializer.ToJsonString(messageObject)));
                    if (changeType == CacheItemChangeType.AddOrUpdate)
                    {
                        if (options.useRedisAs2ndLevelCache && objectData != null)
                        {
                            redis.GetDatabase().StringSet(key, objectData, TimeSpan.FromMilliseconds(ttlValue.absoluteExpireTime - DateTimeOffset.Now.ToUnixTimeMilliseconds()));
                        }
                    }
                    else
                    {
                        if (isPattern.HasValue && isPattern.Value)
                        {
                            RemoveByPattern(key, false);
                            if (options.useRedisAs2ndLevelCache)
                            {
                                redis.GetDatabase().Execute("EVAL", $"for i, name in ipairs(redis.call(\"KEYS\", \"{key}\")) do redis.call(\"UNLINK\", name); end", "0");
                            }
                        }
                        else
                        {
                            Remove(key, false);
                        }

                        if (options.useRedisAs2ndLevelCache)
                        {
                            redis.GetDatabase().KeyDelete(key);
                        }
                    }
                },
                defaultTTL = options.defaultTTL
            });

            if (string.IsNullOrWhiteSpace(options.redisConnectionString))
            {
                throw new ArgumentNullException("Redis connection string is required");
            }

            if (string.IsNullOrWhiteSpace(options.applicationCachePrefix))
            {
                throw new ArgumentNullException("Application cache prefix is required");
            }

            // Messages from other instances through redis. 
            redis.GetSubscriber().Subscribe(options.applicationCachePrefix, (channel, message) =>
            {
                var invalidationMessage = JsonSerializer.Deserialize<RedisInvalidationMessage>(message.ToString());

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

        public bool Remove(string key, bool sendBackplaneNotification = true, Func<string, bool> skipRemoveIfEqualFunc = null)
        {
            return hzCache.Remove(key, sendBackplaneNotification, skipRemoveIfEqualFunc);
        }

        public T Get<T>(string key) where T : class
        {
            var value = hzCache.Get<T>(key);
            if (value == null && options.useRedisAs2ndLevelCache)
            {
                var redisValue = redis.GetDatabase().StringGet(key);
                if (!redisValue.IsNull)
                {
                    var ttlValue = TTLValue.FromRedisValue<T>(Encoding.ASCII.GetBytes(redisValue.ToString()));
                    hzCache.SetRaw<T>(key, ttlValue);
                    return (T)ttlValue.value;
                }
            }

            return value;
        }

        public void Set<T>(string key, T value) where T : class
        {
            hzCache.Set(key, value);
        }

        public void Set<T>(string key, T value, TimeSpan ttl) where T : class
        {
            hzCache.Set(key, value, ttl);
        }

        public T GetOrSet<T>(string key, Func<string, T> valueFactory, TimeSpan ttl) where T : class
        {
            return hzCache.GetOrSet(key, valueFactory, ttl);
        }

        public bool Remove(string key)
        {
            return hzCache.Remove(key);
        }
    }
}
