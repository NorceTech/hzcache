using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Distributed;
using StackExchange.Redis;
using Utf8Json;

namespace RedisIDistributedCache
{
    public class RedisIDistributedCacheOptions
    {
        public string redisConnectionString { get; set; }
    }

    public class RedisIDistributedCache : IDistributedCache
    {
        public IDatabase Database => redis;

        private readonly RedisIDistributedCacheOptions options;
        private readonly IDatabase redis;

        public RedisIDistributedCache(RedisIDistributedCacheOptions options)
        {
            this.options = options;
            redis = ConnectionMultiplexer.Connect(this.options.redisConnectionString).GetDatabase();
        }

        public byte[] Get(string key)
        {
            var redisValue = redis.StringGet(key);
            if (!redisValue.HasValue)
            {
                return null;
            }

            var ttlValue = JsonSerializer.Deserialize<TTLValue>(redisValue.ToString());
            return ttlValue.value;
        }

        public async Task<byte[]> GetAsync(string key, CancellationToken token = new())
        {
            var redisValue = await redis.StringGetAsync(key);
            if (!redisValue.HasValue)
            {
                return null;
            }

            var ttlValue = JsonSerializer.Deserialize<TTLValue>(redisValue.ToString());
            return ttlValue.value;
        }

        public void Set(string key, byte[] value, DistributedCacheEntryOptions cacheOptions)
        {
            var ttlValue = new TTLValue {value = value, slidingExpiration = cacheOptions.SlidingExpiration?.TotalMilliseconds ?? 0};
            redis.StringSet(key, JsonSerializer.Serialize(ttlValue), cacheOptions.AbsoluteExpirationRelativeToNow);
        }

        public async Task SetAsync(string key, byte[] value, DistributedCacheEntryOptions cacheOptions, CancellationToken token = new())
        {
            var ttlValue = new TTLValue {value = value, slidingExpiration = cacheOptions.SlidingExpiration?.TotalMilliseconds ?? 0};
            await redis.StringSetAsync(key, JsonSerializer.Serialize(ttlValue), cacheOptions.AbsoluteExpirationRelativeToNow);
        }

        public void Refresh(string key)
        {
            var redisValue = redis.StringGet(key);
            if (!redisValue.HasValue)
            {
                return;
            }

            var ttlValue = JsonSerializer.Deserialize<TTLValue>(redisValue.ToString());
            Set(key, ttlValue.value, new DistributedCacheEntryOptions {SlidingExpiration = TimeSpan.FromMilliseconds(ttlValue.slidingExpiration)});
        }

        public async Task RefreshAsync(string key, CancellationToken token = new())
        {
            var redisValue = await redis.StringGetAsync(key);
            if (!redisValue.HasValue)
            {
                return;
            }

            var ttlValue = JsonSerializer.Deserialize<TTLValue>(redisValue.ToString());
            await SetAsync(key, ttlValue.value, new DistributedCacheEntryOptions {SlidingExpiration = TimeSpan.FromMilliseconds(ttlValue.slidingExpiration)}, token);
        }

        public void Remove(string key)
        {
            redis.KeyDelete(key);
        }

        public async Task RemoveAsync(string key, CancellationToken token = new())
        {
            await redis.KeyDeleteAsync(key);
        }
    }
}
