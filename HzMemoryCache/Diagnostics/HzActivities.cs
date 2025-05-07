using System.Collections.Generic;
using System.Diagnostics;

namespace HzCache.Diagnostics
{
    public static class HzActivities
    {
        public const string HzCacheActivitySourceName = "HzMemoryCache";
        public static ActivitySource? Source => new(HzCacheActivitySourceName);

        public static class Names
        {
            public const string SetRedis = "set to redis";
            public const string Get = "get";
            public const string GetRedis = "get from redis";
            public const string GetOrSetCacheMiss = "get or set on cache miss";
            public const string GetOrSetBatch = "get or set batch";
            public const string GetBatchRedis = "get batch from redis";
            public const string Remove = "remove";
            public const string RemoveByPattern = "remove by pattern";
            public const string RemoveRedis = "remove from redis";
            public const string RemoveByPatternRedis = "remove by pattern from redis";
            public const string RemoveItem = "remove item";
            public const string Clear = "clear";
            public const string ExecuteFactory = "execute factory";

            public const string NotifyItemChange = "notify item change";
            public const string ValueChanged = "value changed";
            public const string EvictExpired = "evict expired";
            public const string ProcessExpiredEviction = "process expired eviction";

            public const string AcquireLock = "acquire lock";
            public const string GetSemaphore = "get semaphore";
            public const string ReleaseLock = "release lock";
        }

        public static class Area
        {
            public const string HzMemoryCache = "HzMemoryCache";
            public const string HzCacheMemoryLocker = "HzCacheMemoryLocker";
            public const string RedisBackedHzCache = "RedisBackedHzCache";
            public const string Redis = "RedisCache";
        }

        private static IEnumerable<KeyValuePair<string, object>> GetCommonTags(string? key, string project, bool async, string? pattern, bool? sendNotification)
        {
            var res = new List<KeyValuePair<string, object?>>
            {
                new KeyValuePair<string, object?>(Tags.Names.OperationKey, key),
                new KeyValuePair<string, object?>(Tags.Names.Project, project),
                new KeyValuePair<string, object?>(Tags.Names.Async, async),
                new KeyValuePair<string, object?>(Tags.Names.Pattern, pattern),
                new KeyValuePair<string, object?>(Tags.Names.SendNotification, sendNotification),
            };

            return res;
        }

        public static Activity? StartActivityWithCommonTags(this ActivitySource source, string activityName, string project, bool async = false, string? key = null, string? pattern = null, bool? sendNotification = null)
        {
            if (source.HasListeners() == false || !HzCacheTracesInstrumentationOptions.Instance.IsActive(activityName, project, key))
                return null;

            return source.StartActivity(
                ActivityKind.Internal,
                tags: GetCommonTags(key, project, async, pattern, sendNotification),
                name: activityName
            );
        }
    }
}