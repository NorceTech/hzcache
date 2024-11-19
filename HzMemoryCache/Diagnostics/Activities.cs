using System.Collections.Generic;
using System.Diagnostics;

namespace HzCache.Diagnostics
{
    public static class Activities
    {
        public static ActivitySource? Source => new(HzCacheDiagnostics.ActivitySourceName, HzCacheDiagnostics.HzCacheVersion);

        public static class Names
        {
            public const string Set = "set from cache";
            public const string Get = "get from cache";
            public const string GetOrSet = "get or set from cache";
            public const string GetOrSetBatch = "get or set batch from cache";
            public const string Remove = "remove";
            public const string RemoveByPattern = "remove by pattern";
            public const string RemoveItem = "remove item";
            public const string Clear = "clear";

            public const string NotifyItemChange = "notify item change";
            public const string Subscribe = "subscribe";
            public const string ValueChanged = "value changed";
            public const string EvictExpired = "evict expired";
            public const string GetStatistics = "get statistics";
        }

        public static class Project
        {
            public const string HzMemoryCache = "HzMemoryCache";
            public const string RedisBackedHzCache = "RedisBackedHzCache";
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
            if (source.HasListeners() == false || !HzCacheTracesInstrumentationOptions.Instance.IsActive(activityName))
                return null;

            return source.StartActivity(
                ActivityKind.Internal,
                tags: GetCommonTags(key, project, async, pattern, sendNotification),
                name: activityName
            );
        }
    }
}