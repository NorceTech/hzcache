namespace RedisBackplaneMemoryCache
{
    public class RedisInvalidationMessage
    {
        public RedisInvalidationMessage(string applicationCachePrefix, string instanceId, string key, string checksum, long timestamp, bool isRegexp = false)
        {
            this.key = key;
            this.instanceId = instanceId;
            this.isRegexp = isRegexp;
            this.checksum = checksum;
            this.timestamp = timestamp;
            this.applicationCachePrefix = applicationCachePrefix;
        }

        public string applicationCachePrefix { get; }
        public string instanceId { get; }
        public string key { get; }
        public bool isRegexp { get; set; }
        public string checksum { get; }
        public long timestamp { get; set; }
    }
}
