namespace HzCache
{
    public class RedisInvalidationMessage
    {
        public RedisInvalidationMessage(string applicationCachePrefix, string instanceId, string key, string checksum, long? timestamp, bool? isPattern = false)
        {
            this.key = key;
            this.instanceId = instanceId;
            this.isPattern = isPattern;
            this.checksum = checksum;
            this.timestamp = timestamp;
            this.applicationCachePrefix = applicationCachePrefix;
        }

        public string applicationCachePrefix { get; }
        public string instanceId { get; set; }
        public string key { get; set; }
        public bool? isPattern { get; set; }
        public string checksum { get; set; }
        public long? timestamp { get; set; }
    }
}
