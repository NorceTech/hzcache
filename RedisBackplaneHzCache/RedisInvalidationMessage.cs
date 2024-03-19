namespace RedisBackplane
{
    public class RedisInvalidationMessage
    {
        public RedisInvalidationMessage(string instanceId, string key, string checksum, long? timestamp, bool? isPattern)
        {
            this.key = key;
            this.instanceId = instanceId;
            this.isPattern = isPattern;
            this.checksum = checksum;
            this.timestamp = timestamp;
        }

        public string instanceId { get; set; }
        public string key { get; set; }
        public bool? isPattern { get; set; } = false;
        public string checksum { get; set; }
        public long? timestamp { get; set; }
    }
}
