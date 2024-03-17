using System;

namespace RedisBackplane
{
    public class RedisInvalidationMessage
    {
        public string instanceId { get; set; }
        public string key { get; set; }
        public bool isRegexp { get; set; } = false;
        public string checksum { get; set; }
        public long timestamp { get; set; }
        public RedisInvalidationMessage(string instanceId, string key, string checksum, long timestamp, bool isRegexp = false)
        {
            this.key = key;
            this.instanceId = instanceId;
            this.isRegexp = isRegexp;
            this.checksum = checksum;
            this.timestamp = timestamp;
        }
    }
}
