#nullable enable
using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Threading.Tasks.Dataflow;
using Utf8Json;

namespace hzcache
{
    public class TTLValue
    {
        private readonly Action<TTLValue, byte[]?>? postCompletionCallback;
        private int tickCountWhenToKill;
        private int ttlInMs;
        public object? value;

        private TTLValue()
        {
        }

        public TTLValue(string key, object? value, TimeSpan ttl, IPropagatorBlock<TTLValue, IList<TTLValue>> checksumAndNotifyQueue, NotificationType notificationType,
            Action<TTLValue, byte[]?>? postCompletionCallback)
        {
            timestampCreated = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            this.value = value;
            this.key = key;
            ttlInMs = (int)ttl.TotalMilliseconds;
            this.postCompletionCallback = postCompletionCallback;
            tickCountWhenToKill = Environment.TickCount + ttlInMs;
            absoluteExpireTime = DateTimeOffset.Now.ToUnixTimeMilliseconds() + ttlInMs;
            if (postCompletionCallback != null)
            {
                switch (notificationType)
                {
                    case NotificationType.Async:
                        checksumAndNotifyQueue.SendAsync(this);
                        break;
                    case NotificationType.Sync:
                        UpdateChecksum();
                        break;
                }
            }
        }

        public string key { get; set; }
        public long timestampCreated { get; set; }
        public long absoluteExpireTime { get; set; }
        public string checksum { get; set; }

        public static TTLValue FromRedisValue<T>(byte[] compressedData)
        {
            // var target = new byte[LZ4Codec.MaximumOutputSize(compressedData.Length)];
            // LZ4Codec.Decode(
            // compressedData, 0, compressedData.Length,
            // target, 0, target.Length);
            var redisValue = JsonSerializer.Deserialize<TTLRedisValue>(compressedData);
            return new TTLValue
            {
                checksum = redisValue.checksum,
                key = redisValue.key,
                ttlInMs = redisValue.ttlInMs,
                value = JsonSerializer.Deserialize<T>(redisValue.valueJson),
                timestampCreated = redisValue.timestampCreated,
                tickCountWhenToKill = redisValue.tickCountWhenToKill,
                absoluteExpireTime = redisValue.absoluteExpireTime
            };
        }

        public void UpdateChecksum()
        {
            try
            {
                using var md5 = MD5.Create();
                var valueJson = JsonSerializer.Serialize(value);
                checksum = BitConverter.ToString(md5.ComputeHash(valueJson));
                var redisValue = new TTLRedisValue
                {
                    valueJson = valueJson,
                    key = key,
                    timestampCreated = timestampCreated,
                    absoluteExpireTime = absoluteExpireTime,
                    checksum = checksum,
                    ttlInMs = ttlInMs,
                    tickCountWhenToKill = tickCountWhenToKill
                };
                var json = JsonSerializer.Serialize(redisValue);
                postCompletionCallback?.Invoke(this, json);
            }
            catch (Exception e)
            {
                Console.WriteLine("error " + e);
            }
        }

        public void UpdateTimeToKill()
        {
            tickCountWhenToKill = Environment.TickCount + ttlInMs;
            absoluteExpireTime = DateTimeOffset.Now.ToUnixTimeMilliseconds() + ttlInMs;
        }

        public bool IsExpired()
        {
            return Environment.TickCount > tickCountWhenToKill;
        }
    }

    public class TTLRedisValue
    {
        public byte[] valueJson { get; set; }
        public string key { get; set; }
        public int tickCountWhenToKill { get; set; }
        public int ttlInMs { get; set; }
        public long timestampCreated { get; set; }
        public long absoluteExpireTime { get; set; }
        public string checksum { get; set; }
    }
}
