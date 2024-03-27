#nullable enable
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Security.Cryptography;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Utf8Json;

namespace HzCache
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
        public long sizeInBytes { get; set; }

        public static byte[] Compress(byte[] data)
        {
            using (var compressedStream = new MemoryStream())
            using (var zipStream = new GZipStream(compressedStream, CompressionLevel.Fastest))
            {
                zipStream.Write(data, 0, data.Length);
                zipStream.Close();
                return compressedStream.ToArray();
            }
        }

        public static byte[] Decompress(byte[] data)
        {
            using (var compressedStream = new MemoryStream(data))
            using (var zipStream = new GZipStream(compressedStream, CompressionMode.Decompress))
            using (var resultStream = new MemoryStream())
            {
                zipStream.CopyTo(resultStream);
                return resultStream.ToArray();
            }
        }

        public static TTLValue FromRedisValue<T>(byte[] compressedData)
        {
            var redisValue = JsonSerializer.Deserialize<TTLRedisValue>(compressedData);
            using Stream valueStream = new MemoryStream(false ? Decompress(redisValue.valueJson) : redisValue.valueJson);
            return new TTLValue
            {
                checksum = redisValue.checksum,
                key = redisValue.key,
                ttlInMs = redisValue.ttlInMs,
                value = JsonSerializer.Deserialize<T>(valueStream),
                sizeInBytes = redisValue.valueJson.Length,
                timestampCreated = redisValue.timestampCreated,
                tickCountWhenToKill = redisValue.tickCountWhenToKill,
                absoluteExpireTime = redisValue.absoluteExpireTime
            };
        }

        public static async Task<TTLValue> FromRedisValueAsync<T>(byte[] compressedData)
        {
            using Stream stream = new MemoryStream(compressedData);
            var redisValue = await JsonSerializer.DeserializeAsync<TTLRedisValue>(stream);
            using Stream valueStream = new MemoryStream(false ? Decompress(redisValue.valueJson) : redisValue.valueJson);
            return new TTLValue
            {
                checksum = redisValue.checksum,
                key = redisValue.key,
                ttlInMs = redisValue.ttlInMs,
                value = await JsonSerializer.DeserializeAsync<T>(valueStream),
                sizeInBytes = redisValue.valueJson.Length,
                timestampCreated = redisValue.timestampCreated,
                tickCountWhenToKill = redisValue.tickCountWhenToKill,
                absoluteExpireTime = redisValue.absoluteExpireTime
            };
        }

        public void UpdateChecksum()
        {
            using var md5 = MD5.Create();
            var valueJson = JsonSerializer.Serialize(value);
            checksum = BitConverter.ToString(md5.ComputeHash(valueJson));
            sizeInBytes = valueJson.Length;
            var redisValue = new TTLRedisValue
            {
                valueJson = false ? Compress(valueJson) : valueJson,
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
