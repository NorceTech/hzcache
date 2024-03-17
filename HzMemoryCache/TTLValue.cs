#nullable enable
using System;
using System.Collections.Concurrent;
using System.Security.Cryptography;
using Utf8Json;

namespace hzcache
{
    public class TTLValue
    {
        private readonly Action<TTLValue>? postCompletionCallback;
        private readonly int ttlInMs;
        public readonly object? value;
        private int tickCountWhenToKill;


        public TTLValue(object? value, TimeSpan ttl, BlockingCollection<TTLValue> checksumAndNotifyQueue, bool asyncCallback, Action<TTLValue>? postCompletionCallback)
        {
            timestampCreated = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            this.value = value;
            ttlInMs = (int)ttl.TotalMilliseconds;
            this.postCompletionCallback = postCompletionCallback;
            tickCountWhenToKill = Environment.TickCount + ttlInMs;
            if (postCompletionCallback != null)
            {
                if (asyncCallback)
                {
                    checksumAndNotifyQueue.TryAdd(this);
                }
                else
                {
                    UpdateChecksum();
                }
            }
        }

        public long timestampCreated { get; }
        public string? checksum { get; private set; }

        public void UpdateChecksum()
        {
            try
            {
                using var md5 = MD5.Create();
                var json = JsonSerializer.Serialize(value);
                checksum = BitConverter.ToString(md5.ComputeHash(json));
                postCompletionCallback?.Invoke(this);
            }
            catch (Exception e)
            {
                Console.WriteLine("error " + e);
            }
        }

        public void UpdateTimeToKill()
        {
            tickCountWhenToKill = Environment.TickCount + ttlInMs;
        }

        public bool IsExpired()
        {
            return Environment.TickCount > tickCountWhenToKill;
        }
    }
}
