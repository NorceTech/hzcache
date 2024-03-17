#nullable enable
using System;
using System.Collections.Concurrent;

namespace hzcache
{
    public class TTLValue
    {
        public readonly object? value;
        private int tickCountWhenToKill;
        private readonly int ttlInMs;
        public long timestampCreated { get; }
        public string? checksum { get; private set; }
        private readonly Action<TTLValue>? postCompletionCallback;


        public TTLValue(object? value, TimeSpan ttl, BlockingCollection<TTLValue> checksumAndNotifyQueue, bool asyncCallback, Action<TTLValue>? postCompletionCallback)
        {
            this.timestampCreated = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            this.value = value;
            this.ttlInMs = (int)ttl.TotalMilliseconds;
            this.postCompletionCallback = postCompletionCallback;
            tickCountWhenToKill = Environment.TickCount + this.ttlInMs;
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

        public void UpdateChecksum()
        {
            try
            {
                using var md5 = System.Security.Cryptography.MD5.Create();
                var json = Utf8Json.JsonSerializer.Serialize(value);
                checksum = BitConverter.ToString(md5.ComputeHash(json));
                this.postCompletionCallback?.Invoke(this);
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
