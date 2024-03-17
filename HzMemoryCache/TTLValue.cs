#nullable enable
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading.Tasks;

namespace hzcache
{
    public class TTLValue
    {
            public readonly object? value;
            public int tickCountWhenToKill;
            private readonly int ttlInMs;
            public long timestampCreated { get; }
            public string checksum { get; set; }
            private readonly Action<TTLValue>? postCompletionCallback;


            public TTLValue(object? value, TimeSpan ttl, BlockingCollection<TTLValue> checksumAndNotifyQueue, bool computeChecksum, Action<TTLValue>? postCompletionCallback)
            {
                this.timestampCreated = DateTimeOffset.Now.ToUnixTimeMilliseconds();
                this.value = value;
                this.ttlInMs = (int)ttl.TotalMilliseconds;
                this.postCompletionCallback = postCompletionCallback;
                tickCountWhenToKill = Environment.TickCount + this.ttlInMs;
                if (!computeChecksum)
                {
                    checksumAndNotifyQueue.TryAdd(this);
                }
                else
                {
                    postCompletionCallback?.Invoke(this);
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
                    Console.WriteLine("error " + e.ToString());
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
