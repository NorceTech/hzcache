using System.Diagnostics;
using HzCache;
using HzCache.Diagnostics;

namespace UnitTests
{
    [TestClass]
    public class DiagnosticsTests
    {
        [TestMethod]
        public void SourceIsASingleInstance()
        {
            Assert.AreSame(HzActivities.Source, HzActivities.Source);
        }

        [TestMethod]
        public async Task CacheOperationsDoNotCreateNewActivitySources()
        {
            // Every ActivitySource constructor registers the instance in the runtime's global list and
            // invokes ShouldListenTo on all listeners, so counting those callbacks after registration
            // detects any per-call construction of the source.
            var created = 0;
            var armed = false;
            using var listener = new ActivityListener
            {
                ShouldListenTo = source =>
                {
                    if (armed && source.Name == HzActivities.HzCacheActivitySourceName)
                        Interlocked.Increment(ref created);
                    return false;
                }
            };
            ActivitySource.AddActivityListener(listener);
            armed = true;

            using var cache = new HzMemoryCache(new HzCacheOptions { cleanupJobInterval = 20, notificationType = NotificationType.Sync });
            for (var i = 0; i < 100; i++)
            {
                cache.GetOrSet($"key{i}", _ => new MockObject(i), TimeSpan.FromMinutes(1));
                cache.Get<MockObject>($"key{i}");
            }
            cache.Remove("key0");
            cache.RemoveByPattern("key1*");
            await Task.Delay(200); // several cleanup-timer ticks

            Assert.AreEqual(0, created);
        }
    }
}
