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
            // invokes ShouldListenTo on all listeners, so any source with our name other than the one
            // expected instance is a per-call construction. Touch Source first so the static instance
            // exists before the listener is registered, regardless of test ordering.
            var expected = HzActivities.Source;
            var created = 0;
            using var listener = new ActivityListener
            {
                ShouldListenTo = source =>
                {
                    if (source.Name == HzActivities.HzCacheActivitySourceName && !ReferenceEquals(source, expected))
                        Interlocked.Increment(ref created);
                    return false;
                }
            };
            ActivitySource.AddActivityListener(listener);

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
