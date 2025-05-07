using System.Diagnostics;
using HzCache;

namespace UnitTests
{
    [TestClass]
    public class UnitTestsAsync
    {
        [TestMethod]
        public async Task TestValueChangeNotificationAsync()
        {
            var addOrUpdates = 0;
            var removals = 0;
            var cache = new HzMemoryCache(
                new HzCacheOptions
                {
                    cleanupJobInterval = 50,
                    notificationType = NotificationType.Sync,
                    valueChangeListener = (_, changeType, _, _, _) =>
                    {
                        switch (changeType)
                        {
                            case CacheItemChangeType.AddOrUpdate:
                                addOrUpdates++;
                                break;
                            case CacheItemChangeType.Expire:
                                // Expire should no longer send a notification
                                break;
                            case CacheItemChangeType.Remove:
                                removals++;
                                break;
                        }
                    }
                });
            await cache.SetAsync("mock2", new MockObject(1));
            Assert.AreEqual(1, addOrUpdates);
            await cache.SetAsync("mock2", new MockObject(2));
            Assert.AreEqual(2, addOrUpdates);
            await cache.RemoveAsync("mock2");
            Assert.AreEqual(1, removals);
            await cache.RemoveAsync("mock2");
            Assert.AreEqual(2, removals);
            await cache.GetOrSetAsync("m", _ => Task.FromResult(new MockObject(1)), TimeSpan.FromMilliseconds(100));
            Assert.AreEqual(3, addOrUpdates);
        }

        [TestMethod]
        public async Task TestRemoveByPatternAsync()
        {
            var removals = 0;
            var cache = new HzMemoryCache(
                new HzCacheOptions
                {
                    cleanupJobInterval = 200,
                    valueChangeListener = async (key, changeType, _, _, _) =>
                    {
                        switch (changeType)
                        {
                            case CacheItemChangeType.Remove:
                                Console.WriteLine("Removed " + key);
                                removals++;
                                break;
                        }
                    }
                });
            await cache.SetAsync("pelle", new MockObject(42));
            await cache.SetAsync("olle", new MockObject(42));
            await cache.SetAsync("kalle", new MockObject(42));
            await cache.SetAsync("stina", new MockObject(42));
            await cache.SetAsync("lina", new MockObject(42));
            await cache.SetAsync("nina", new MockObject(42));
            await cache.SetAsync("tom", new MockObject(42));
            await cache.SetAsync("tomma", new MockObject(42));
            await cache.SetAsync("flina", new MockObject(42));

            await cache.RemoveByPatternAsync("tom*");
            Assert.IsNull(cache.Get<MockObject>("tom"));
            Assert.IsNull(cache.Get<MockObject>("tomma"));

            await cache.RemoveByPatternAsync("*lle*");
            Assert.IsNull(cache.Get<MockObject>("pelle"));
            Assert.IsNull(cache.Get<MockObject>("kalle"));
            Assert.IsNull(cache.Get<MockObject>("olle"));
            Assert.IsNotNull(cache.Get<MockObject>("stina"));
            Assert.IsNotNull(cache.Get<MockObject>("lina"));
            Assert.IsNotNull(cache.Get<MockObject>("nina"));
            await Task.Delay(100);
            Assert.AreEqual(2, removals); // Regex removals are not object by object.
            Assert.AreEqual(4, cache.Count);
        }

        [TestMethod]
        public async Task TestGetSetCleanupAsync()
        {
            var cache = new HzMemoryCache(new HzCacheOptions {cleanupJobInterval = 200});
            await cache.SetAsync("42", new MockObject(42), TimeSpan.FromMilliseconds(100));
            var v = await cache.GetAsync<MockObject>("42");
            Assert.IsTrue(v != null);
            Assert.IsTrue(v.num == 42);

            await Task.Delay(300);
            Assert.IsTrue(cache.Count == 0); //cleanup job has run?
        }

        [TestMethod]
        public async Task TestEvictionAsync()
        {
            var list = new List<HzMemoryCache>();
            for (var i = 0; i < 20; i++)
            {
                var cache = new HzMemoryCache(new HzCacheOptions {cleanupJobInterval = 200});
                await cache.SetAsync("42", new MockObject(42), TimeSpan.FromMilliseconds(100));
                list.Add(cache);
            }

            await Task.Delay(300);

            for (var i = 0; i < 20; i++)
            {
                Assert.IsTrue(list[i].Count == 0); //cleanup job has run?
            }

            //cleanup
            for (var i = 0; i < 20; i++)
            {
                list[i].Dispose();
            }
        }

        [TestMethod]
        public async Task ShortdelayAsync()
        {
            var cache = new HzMemoryCache();
            await cache.SetAsync("42", new MockObject(42), TimeSpan.FromMilliseconds(500));

            await Task.Delay(20);

            var result = await cache.GetAsync<MockObject>("42");
            Assert.IsNotNull(result); //not evicted
            Assert.IsTrue(result.num == 42);
        }

        [TestMethod]
        public async Task TestPrimitivesAsync()
        {
            var cache = new HzMemoryCache();
            await cache.GetOrSetAsync("42", v => Task.FromResult(42), TimeSpan.FromMilliseconds(500));

            await Task.Delay(20);

            var result = await cache.GetAsync<int>("42");
            Assert.IsNotNull(result); //not evicted
            Assert.IsTrue(result == 42);
        }

        [TestMethod]
        public async Task TestWithDefaultJobIntervalAsync()
        {
            var cache2 = new HzMemoryCache();
            await cache2.SetAsync("42", new MockObject(42), TimeSpan.FromMilliseconds(100));
            Assert.IsNotNull(cache2.Get<MockObject>("42"));
            await Task.Delay(150);
            Assert.IsNull(await cache2.GetAsync<MockObject>("42"));
        }

        [TestMethod]
        public async Task TestRemoveAsync()
        {
            var cache = new HzMemoryCache();
            await cache.SetAsync("42", new MockObject(42), TimeSpan.FromMilliseconds(100));
            await cache.RemoveAsync("42");
            Assert.IsNull(cache.Get<MockObject>("42"));
        }

        [TestMethod]
        public async Task TestTryRemoveAsync()
        {
            var cache = new HzMemoryCache();
            cache.Set("42", new MockObject(42), TimeSpan.FromMilliseconds(100));
            var res = await cache.RemoveAsync("42");
            Assert.IsTrue(res);
            Assert.IsNull(await cache.GetAsync<MockObject>("42"));

            //now try remove non-existing item
            Assert.IsFalse(await cache.RemoveAsync("blabblah"));
        }

        [TestMethod]
        public async Task TestTryRemoveWithTtlAsync()
        {
            var cache = new HzMemoryCache();
            await cache.SetAsync("42", new MockObject(42), TimeSpan.FromMilliseconds(100));
            await Task.Delay(120); //let the item expire

            var res = await cache.RemoveAsync("42");
            Assert.IsFalse(res);
        }

        [TestMethod]
        public async Task TestClear()
        {
            var cache = new HzMemoryCache();
            await cache.GetOrSetAsync("key", _ => Task.FromResult(new MockObject(1024)), TimeSpan.FromSeconds(100));

            await cache.ClearAsync();

            Assert.IsNull(cache.Get<MockObject>("key"));
        }

        [TestMethod]
        public async Task TestStampedeProtectionAsync()
        {
            var cache = new HzMemoryCache();
            var stopwatch = Stopwatch.StartNew();
            Task.Run(async () =>
            {
                await cache.GetOrSetAsync("key", _ =>
                {
                    Task.Delay(2000).GetAwaiter().GetResult();
                    return Task.FromResult(new MockObject(1024));
                }, TimeSpan.FromSeconds(100));
            });
            await Task.Delay(50);
            var timeToInsert = stopwatch.ElapsedTicks / Stopwatch.Frequency * 1000;
            stopwatch.Restart();
            var x = await cache.GetOrSetAsync("key", _ => Task.FromResult(new MockObject(1024)), TimeSpan.FromSeconds(100));
            var timeToWait = stopwatch.ElapsedTicks / Stopwatch.Frequency * 1000;
            stopwatch.Restart();
            await cache.GetOrSetAsync("key", _ => Task.FromResult(new MockObject(1024)), TimeSpan.FromSeconds(100));

            var timeToGetFromMemoryCache = stopwatch.ElapsedTicks / Stopwatch.Frequency * 1000;

            Console.WriteLine($"Insert: {timeToInsert}, Wait: {timeToWait}, Get: {timeToGetFromMemoryCache}");

            Assert.IsTrue(timeToInsert < 50);
            Assert.IsTrue(timeToWait is > 950 and < 1050);
            Assert.IsTrue(timeToGetFromMemoryCache < 50);
        }

        [TestMethod]
        public async Task TestStampedeProtectionTimeoutAsync()
        {
            var cache = new HzMemoryCache();
            var stopwatch = Stopwatch.StartNew();
            Task.Run(async () =>
            {
                await cache.GetOrSetAsync("key",async _ =>
                {
                    await Task.Delay(2000);
                    return new MockObject(1024);
                }, TimeSpan.FromSeconds(100), 1000);
            });
            await Task.Delay(50);
            var timeToInsert = stopwatch.ElapsedTicks / Stopwatch.Frequency * 1000;
            stopwatch.Restart();
            // The above "factory" takes 2000 ms to complete, this call should hang for 2000 ms and then return 1024.
            var x = await cache.GetOrSetAsync("key", async _ => new MockObject(2048), TimeSpan.FromSeconds(100));
            var timeToWait = stopwatch.ElapsedTicks / Stopwatch.Frequency * 1000;
            stopwatch.Restart();
            var value = await cache.GetOrSetAsync("key", async _ => new MockObject(1024), TimeSpan.FromSeconds(100));
            Assert.AreEqual(value?.num, 1024);
        }

        [TestMethod]
        public async Task TestNullValueAsync()
        {
            var cache = new HzMemoryCache();
            await cache.SetAsync<MockObject>("key", null, TimeSpan.FromSeconds(100));
            Assert.IsNull(await cache.GetAsync<MockObject>("key"));
        }

        [TestMethod]
        public async Task TestLRUPolicyAsync()
        {
            var cache = new HzMemoryCache(new HzCacheOptions {evictionPolicy = EvictionPolicy.LRU, cleanupJobInterval = 50});
            await cache.SetAsync("key", new MockObject(1), TimeSpan.FromMilliseconds(120));
            Assert.IsNotNull(await cache.GetAsync<MockObject>("key"));
            await Task.Delay(100);
            Assert.IsNotNull(await cache.GetAsync<MockObject>("key"));
            await Task.Delay(100);
            Assert.IsNotNull(await cache.GetAsync<MockObject>("key"));
            await Task.Delay(125);
            Assert.IsNull(await cache.GetAsync<MockObject>("key"));
        }

        [TestMethod]
        [TestCategory("Integration")]
        public async Task TestRedisBatchGetAsync()
        {
            var cache = new HzMemoryCache(new HzCacheOptions {evictionPolicy = EvictionPolicy.FIFO, cleanupJobInterval = 50});

            for (var i = 0; i < 10; i++)
            {
                cache.Set("key." + i, new Mocko(i));
            }

            var keys = new List<string>
            {
                "key.0",
                "key.2",
                "key.20",
                "key.30",
                "key.40"
            };

            var x = await cache.GetOrSetBatchAsync(keys, async list =>
            {
                var strKeys = String.Join(",", list);
                Assert.AreEqual("key.20,key.30,key.40", strKeys);
                return list.Select(k => k.Substring("key.".Length)).Select(int.Parse).Select(i => new KeyValuePair<string, Mocko>("key." + i, new Mocko(i))).ToList();
            });

            for (var i = 0; i < keys.Count; i++)
            {
                var key = keys[i];
                var num = int.Parse(key.Substring("key.".Length));
                Assert.AreEqual(num, x[i].num);
            }
        }


        [TestMethod]
        public async Task TestFIFOPolicyAsync()
        {
            var cache = new HzMemoryCache(new HzCacheOptions {evictionPolicy = EvictionPolicy.FIFO, cleanupJobInterval = 50});
            await cache.SetAsync("key", new MockObject(1), TimeSpan.FromMilliseconds(220));
            await Task.Delay(100);
            Assert.IsNotNull(await cache.GetAsync<MockObject>("key"));
            await Task.Delay(100);
            Assert.IsNotNull(await cache.GetAsync<MockObject>("key"));
            await Task.Delay(100);
            Assert.IsNull(await cache.GetAsync<MockObject>("key"));
        }

        [TestMethod]
        public async Task TestTtlExtended()
        {
            var cache = new HzMemoryCache();
            await cache.SetAsync("42", new MockObject(42), TimeSpan.FromMilliseconds(300));

            await Task.Delay(50);
            var result = await cache.GetAsync<MockObject>("42");
            Assert.IsNotNull(result); //not evicted
            Assert.IsTrue(result.num == 42);

            await cache.SetAsync("42", new MockObject(42), TimeSpan.FromMilliseconds(300));

            await Task.Delay(250);

            result = await cache.GetAsync<MockObject>("42");
            Assert.IsNotNull(result); //still not evicted
            Assert.IsTrue(result.num == 42);
        }
    }
}
