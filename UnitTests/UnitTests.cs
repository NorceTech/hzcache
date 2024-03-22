using System.Diagnostics;
using hzcache;

namespace UnitTests
{
    public class MockObject
    {
        public int? num;

        public MockObject(int num)
        {
            this.num = num;
        }

        public MockObject()
        {
        }
    }

    [TestClass]
    public class UnitTests
    {
        [TestMethod]
        public async Task TestValueChangeNotification()
        {
            var addOrUpdates = 0;
            var removals = 0;
            var expires = 0;
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
                                expires++;
                                break;
                            case CacheItemChangeType.Remove:
                                removals++;
                                break;
                        }
                    }
                });
            cache.Set("mock2", new MockObject(1));
            Assert.AreEqual(1, addOrUpdates);
            cache.Set("mock2", new MockObject(2));
            Assert.AreEqual(2, addOrUpdates);
            cache.Remove("mock2");
            Assert.AreEqual(1, removals);
            cache.Remove("mock2");
            Assert.AreEqual(2, removals);
            cache.GetOrSet("m", _ => new MockObject(1), TimeSpan.FromMilliseconds(100));
            Assert.AreEqual(3, addOrUpdates);
            await Task.Delay(200);
            Assert.AreEqual(1, expires);
        }

        [TestMethod]
        public async Task TestRemoveByPattern()
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
            cache.Set("pelle", new MockObject(42));
            cache.Set("olle", new MockObject(42));
            cache.Set("kalle", new MockObject(42));
            cache.Set("stina", new MockObject(42));
            cache.Set("lina", new MockObject(42));
            cache.Set("nina", new MockObject(42));
            cache.Set("tom", new MockObject(42));
            cache.Set("tomma", new MockObject(42));
            cache.Set("flina", new MockObject(42));

            cache.RemoveByPattern("tom*");
            Assert.IsNull(cache.Get<MockObject>("tom"));
            Assert.IsNull(cache.Get<MockObject>("tomma"));

            cache.RemoveByPattern("*lle*");
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
        public async Task TestGetSetCleanup()
        {
            var cache = new HzMemoryCache(new HzCacheOptions {cleanupJobInterval = 200});
            cache.Set("42", new MockObject(42), TimeSpan.FromMilliseconds(100));
            var v = cache.Get<MockObject>("42");
            Assert.IsTrue(v != null);
            Assert.IsTrue(v.num == 42);

            await Task.Delay(300);
            Assert.IsTrue(cache.Count == 0); //cleanup job has run?
        }

        [TestMethod]
        public async Task TestEviction()
        {
            var list = new List<HzMemoryCache>();
            for (var i = 0; i < 20; i++)
            {
                var cache = new HzMemoryCache(new HzCacheOptions {cleanupJobInterval = 200});
                cache.Set("42", new MockObject(42), TimeSpan.FromMilliseconds(100));
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
        public async Task Shortdelay()
        {
            var cache = new HzMemoryCache();
            cache.Set("42", new MockObject(42), TimeSpan.FromMilliseconds(500));

            await Task.Delay(20);

            var result = cache.Get<MockObject>("42");
            Assert.IsNotNull(result); //not evicted
            Assert.IsTrue(result.num == 42);
        }

        [TestMethod]
        public async Task TestPrimitives()
        {
            var cache = new HzMemoryCache();
            cache.GetOrSet("42", v => 42, TimeSpan.FromMilliseconds(500));

            await Task.Delay(20);

            var result = cache.Get<int>("42");
            Assert.IsNotNull(result); //not evicted
            Assert.IsTrue(result == 42);
        }

        [TestMethod]
        public async Task TestWithDefaultJobInterval()
        {
            var cache2 = new HzMemoryCache();
            cache2.Set("42", new MockObject(42), TimeSpan.FromMilliseconds(100));
            Assert.IsNotNull(cache2.Get<MockObject>("42"));
            await Task.Delay(150);
            Assert.IsNull(cache2.Get<MockObject>("42"));
        }

        [TestMethod]
        public void TestRemove()
        {
            var cache = new HzMemoryCache();
            cache.Set("42", new MockObject(42), TimeSpan.FromMilliseconds(100));
            cache.Remove("42");
            Assert.IsNull(cache.Get<MockObject>("42"));
        }

        [TestMethod]
        public void TestTryRemove()
        {
            var cache = new HzMemoryCache();
            cache.Set("42", new MockObject(42), TimeSpan.FromMilliseconds(100));
            var res = cache.Remove("42");
            Assert.IsTrue(res);
            Assert.IsNull(cache.Get<MockObject>("42"));

            //now try remove non-existing item
            Assert.IsFalse(cache.Remove("blabblah"));
        }

        [TestMethod]
        public async Task TestTryRemoveWithTtl()
        {
            var cache = new HzMemoryCache();
            cache.Set("42", new MockObject(42), TimeSpan.FromMilliseconds(100));
            await Task.Delay(120); //let the item expire

            var res = cache.Remove("42");
            Assert.IsFalse(res);
        }

        [TestMethod]
        public void TestClear()
        {
            var cache = new HzMemoryCache();
            cache.GetOrSet("key", _ => new MockObject(1024), TimeSpan.FromSeconds(100));

            cache.Clear();

            Assert.IsNull(cache.Get<MockObject>("key"));
        }

        [TestMethod]
        public async Task TestStampedeProtection()
        {
            var cache = new HzMemoryCache();
            var stopwatch = Stopwatch.StartNew();
            Task.Run(() =>
            {
                cache.GetOrSet("key", _ =>
                {
                    Task.Delay(2000).GetAwaiter().GetResult();
                    return new MockObject(1024);
                }, TimeSpan.FromSeconds(100));
            });
            await Task.Delay(50);
            var timeToInsert = stopwatch.ElapsedTicks / Stopwatch.Frequency * 1000;
            stopwatch.Restart();
            var x = cache.GetOrSet("key", _ => new MockObject(1024), TimeSpan.FromSeconds(100));
            var timeToWait = stopwatch.ElapsedTicks / Stopwatch.Frequency * 1000;
            stopwatch.Restart();
            cache.GetOrSet("key", _ => new MockObject(1024), TimeSpan.FromSeconds(100));

            var timeToGetFromMemoryCache = stopwatch.ElapsedTicks / Stopwatch.Frequency * 1000;

            Console.WriteLine($"Insert: {timeToInsert}, Wait: {timeToWait}, Get: {timeToGetFromMemoryCache}");

            Assert.IsTrue(timeToInsert < 50);
            Assert.IsTrue(timeToWait is > 950 and < 1050);
            Assert.IsTrue(timeToGetFromMemoryCache < 50);
        }

        [TestMethod]
        public async Task TestStampedeProtectionTimeout()
        {
            var cache = new HzMemoryCache();
            var stopwatch = Stopwatch.StartNew();
            Task.Run(() =>
            {
                cache.GetOrSet("key", _ =>
                {
                    Task.Delay(2000).GetAwaiter().GetResult();
                    return new MockObject(1024);
                }, TimeSpan.FromSeconds(100), 1000);
            });
            await Task.Delay(50);
            var timeToInsert = stopwatch.ElapsedTicks / Stopwatch.Frequency * 1000;
            stopwatch.Restart();
            var x = cache.GetOrSet("key", _ => new MockObject(2048), TimeSpan.FromSeconds(100));
            var timeToWait = stopwatch.ElapsedTicks / Stopwatch.Frequency * 1000;
            stopwatch.Restart();
            var value = cache.GetOrSet("key", _ => new MockObject(1024), TimeSpan.FromSeconds(100));
            Assert.AreEqual(value?.num, 2048);
        }

        [TestMethod]
        public void TestNullValue()
        {
            var cache = new HzMemoryCache();
            cache.Set<MockObject>("key", null, TimeSpan.FromSeconds(100));
            Assert.IsNull(cache.Get<MockObject>("key"));
        }

        [TestMethod]
        public async Task TestLRUPolicy()
        {
            var cache = new HzMemoryCache(new HzCacheOptions {evictionPolicy = EvictionPolicy.LRU, cleanupJobInterval = 50});
            cache.Set("key", new MockObject(1), TimeSpan.FromMilliseconds(120));
            Assert.IsNotNull(cache.Get<MockObject>("key"));
            await Task.Delay(100);
            Assert.IsNotNull(cache.Get<MockObject>("key"));
            await Task.Delay(100);
            Assert.IsNotNull(cache.Get<MockObject>("key"));
            await Task.Delay(125);
            Assert.IsNull(cache.Get<MockObject>("key"));
        }

        [TestMethod]
        [TestCategory("Integration")]
        public async Task TestRedisBatchGet()
        {
            var cache = new HzMemoryCache(new HzCacheOptions {evictionPolicy = EvictionPolicy.FIFO, cleanupJobInterval = 50});

            for (int i = 0; i < 10; i++)
            {
                cache.Set("key."+i, new Mocko(i));
            }

            var keys = new List<string> { "key.0", "key.2", "key.20", "key.30", "key.40"};

            var x = cache.GetOrSetBatch(keys, list =>
            {
                var strKeys = String.Join(",", list);
                Assert.AreEqual("key.20,key.30,key.40", strKeys);
                return list.Select(k => k.Substring("key.".Length)).Select(int.Parse).Select(i => new KeyValuePair<string,Mocko>("key."+i, new Mocko(i))).ToList();
            });

            for (int i=0; i<keys.Count; i++)
            {
                var key = keys[i];
                var num = int.Parse(key.Substring("key.".Length));
                Assert.AreEqual(num, x[i].num);
            }
        }

        
        [TestMethod]
        public async Task TestFIFOPolicy()
        {
            var cache = new HzMemoryCache(new HzCacheOptions {evictionPolicy = EvictionPolicy.FIFO, cleanupJobInterval = 50});
            cache.Set("key", new MockObject(1), TimeSpan.FromMilliseconds(220));
            await Task.Delay(100);
            Assert.IsNotNull(cache.Get<MockObject>("key"));
            await Task.Delay(100);
            Assert.IsNotNull(cache.Get<MockObject>("key"));
            await Task.Delay(100);
            Assert.IsNull(cache.Get<MockObject>("key"));
        }
        
        [TestMethod]
        public async Task TestTtlExtended()
        {
            var cache = new HzMemoryCache();
            cache.Set("42", new MockObject(42), TimeSpan.FromMilliseconds(300));

            await Task.Delay(50);
            var result = cache.Get<MockObject>("42");
            Assert.IsNotNull(result); //not evicted
            Assert.IsTrue(result.num == 42);

            cache.Set("42", new MockObject(42), TimeSpan.FromMilliseconds(300));

            await Task.Delay(250);

            result = cache.Get<MockObject>("42");
            Assert.IsNotNull(result); //still not evicted
            Assert.IsTrue(result.num == 42);
        }
    }
}
