using System.Diagnostics;
using hzcache;
using RedisBackplaneMemoryCache;
using StackExchange.Redis;

namespace UnitTests
{
    public class Mocko
    {
        public Mocko(long num)
        {
            this.num = num;
            str = num.ToString();
        }

        public long num { get; set; }
        public string str { get; set; }
    }

    public class LargeMocko
    {
        public LargeMocko() { }

        public LargeMocko(long numOfItems)
        {
            for (var i = 0; i < numOfItems; i++)
            {
                var m = new Mocko(i);
                for (var j = 0; j < 100; j++)
                {
                    m.str += Guid.NewGuid().ToString();
                }

                items[i] = m;
            }
        }

        public Dictionary<int, Mocko> items { get; set; } = new();
    }

    [TestClass]
    public class IntegrationTests
    {
        [TestMethod]
        [TestCategory("Integration")]
        public async Task TestRedisBackplaneInvalidation()
        {
            var c1 = new RedisBackplaneHzCache(
                new RedisBackplaneMemoryMemoryCacheOptions {redisConnectionString = "localhost", applicationCachePrefix = "test", instanceId = "c1"});
            await Task.Delay(200);
            var c2 = new RedisBackplaneHzCache(
                new RedisBackplaneMemoryMemoryCacheOptions {redisConnectionString = "localhost", applicationCachePrefix = "test", instanceId = "c2"});

            Console.WriteLine("Adding 1 to c1");
            c1.Set("1", new Mocko(1));
            await Task.Delay(200);
            Console.WriteLine("Adding 1 to c2");
            c2.Set("1", new Mocko(2));
            await Task.Delay(200);

            Assert.IsNull(c1.Get<Mocko>("1"));
            Assert.IsNotNull(c2.Get<Mocko>("1"));
        }


        [TestMethod]
        [TestCategory("Integration")]
        public async Task TestLargeObjects()
        {
            var redis = ConnectionMultiplexer.Connect("localhost");
            var c1 = new RedisBackplaneHzCache(
                new RedisBackplaneMemoryMemoryCacheOptions
                {
                    redisConnectionString = "localhost", applicationCachePrefix = "test", instanceId = "c1", useRedisAs2ndLevelCache = true
                });
            await Task.Delay(200);
            var c2 = new RedisBackplaneHzCache(
                new RedisBackplaneMemoryMemoryCacheOptions
                {
                    redisConnectionString = "localhost", applicationCachePrefix = "test", instanceId = "c2", useRedisAs2ndLevelCache = true
                });

            for (var q = 1; q <= 10; q++)
            {
                Console.WriteLine("Adding 1 to c1");
                var o = new LargeMocko(5000);
                var s = Stopwatch.StartNew();
                c1.Set("" + q, o);
                Console.WriteLine($"Time to write large object: {s.ElapsedMilliseconds} ms");
                s = Stopwatch.StartNew();
                while (c2.Get<LargeMocko>("" + q) == null && s.ElapsedMilliseconds < 30000)
                {
                    await Task.Delay(100);
                }

                Console.WriteLine($"Time from write to available on second node: {s.ElapsedMilliseconds} ms");
                Assert.IsNotNull(c2.Get<Mocko>("1"));
            }

            c1.Set("testinglargeretrieval", new LargeMocko(10000));

            var stopWatch = Stopwatch.StartNew();
            while (!redis.GetDatabase().KeyExists("testinglargeretrieval") && stopWatch.ElapsedMilliseconds < 30000)
            {
                await Task.Delay(100);
            }

            stopWatch.Restart();
            c2.Get<LargeMocko>("testinglargeretrieval");
            stopWatch.Stop();
            Assert.IsTrue(stopWatch.ElapsedMilliseconds < 500);
            Console.WriteLine($"Reading from redis took {stopWatch.ElapsedMilliseconds} ms");
        }


        [TestMethod]
        [TestCategory("Integration")]
        public async Task TestRedisClear()
        {
            var redis = ConnectionMultiplexer.Connect("localhost");
            var c1 = new RedisBackplaneHzCache(
                new RedisBackplaneMemoryMemoryCacheOptions {redisConnectionString = "localhost", applicationCachePrefix = "test", instanceId = "c1"});
            await Task.Delay(200);
            var c2 = new RedisBackplaneHzCache(
                new RedisBackplaneMemoryMemoryCacheOptions {redisConnectionString = "localhost", applicationCachePrefix = "test", instanceId = "c2"});

            Console.WriteLine("Adding 1 to c1");
            c1.Set("1", new Mocko(1));
            await Task.Delay(100);
            Console.WriteLine("Adding 2 to c2");
            c1.Set("2", new Mocko(2));
            Console.WriteLine("Adding 2 to c2");
            c2.Set("3", new Mocko(3));
            await Task.Delay(100);

            c1.Clear();
            await Task.Delay(2000);
            Assert.IsNull(c1.Get<Mocko>("1"));
            Assert.IsNull(c2.Get<Mocko>("2"));
            Assert.IsNull(c2.Get<Mocko>("3"));
        }

        [TestMethod]
        [TestCategory("Integration")]
        public async Task TestRedisGet()
        {
            var redis = ConnectionMultiplexer.Connect("localhost");
            var c1 = new RedisBackplaneHzCache(
                new RedisBackplaneMemoryMemoryCacheOptions
                {
                    redisConnectionString = "localhost", applicationCachePrefix = "test", instanceId = "c1", useRedisAs2ndLevelCache = true
                });
            await Task.Delay(200);
            var c2 = new RedisBackplaneHzCache(
                new RedisBackplaneMemoryMemoryCacheOptions
                {
                    redisConnectionString = "localhost", applicationCachePrefix = "test", instanceId = "c2", useRedisAs2ndLevelCache = true
                });

            Console.WriteLine("Adding 1 to c1");
            c1.Set("1", new Mocko(10));
            await Task.Delay(100);
            Console.WriteLine("Getting 1 from c2");
            Assert.IsNotNull(c2.Get<Mocko>("1"));
        }


        [TestMethod]
        [TestCategory("Integration")]
        public async Task TestRedisBackplaneDelete()
        {
            var redis = ConnectionMultiplexer.Connect("localhost");
            var c1 = new RedisBackplaneHzCache(
                new RedisBackplaneMemoryMemoryCacheOptions {redisConnectionString = "localhost", applicationCachePrefix = "test", instanceId = "c1"});
            await Task.Delay(200);
            var c2 = new RedisBackplaneHzCache(
                new RedisBackplaneMemoryMemoryCacheOptions {redisConnectionString = "localhost", applicationCachePrefix = "test", instanceId = "c2"});

            Console.WriteLine("Adding 1 to c1");
            c1.Set("1", new Mocko(1));
            await Task.Delay(100);
            Console.WriteLine("Adding 2 to c2");
            c1.Set("2", new Mocko(2));
            await Task.Delay(100);
            Console.WriteLine("Delete 1 from c2");
            c2.Remove("1");
            await Task.Delay(300);
            Assert.IsNull(c1.Get<Mocko>("1"));
            Assert.IsNotNull(c1.Get<Mocko>("2"));
        }


        [TestMethod]
        [TestCategory("Integration")]
        public async Task TestRedisBackplaneDeleteByPattern()
        {
            var redis = ConnectionMultiplexer.Connect("localhost");
            var c1 = new RedisBackplaneHzCache(
                new RedisBackplaneMemoryMemoryCacheOptions {redisConnectionString = "localhost", applicationCachePrefix = "test", instanceId = "c1"});
            await Task.Delay(200);
            var c2 = new RedisBackplaneHzCache(
                new RedisBackplaneMemoryMemoryCacheOptions {redisConnectionString = "localhost", applicationCachePrefix = "test", instanceId = "c2"});

            c1.Set("11", new Mocko(11));
            c1.Set("12", new Mocko(12));
            c1.Set("22", new Mocko(22));
            c1.Set("13", new Mocko(13));
            c1.Set("23", new Mocko(23));
            c1.Set("33", new Mocko(33));
            Console.WriteLine("Deleting by pattern 2* on c2");
            await Task.Delay(200);
            c2.RemoveByPattern("2*");
            await Task.Delay(300);
            Assert.IsNotNull(c1.Get<Mocko>("11"));
            Assert.IsNotNull(c1.Get<Mocko>("12"));
            Assert.IsNotNull(c1.Get<Mocko>("13"));
            Assert.IsNotNull(c1.Get<Mocko>("33"));
            Assert.IsNull(c1.Get<Mocko>("22"));
            Assert.IsNull(c1.Get<Mocko>("23"));
            Console.WriteLine("Deleting by pattern 1*");
            c2.RemoveByPattern("1*");
            await Task.Delay(400);
            Assert.IsNull(c1.Get<Mocko>("11"));
            Assert.IsNull(c1.Get<Mocko>("12"));
            Assert.IsNull(c1.Get<Mocko>("13"));
            Assert.IsNotNull(c1.Get<Mocko>("33"));
        }

        [TestMethod]
        [TestCategory("Integration")]
        public async Task TestDistributedInvalidationPerformance()
        {
            var iterations = 1000000.0d;
            var redis = ConnectionMultiplexer.Connect("localhost");
            var c1 = new RedisBackplaneHzCache(new RedisBackplaneMemoryMemoryCacheOptions
            {
                redisConnectionString = "localhost",
                applicationCachePrefix = "test",
                defaultTTL = TimeSpan.FromSeconds(Math.Max(iterations / 10000, 20)),
                notificationType = NotificationType.Async,
                useRedisAs2ndLevelCache = true
            });

            var c2 = new RedisBackplaneHzCache(new RedisBackplaneMemoryMemoryCacheOptions
            {
                redisConnectionString = "localhost:6379",
                applicationCachePrefix = "test",
                defaultTTL = TimeSpan.FromSeconds(Math.Max(iterations / 10000, 20)),
                notificationType = NotificationType.Async,
                useRedisAs2ndLevelCache = true
            });

            Console.WriteLine("Adding 1 to c1");
            c1.Set("1", new Mocko(1));
            await Task.Delay(10);
            Console.WriteLine("Adding 1 to c2");
            c2.Set("1", new Mocko(1));
            await Task.Delay(20);
            Assert.IsNotNull(c1.Get<Mocko>("1"));
            Assert.IsNotNull(c2.Get<Mocko>("1"));

            var start = Stopwatch.StartNew();
            for (var i = 0; i < iterations; i++)
            {
                c1.Set("test" + i, new Mocko(i));
            }

            var end = start.ElapsedTicks;
            start.Restart();
            var size = 0;
            var max = Math.Max((int)iterations / 500, 20);
            while ((size = (int)redis.GetDatabase().Execute("DBSIZE")) < iterations + 1 && max-- > 0)
            {
                await Task.Delay(100);
            }

            Assert.IsTrue(max > 0);

            var setTime = (double)end / Stopwatch.Frequency * 1000;
            var postProcessingTime = (double)start.ElapsedTicks / Stopwatch.Frequency * 1000;
            Console.WriteLine($"Max: {max}, size: {size}");
            Console.WriteLine($"TTA: {setTime / iterations} ms/cache storage operation {setTime} ms/{iterations} items");
            Console.WriteLine($"Postprocessing {iterations} items took {postProcessingTime} ms, {postProcessingTime / iterations} ms/item");
            Console.WriteLine($"Complete throughput to redis: {iterations / (setTime + postProcessingTime)} items/ms");
        }
    }
}
