using System.Diagnostics;
using HzCache;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
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

        public string guid { get; } = Guid.NewGuid().ToString();
        public long timestamp { get; } = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        public long num { get; set; }
        public string str { get; set; }
    }

    public class LargeMocko
    {
        public LargeMocko() { }

        public LargeMocko(string key, long numOfItems)
        {
            this.key = key;
            for (var i = 0; i < numOfItems; i++)
            {
                var m = new Mocko(i);
                m.str = $"asd qwe zxc ert {i}";
                items[i] = m;
            }
        }

        public Dictionary<int, Mocko> items { get; set; } = new();
        public string key { get; set; }
    }

    [TestClass]
    public class IntegrationTests
    {
        [TestMethod]
        [TestCategory("Integration")]
        public async Task TestRedisBackplaneInvalidation()
        {
            var c1 = new RedisBackedHzCache(
                new RedisBackedHzCacheOptions {redisConnectionString = "localhost", applicationCachePrefix = "test", instanceId = "c1"});
            await Task.Delay(200);
            var c2 = new RedisBackedHzCache(
                new RedisBackedHzCacheOptions {redisConnectionString = "localhost", applicationCachePrefix = "test", instanceId = "c2"});

            Console.WriteLine("Adding 1 to c1");
            await c1.SetAsync("1", new Mocko(1));
            await Task.Delay(200);
            Console.WriteLine("Adding 1 to c2");
            await c2.SetAsync("1", new Mocko(2));
            await Task.Delay(200);

            Assert.IsNull(c1.Get<Mocko>("1"));
            Assert.IsNotNull(c2.Get<Mocko>("1"));
        }


        [TestMethod]
        [TestCategory("Integration")]
        public async Task TestLargeObjects()
        {
            var serviceProvider = new ServiceCollection()
                .AddLogging(
                    builder => builder
                        .AddSimpleConsole(options =>
                        {
                            options.IncludeScopes = true;
                            options.SingleLine = true;
                            options.TimestampFormat = "HH:mm:ss ";
                        })
                        .SetMinimumLevel(LogLevel.Trace)
                )
                .BuildServiceProvider();

            var factory = serviceProvider.GetService<ILoggerFactory>();

            var logger = factory.CreateLogger<IntegrationTests>();

            var redis = ConnectionMultiplexer.Connect("localhost");
            var c1 = new RedisBackedHzCache(
                new RedisBackedHzCacheOptions
                {
                    redisConnectionString = "localhost",
                    applicationCachePrefix = "largeobjectstest",
                    instanceId = "c1",
                    useRedisAs2ndLevelCache = true,
                    notificationType = NotificationType.Async,
                    logger = factory.CreateLogger<RedisBackedHzCache>()
                });
            await Task.Delay(200);
            var c2 = new RedisBackedHzCache(
                new RedisBackedHzCacheOptions
                {
                    redisConnectionString = "localhost",
                    applicationCachePrefix = "largeobjectstest",
                    instanceId = "c2",
                    useRedisAs2ndLevelCache = true,
                    notificationType = NotificationType.Async,
                    logger = factory.CreateLogger<RedisBackedHzCache>()
                });

            var objectList = new List<LargeMocko>();

            for (var q = 1; q <= 10; q++)
            {
                objectList.Add(new LargeMocko("o" + q, 50000));
            }

            foreach (var o in objectList)
            {
                var s = Stopwatch.StartNew();
                await c1.SetAsync(o.key, o);
                logger.LogInformation("Time to write large object {Q} to c1: {Elapsed} ms", o.key, s.ElapsedMilliseconds);
                s = Stopwatch.StartNew();
                while (await c2.GetAsync<LargeMocko>(o.key) == null && s.ElapsedMilliseconds < 30000)
                {
                    await Task.Delay(50);
                }

                logger.LogInformation("Time from write to available on second node: {Elapsed} ms", s.ElapsedMilliseconds);
                Assert.IsNotNull(await c2.GetAsync<LargeMocko>(o.key));
            }

            logger.LogInformation("Starting testinglargeretrieval");
            await c1.SetAsync("testinglargeretrieval", new LargeMocko("testinglargeretrieval", 100000));

            var stopWatch = Stopwatch.StartNew();
            while (!await redis.GetDatabase().KeyExistsAsync("testinglargeretrieval") && stopWatch.ElapsedMilliseconds < 30000)
            {
                await Task.Delay(50);
            }

            stopWatch.Restart();
            await c2.GetAsync<LargeMocko>("testinglargeretrieval");
            stopWatch.Stop();
            Assert.IsTrue(stopWatch.ElapsedMilliseconds < 1500);
            logger.LogInformation("Reading from redis took {Elapsed} ms", stopWatch.ElapsedMilliseconds);
            logger.LogInformation("c1 stats: {Stats}", c1.GetStatistics());
            logger.LogInformation("c2 stats: {Stats}", c2.GetStatistics());
        }

                [TestMethod]
        [TestCategory("Integration")]
        public async Task TestLargeObjectsWithCompression()
        {
            var serviceProvider = new ServiceCollection()
                .AddLogging(
                    builder => builder
                        .AddSimpleConsole(options =>
                        {
                            options.IncludeScopes = true;
                            options.SingleLine = true;
                            options.TimestampFormat = "HH:mm:ss ";
                        })
                        .SetMinimumLevel(LogLevel.Trace)
                )
                .BuildServiceProvider();

            var factory = serviceProvider.GetService<ILoggerFactory>();

            var logger = factory.CreateLogger<IntegrationTests>();

            var redis = ConnectionMultiplexer.Connect("localhost");
            var c1 = new RedisBackedHzCache(
                new RedisBackedHzCacheOptions
                {
                    redisConnectionString = "localhost",
                    applicationCachePrefix = "largeobjectstest",
                    instanceId = "c1",
                    useRedisAs2ndLevelCache = true,
                    notificationType = NotificationType.Async,
                    logger = factory.CreateLogger<RedisBackedHzCache>(),
                    enableLObCompression = true
                });
            await Task.Delay(200);
            var c2 = new RedisBackedHzCache(
                new RedisBackedHzCacheOptions
                {
                    redisConnectionString = "localhost",
                    applicationCachePrefix = "largeobjectstest",
                    instanceId = "c2",
                    useRedisAs2ndLevelCache = true,
                    notificationType = NotificationType.Async,
                    logger = factory.CreateLogger<RedisBackedHzCache>(),
                    enableLObCompression = true
                });

            var objectList = new List<LargeMocko>();

            for (var q = 1; q <= 10; q++)
            {
                objectList.Add(new LargeMocko("o" + q, 50000));
            }

            foreach (var o in objectList)
            {
                var s = Stopwatch.StartNew();
                await c1.SetAsync(o.key, o);
                logger.LogInformation("Time to write large object {Q} to c1: {Elapsed} ms", o.key, s.ElapsedMilliseconds);
                s = Stopwatch.StartNew();
                while (await c2.GetAsync<LargeMocko>(o.key) == null && s.ElapsedMilliseconds < 30000)
                {
                    await Task.Delay(50);
                }

                logger.LogInformation("Time from write to available on second node: {Elapsed} ms", s.ElapsedMilliseconds);
                Assert.IsNotNull(await c2.GetAsync<LargeMocko>(o.key));
            }

            logger.LogInformation("Starting testinglargeretrieval");
            await c1.SetAsync("testinglargeretrieval", new LargeMocko("testinglargeretrieval", 100000));

            var stopWatch = Stopwatch.StartNew();
            while (!await redis.GetDatabase().KeyExistsAsync("testinglargeretrieval") && stopWatch.ElapsedMilliseconds < 30000)
            {
                await Task.Delay(50);
            }

            stopWatch.Restart();
            await c2.GetAsync<LargeMocko>("testinglargeretrieval");
            stopWatch.Stop();
            Assert.IsTrue(stopWatch.ElapsedMilliseconds < 1500);
            logger.LogInformation("Reading from redis took {Elapsed} ms", stopWatch.ElapsedMilliseconds);
            logger.LogInformation("c1 stats: {Stats}", c1.GetStatistics());
            logger.LogInformation("c2 stats: {Stats}", c2.GetStatistics());
        }


        [TestMethod]
        [TestCategory("Integration")]
        public async Task TestRedisClear()
        {
            var redis = ConnectionMultiplexer.Connect("localhost");
            var c1 = new RedisBackedHzCache(
                new RedisBackedHzCacheOptions {redisConnectionString = "localhost", applicationCachePrefix = "test", instanceId = "c1"});
            await Task.Delay(200);
            var c2 = new RedisBackedHzCache(
                new RedisBackedHzCacheOptions {redisConnectionString = "localhost", applicationCachePrefix = "test", instanceId = "c2"});

            Console.WriteLine("Adding 1 to c1");
            await c1.SetAsync("1", new Mocko(1));
            await Task.Delay(100);
            Console.WriteLine("Adding 2 to c2");
            await c1.SetAsync("2", new Mocko(2));
            Console.WriteLine("Adding 2 to c2");
            await c2.SetAsync("3", new Mocko(3));
            await Task.Delay(100);

            await c1.ClearAsync();
            await Task.Delay(2000);
            Assert.IsNull(await c1.GetAsync<Mocko>("1"));
            Assert.IsNull(await c2.GetAsync<Mocko>("2"));
            Assert.IsNull(await c2.GetAsync<Mocko>("3"));
        }

        [TestMethod]
        [TestCategory("Integration")]
        public async Task TestRedisGet()
        {
            var c1 = new RedisBackedHzCache(
                new RedisBackedHzCacheOptions {redisConnectionString = "localhost", applicationCachePrefix = "test", instanceId = "c1", useRedisAs2ndLevelCache = true});
            await Task.Delay(200);
            var c2 = new RedisBackedHzCache(
                new RedisBackedHzCacheOptions {redisConnectionString = "localhost", applicationCachePrefix = "test", instanceId = "c2", useRedisAs2ndLevelCache = true});

            Console.WriteLine("Adding 1 to c1");
            await c1.SetAsync("1", new Mocko(10));
            await Task.Delay(100);
            Console.WriteLine("Getting 1 from c2");
            Assert.IsNotNull(await c2.GetAsync<Mocko>("1"));
        }

        [TestMethod]
        [TestCategory("Integration")]
        public async Task TestRedisGetOrSet()
        {
            var c1 = new RedisBackedHzCache(
                new RedisBackedHzCacheOptions {redisConnectionString = "localhost", applicationCachePrefix = "test", instanceId = "c1", useRedisAs2ndLevelCache = true});
            await Task.Delay(200);
            var c2 = new RedisBackedHzCache(
                new RedisBackedHzCacheOptions {redisConnectionString = "localhost", applicationCachePrefix = "test", instanceId = "c2", useRedisAs2ndLevelCache = true});

            Console.WriteLine("Adding 1 to c1");
            var v1 = c1.GetOrSet("1", _ => new Mocko(10), TimeSpan.FromMinutes(1));
            Assert.IsNotNull(v1);
            Assert.IsTrue(c1.Get<Mocko>("1").num == 10);
            await Task.Delay(100);
            var c21 = await c2.GetAsync<Mocko>("1");
            Assert.IsTrue(c21.num == 10);
            Assert.IsTrue(c21.guid != v1.guid);
        }


        [TestMethod]
        [TestCategory("Integration")]
        public async Task TestRedisBackplaneDelete()
        {
            var redis = ConnectionMultiplexer.Connect("localhost");
            var c1 = new RedisBackedHzCache(
                new RedisBackedHzCacheOptions {redisConnectionString = "localhost", applicationCachePrefix = "test", instanceId = "c1"});
            await Task.Delay(200);
            var c2 = new RedisBackedHzCache(
                new RedisBackedHzCacheOptions {redisConnectionString = "localhost", applicationCachePrefix = "test", instanceId = "c2"});

            Console.WriteLine("Adding 1 to c1");
            await c1.SetAsync("1", new Mocko(1));
            await Task.Delay(100);
            Console.WriteLine("Adding 2 to c2");
            await c1.SetAsync("2", new Mocko(2));
            await Task.Delay(100);
            Console.WriteLine("Delete 1 from c2");
            await c2.RemoveAsync("1");
            await Task.Delay(300);
            Assert.IsNull(await c1.GetAsync<Mocko>("1"));
            Assert.IsNotNull(await c1.GetAsync<Mocko>("2"));
        }


        [TestMethod]
        [TestCategory("Integration")]
        public async Task TestRedisBatchGet()
        {
            var redis = ConnectionMultiplexer.Connect("localhost");
            var c1 = new RedisBackedHzCache(
                new RedisBackedHzCacheOptions
                {
                    redisConnectionString = "localhost",
                    applicationCachePrefix = "batch2",
                    instanceId = "c1",
                    useRedisAs2ndLevelCache = true,
                    defaultTTL = TimeSpan.FromSeconds(60)
                });

            for (var i = 0; i < 10; i++)
            {
                await c1.SetAsync("key." + i, new Mocko(i));
            }

            await Task.Delay(3000);

            var keys = new List<string>
            {
                "key.0",
                "key.2",
                "key.20",
                "key.30",
                "key.40"
            };

            var x = await c1.GetOrSetBatchAsync(keys, async list =>
            {
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
        [TestCategory("Integration")]
        public async Task TestRedisBackplaneDeleteByPattern()
        {
            var redis = ConnectionMultiplexer.Connect("localhost");
            var c1 = new RedisBackedHzCache(
                new RedisBackedHzCacheOptions {redisConnectionString = "localhost", applicationCachePrefix = "test", instanceId = "c1"});
            await Task.Delay(200);
            var c2 = new RedisBackedHzCache(
                new RedisBackedHzCacheOptions {redisConnectionString = "localhost", applicationCachePrefix = "test", instanceId = "c2"});

            await c1.SetAsync("11", new Mocko(11));
            await c1.SetAsync("12", new Mocko(12));
            await c1.SetAsync("22", new Mocko(22));
            await c1.SetAsync("13", new Mocko(13));
            await c1.SetAsync("23", new Mocko(23));
            await c1.SetAsync("33", new Mocko(33));
            Console.WriteLine("Deleting by pattern 2* on c2");
            await Task.Delay(200);
            await c2.RemoveByPatternAsync("2*");
            await Task.Delay(300);
            Assert.IsNotNull(await c1.GetAsync<Mocko>("11"));
            Assert.IsNotNull(await c1.GetAsync<Mocko>("12"));
            Assert.IsNotNull(await c1.GetAsync<Mocko>("13"));
            Assert.IsNotNull(await c1.GetAsync<Mocko>("33"));
            Assert.IsNull(c1.Get<Mocko>("22"));
            Assert.IsNull(c1.Get<Mocko>("23"));
            Console.WriteLine("Deleting by pattern 1*");
            await c2.RemoveByPatternAsync("1*");
            await Task.Delay(400);
            Assert.IsNull(c1.Get<Mocko>("11"));
            Assert.IsNull(c1.Get<Mocko>("12"));
            Assert.IsNull(c1.Get<Mocko>("13"));
            Assert.IsNotNull(await c1.GetAsync<Mocko>("33"));
        }

        [TestMethod]
        [TestCategory("Integration")]
        public async Task TestDistributedInvalidationPerformance()
        {
            var iterations = 500000.0d;
            var redis = ConnectionMultiplexer.Connect("localhost");
            var c1 = new RedisBackedHzCache(new RedisBackedHzCacheOptions
            {
                redisConnectionString = "localhost",
                applicationCachePrefix = "test",
                defaultTTL = TimeSpan.FromSeconds(Math.Max(iterations / 10000, 20)),
                notificationType = NotificationType.Async,
                useRedisAs2ndLevelCache = true
            });

            var c2 = new RedisBackedHzCache(new RedisBackedHzCacheOptions
            {
                redisConnectionString = "localhost:6379",
                applicationCachePrefix = "test",
                defaultTTL = TimeSpan.FromSeconds(Math.Max(iterations / 10000, 20)),
                notificationType = NotificationType.Async,
                useRedisAs2ndLevelCache = true
            });

            Console.WriteLine("Adding 1 to c1");
            await c1.SetAsync("1", new Mocko(1));
            await Task.Delay(10);
            Console.WriteLine("Adding 1 to c2");
            await c2.SetAsync("1", new Mocko(1));
            await Task.Delay(20);
            Assert.IsNotNull(await c1.GetAsync<Mocko>("1"));
            Assert.IsNotNull(await c2.GetAsync<Mocko>("1"));

            var start = Stopwatch.StartNew();
            for (var i = 0; i < iterations; i++)
            {
                await c1.SetAsync("test" + i, new Mocko(i));
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
            Console.WriteLine(c1.GetStatistics().ToString());
        }
    }
}
