using System.Diagnostics;
using RedisBackplaneMemoryCache;

namespace UnitTests
{
    public class Mocko
    {
        public Mocko(long num)
        {
            this.num = num;
            str = num.ToString();
        }

        public long num { get; }
        public string str { get; }
    }

    [TestClass]
    public class IntegrationTests
    {
        [TestMethod]
        [TestCategory("Integration")]
        public async Task TestRedisBackplaneInvalidation()
        {
            var c1 = new RedisBackplaneHzCache(
                new RedisBackplanceMemoryMemoryCacheOptions {redisConnectionString = "localhost", applicationCachePrefix = "trbpi", instanceId = "c1", asyncNotifications = false});
            await Task.Delay(200);
            var c2 = new RedisBackplaneHzCache(
                new RedisBackplanceMemoryMemoryCacheOptions {redisConnectionString = "localhost", applicationCachePrefix = "trbpi", instanceId = "c2", asyncNotifications = false});

            Console.WriteLine("Adding 1 to c1");
            c1.Set("1", new Mocko(1));
            await Task.Delay(100);
            Console.WriteLine("Adding 1 to c2");
            c2.Set("1", new Mocko(2));
            await Task.Delay(100);

            Assert.IsNull(c1.Get<Mocko>("1"));
            Assert.IsNotNull(c2.Get<Mocko>("1"));
        }

        [TestMethod]
        [TestCategory("Integration")]
        public async Task TestDistributedInvalidationPerformance()
        {
            var c1 = new RedisBackplaneHzCache(new RedisBackplanceMemoryMemoryCacheOptions
            {
                redisConnectionString = "localhost",
                applicationCachePrefix = "tdip",
                defaultTTL = TimeSpan.FromMinutes(5),
                asyncNotifications = false,
                instanceId = "c1"
            });

            var c2 = new RedisBackplaneHzCache(new RedisBackplanceMemoryMemoryCacheOptions
            {
                redisConnectionString = "localhost:6379",
                applicationCachePrefix = "tdip",
                defaultTTL = TimeSpan.FromMinutes(5),
                asyncNotifications = false,
                instanceId = "c2"
            });

            Console.WriteLine("Adding '10' to c1");
            c1.Set("10", new Mocko(1));
            await Task.Delay(10);
            Console.WriteLine("Adding '10' to c2");
            c2.Set("10", new Mocko(1));
            await Task.Delay(1000);
            Assert.IsNotNull(c1.Get<Mocko>("10"));
            Assert.IsNotNull(c2.Get<Mocko>("10"));

            var start = Stopwatch.StartNew();
            var iterations = 1000.0d;
            for (var i = 0; i < iterations; i++)
            {
                c1.Set("test" + i, new Mocko(i));
            }

            Console.WriteLine("TTA: " + (start.ElapsedMilliseconds / iterations) + " ms/cache storage operation");
        }
    }
}
