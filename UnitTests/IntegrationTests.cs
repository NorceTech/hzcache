using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;

namespace UnitTests
{
    public class Mocko
    {
        public long num { get; }
        public string str { get; }

        public Mocko(long num)
        {
            this.num = num;
            this.str = num.ToString();
        }
    }

    [TestClass]
    public class IntegrationTests
    {
        [TestMethod]
        public async Task TestRedisBackplaneInvalidation()
        {
            RedisBackplaneMemoryHzCache c1 = new RedisBackplaneMemoryHzCache(new RedisBackplanceMemoryMemoryCacheOptions
            {
                redisConnectionString = "localhost", applicationCachePrefix = "test", instanceId = "c1"
            });
            await Task.Delay(200);
            RedisBackplaneMemoryHzCache c2 = new RedisBackplaneMemoryHzCache(new RedisBackplanceMemoryMemoryCacheOptions
            {
                redisConnectionString = "localhost", applicationCachePrefix = "test", instanceId = "c2"
            });

            Console.WriteLine("Adding 1 to c1");
            c1.Set("1", new Mocko(1));
            await Task.Delay(100);
            Console.WriteLine("Adding 1 to c2");
            c2.Set("1", new Mocko(2));
            await Task.Delay(2000);
            
            Assert.IsNull(c1.Get<Mocko>("1"));
            Assert.IsNotNull(c2.Get<Mocko>("1"));
        }
        
        [TestMethod]
        public async Task TestDistributedInvalidationPerformance()
        {
            RedisBackplaneMemoryHzCache c1 = new RedisBackplaneMemoryHzCache(new RedisBackplanceMemoryMemoryCacheOptions
            {
                redisConnectionString = "localhost", applicationCachePrefix = "test", defaultTTL = TimeSpan.FromMinutes(5)
            });

            RedisBackplaneMemoryHzCache c2 = new RedisBackplaneMemoryHzCache(new RedisBackplanceMemoryMemoryCacheOptions
            {
                redisConnectionString = "localhost:6379", applicationCachePrefix = "test", defaultTTL = TimeSpan.FromMinutes(5)
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
            var iterations = 1000.0d;
            for (int i=0; i<iterations; i++)
            {
                c1.Set("test"+i, new Mocko(i));
            }
            Console.WriteLine("TTA: "+start.ElapsedMilliseconds/iterations+" ms/cache storage operation");
        }

    }
}
