using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using RedisBackplaneMemoryCache;

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
        [TestMethod, TestCategory("Integration")]
        public async Task TestRedisBackplaneInvalidation()
        {
            RedisBackplaneHzCache c1 = new RedisBackplaneHzCache(new RedisBackplanceMemoryMemoryCacheOptions
            {
                redisConnectionString = "localhost", applicationCachePrefix = "test", instanceId = "c1"
            });
            await Task.Delay(200);
            RedisBackplaneHzCache c2 = new RedisBackplaneHzCache(new RedisBackplanceMemoryMemoryCacheOptions
            {
                redisConnectionString = "localhost", applicationCachePrefix = "test", instanceId = "c2"
            });

            Console.WriteLine("Adding 1 to c1");
            c1.Set("1", new Mocko(1));
            await Task.Delay(100);
            Console.WriteLine("Adding 1 to c2");
            c2.Set("1", new Mocko(2));
            await Task.Delay(100);
            
            Assert.IsNull(c1.Get<Mocko>("1"));
            Assert.IsNotNull(c2.Get<Mocko>("1"));
        }
        
        [TestMethod, TestCategory("Integration")]
        public async Task TestDistributedInvalidationPerformance()
        {
            RedisBackplaneHzCache c1 = new RedisBackplaneHzCache(new RedisBackplanceMemoryMemoryCacheOptions
            {
                redisConnectionString = "localhost", applicationCachePrefix = "test", defaultTTL = TimeSpan.FromMinutes(5)
            });

            RedisBackplaneHzCache c2 = new RedisBackplaneHzCache(new RedisBackplanceMemoryMemoryCacheOptions
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
