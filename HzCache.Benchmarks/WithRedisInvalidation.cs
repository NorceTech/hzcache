using System.Collections.Concurrent;
using System.Runtime.Caching;
using BenchmarkDotNet.Attributes;
using hzcache;
using RedisBackplaneMemoryCache;

namespace HzCache.Benchmarks
{
    [ShortRunJob]
    [MemoryDiagnoser]
    public class WithRedisInvalidation
    {
        private static readonly IDetailedHzCache _hzCache = new RedisBackplaneHzCache(new RedisBackplaneMemoryMemoryCacheOptions
        {
            applicationCachePrefix = "benchmark",
            redisConnectionString = "localhost",
            evictionPolicy = EvictionPolicy.FIFO,
            cleanupJobInterval = 600_000,
            notificationType = NotificationType.Async
        });

        private static readonly ConcurrentDictionary<string, int> _dict = new();

        private static readonly DateTime _dtPlus10Mins = DateTime.Now.AddMinutes(10);

        [GlobalSetup]
        public void GlobalSetup()
        {
            //add 10000 values
            for (var i = 0; i < 1000; i++)
            {
                var v = new BenchMark.Mocko(i);
                _dict.TryAdd("test" + i, i);
                _hzCache.Set("test" + i, v, TimeSpan.FromMinutes(10));
                MemoryCache.Default.Add("test" + i, v, _dtPlus10Mins);
            }
        }

        // [Benchmark]
        public void EvictExpired()
        {
            _hzCache.EvictExpired();
        }


        [Benchmark]
        public void HzMemoryCacheLookup()
        {
            var x = _hzCache.Get<BenchMark.Mocko>("test123");
            x = _hzCache.Get<BenchMark.Mocko>("test234");
            x = _hzCache.Get<BenchMark.Mocko>("test673");
            x = _hzCache.Get<BenchMark.Mocko>("test987");
        }

        [Benchmark]
        public void MemoryCacheLookup()
        {
            var x = MemoryCache.Default["test123"];
            x = MemoryCache.Default["test234"];
            x = MemoryCache.Default["test673"];
            x = MemoryCache.Default["test987"];
        }

        [Benchmark]
        public void HzMemoryCacheGetOrSet()
        {
            _hzCache.GetOrSet("test123", _ => new BenchMark.Mocko(123), TimeSpan.FromSeconds(120));
            _hzCache.GetOrSet("test234", _ => new BenchMark.Mocko(124), TimeSpan.FromSeconds(120));
            _hzCache.GetOrSet("test673", _ => new BenchMark.Mocko(125), TimeSpan.FromSeconds(120));
            _hzCache.GetOrSet("test987", _ => new BenchMark.Mocko(126), TimeSpan.FromSeconds(120));
        }

        [Benchmark]
        public void MemoryCacheAddOrGetExisting()
        {
            MemoryCache.Default.AddOrGetExisting("test123", new BenchMark.Mocko(123), DateTime.UtcNow.AddSeconds(120));
            MemoryCache.Default.AddOrGetExisting("test234", new BenchMark.Mocko(124), DateTime.UtcNow.AddSeconds(120));
            MemoryCache.Default.AddOrGetExisting("test673", new BenchMark.Mocko(125), DateTime.UtcNow.AddSeconds(120));
            MemoryCache.Default.AddOrGetExisting("test987", new BenchMark.Mocko(126), DateTime.UtcNow.AddSeconds(120));
        }

        [Benchmark]
        public void HzMemoryCacheSet()
        {
            _hzCache.Set("test123", new BenchMark.Mocko(123), TimeSpan.FromSeconds(120));
            _hzCache.Set("test234", new BenchMark.Mocko(124), TimeSpan.FromSeconds(120));
            _hzCache.Set("test673", new BenchMark.Mocko(125), TimeSpan.FromSeconds(120));
            _hzCache.Set("test987", new BenchMark.Mocko(126), TimeSpan.FromSeconds(120));
        }

        [Benchmark]
        public void MemoryCacheSet()
        {
            MemoryCache.Default.Set("test123", 123, DateTime.UtcNow.AddSeconds(120));
            MemoryCache.Default.Set("test234", 124, DateTime.UtcNow.AddSeconds(120));
            MemoryCache.Default.Set("test673", 125, DateTime.UtcNow.AddSeconds(120));
            MemoryCache.Default.Set("test987", 126, DateTime.UtcNow.AddSeconds(120));
        }

        [Benchmark]
        public void HzMemoryCacheAddRemove()
        {
            _hzCache.Set("1111", new BenchMark.Mocko(42), TimeSpan.FromMinutes(10));
            _hzCache.Remove("1111");
        }

        [Benchmark]
        public void MemoryCacheAddRemove()
        {
            MemoryCache.Default.Add("1111", new BenchMark.Mocko(42), _dtPlus10Mins);
            MemoryCache.Default.Remove("1111");
        }


        public class Mocko
        {
            public Mocko() { }

            public Mocko(int num)
            {
                this.num = num;
            }

            public int num { get; }
        }
    }
}
