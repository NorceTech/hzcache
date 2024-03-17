using System.Collections.Concurrent;
using System.Runtime.Caching;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using hzcache;

BenchmarkRunner.Run<BenchMark>();

[ShortRunJob]
[MemoryDiagnoser]
public class BenchMark
{
    private static readonly HzMemoryCache _hzCache = new(new HzCacheOptions {cleanupJobInterval = 600_000, asyncNotifications = true});
    private static readonly ConcurrentDictionary<string, int> _dict = new();

    private static readonly DateTime _dtPlus10Mins = DateTime.Now.AddMinutes(10);

    [GlobalSetup]
    public void GlobalSetup()
    {
        //add 10000 values
        for (var i = 0; i < 1000; i++)
        {
            var v = new Mocko(i);
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
        var x = _hzCache.Get<Mocko>("test123");
        x = _hzCache.Get<Mocko>("test234");
        x = _hzCache.Get<Mocko>("test673");
        x = _hzCache.Get<Mocko>("test987");
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
        _hzCache.GetOrSet("test123", _ => new Mocko(123), TimeSpan.FromSeconds(1));
        _hzCache.GetOrSet("test234", _ => new Mocko(124), TimeSpan.FromSeconds(1));
        _hzCache.GetOrSet("test673", _ => new Mocko(125), TimeSpan.FromSeconds(1));
        _hzCache.GetOrSet("test987", _ => new Mocko(126), TimeSpan.FromSeconds(1));
    }

    [Benchmark]
    public void MemoryCacheAddOrGetExisting()
    {
        MemoryCache.Default.AddOrGetExisting("test123", new Mocko(123), DateTime.UtcNow.AddSeconds(1));
        MemoryCache.Default.AddOrGetExisting("test234", new Mocko(124), DateTime.UtcNow.AddSeconds(1));
        MemoryCache.Default.AddOrGetExisting("test673", new Mocko(125), DateTime.UtcNow.AddSeconds(1));
        MemoryCache.Default.AddOrGetExisting("test987", new Mocko(126), DateTime.UtcNow.AddSeconds(1));
    }

    [Benchmark]
    public void HzMemoryCacheSet()
    {
        _hzCache.Set("test123", new Mocko(123), TimeSpan.FromSeconds(1));
        _hzCache.Set("test234", new Mocko(124), TimeSpan.FromSeconds(1));
        _hzCache.Set("test673", new Mocko(125), TimeSpan.FromSeconds(1));
        _hzCache.Set("test987", new Mocko(126), TimeSpan.FromSeconds(1));
    }

    [Benchmark]
    public void MemoryCacheSet()
    {
        MemoryCache.Default.Set("test123", 123, DateTime.UtcNow.AddSeconds(1));
        MemoryCache.Default.Set("test234", 124, DateTime.UtcNow.AddSeconds(1));
        MemoryCache.Default.Set("test673", 125, DateTime.UtcNow.AddSeconds(1));
        MemoryCache.Default.Set("test987", 126, DateTime.UtcNow.AddSeconds(1));
    }

    [Benchmark]
    public void HzMemoryCacheAddRemove()
    {
        _hzCache.Set("1111", new Mocko(42), TimeSpan.FromMinutes(10));
        _hzCache.Remove("1111");
    }

    [Benchmark]
    public void MemoryCacheAddRemove()
    {
        MemoryCache.Default.Add("1111", new Mocko(42), _dtPlus10Mins);
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
