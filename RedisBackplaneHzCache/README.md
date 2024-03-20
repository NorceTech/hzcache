# RedisBackplaneHzCache

This is a wrapper around HzCache and uses HzCache's notification capabilities to implement a two-tiered cache with a Redis
as 2nd level cache. This is useful when you have multiple instances of the same application and want to share cache.

## TL;DR

Example usage:

```csharp
class CacheItem
{
    public string Name { get; set; }
    public int Age { get; set; }
}
var cache = new RedisBackplaneHzCache(new RedisBackplaneHzCacheOptions
{
    redisConnectionString = "localhost:6379",
    applicationCachePrefix = "MyApp"
});

var item = new CacheItem { Name = "John", Age = 42 };
cache.Set("John", item, TimeSpan.FromMinutes(5));
```
