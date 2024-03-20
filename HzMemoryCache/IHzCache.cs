#nullable enable
using System;

namespace hzcache
{
    public enum CacheItemChangeType
    {
        /// <summary>
        ///     A cache item was added or updated
        /// </summary>
        AddOrUpdate,

        /// <summary>
        ///     A cache item was removed
        /// </summary>
        Remove,

        /// <summary>
        ///     A cache item expired
        /// </summary>
        Expire
    }

    public enum NotificationType
    {
        Async, Sync, None
    }


    /// <summary>
    ///     The eviction policy to use for the cache.
    ///     LRU is "Least Recently Used" and FIFO is "First In First Out". Which is almost true.
    ///     LRU means that the item expiry TTL is extended on read, and FIFO means that the item
    ///     expiry TTL is set on write. So if all items have the same expiry, then it's LRU and FIFO.
    /// </summary>
    public enum EvictionPolicy
    {
        /// <summary>
        ///     LRU means that the item expiry TTL is set on write and extended on read.
        /// </summary>
        LRU,

        /// <summary>
        ///     FIFO means that the item expiry TTL is set on write never updated, thus being evicted
        ///     when TTL expires independently on how often it's read.
        /// </summary>
        FIFO
    }

    /// <summary>
    ///     Configuration options for hzcache.
    /// </summary>
    public class HzCacheOptions
    {
        public string instanceId { get; set; } = Guid.NewGuid().ToString();

        /// <summary>
        ///     How frequently the cache should clean up expired items. Defaults to 1 second.
        /// </summary>
        public int cleanupJobInterval { get; set; } = 1000;

        /// <summary>
        ///     The default TTL for items added to the cache. Defaults to 5 minutes.
        /// </summary>
        public TimeSpan defaultTTL { get; set; } = TimeSpan.FromMinutes(5);

        /// <summary>
        ///     The listener for value changes in the cache. The first parameter is the key, the second is the change type,
        ///     the third is the checksum of the value, the fourth is the insert timestamp (Unix Time in ms) of the item,
        ///     and the fifth is a boolean indicating if the key is a regex pattern.
        /// </summary>
        public Action<string, CacheItemChangeType, TTLValue, byte[]?, bool?> valueChangeListener { get; set; } = (_, _, _, _, _) => { };

        /// <summary>
        ///     Whether or not to send notifications asynchronously. Defaults to true.
        /// </summary>
        public NotificationType notificationType { get; set; }

        /// <summary>
        ///     Eviction policy to use for the cache. Defaults to LRU.
        /// </summary>
        public EvictionPolicy evictionPolicy { get; set; } = EvictionPolicy.LRU;
    }

    public interface IHzCache
    {
        /// <summary>
        ///     Removes cache keys based on a regex pattern.
        /// </summary>
        /// <param name="re"></param>
        /// <param name="sendNotification"></param>
        void RemoveByPattern(string pattern, bool sendNotification = true);

        /// <summary>
        ///     Attempts to get a value by key
        /// </summary>
        /// <param name="key">The key to get</param>
        /// <returns>True if value exists, otherwise false</returns>
        T? Get<T>(string key);

        /// <summary>
        ///     Attempts to add a key/value item
        /// </summary>
        /// <param name="key">The key to add</param>
        /// <param name="value">The value to add</param>
        /// <returns>True if value was added, otherwise false (already exists)</returns>
        void Set<T>(string key, T? value);

        /// <summary>
        ///     Adds a key/value pair. This method could potentially be optimized, but not sure as of now.
        ///     The initial "TryGetValue" likely costs a bit, which adds time and makes it slower than
        ///     a MemoryCache.Set. However, the "TryGetValue" is required to be able to send the correct
        ///     message type to the listener.
        /// </summary>
        /// <param name="key">The key to add</param>
        /// <param name="value">The value to add</param>
        /// <param name="ttl">TTL of the item</param>
        /// <returns>True if value was added, otherwise false (already exists)</returns>
        void Set<T>(string key, T? value, TimeSpan ttl);

        /// <summary>
        ///     Adds a key/value pair by using the specified function if the key does not already exist, or returns the existing
        ///     value if the key exists.
        /// </summary>
        /// <param name="key">The key to add</param>
        /// <param name="valueFactory">The factory function used to generate the item for the key</param>
        /// <param name="ttl">TTL of the item</param>
        T? GetOrSet<T>(string key, Func<string, T> valueFactory, TimeSpan ttl);

        /// <summary>
        ///     Tries to remove item with the specified key, also returns the object removed in an "out" var
        /// </summary>
        /// <param name="key">The key of the element to remove</param>
        bool Remove(string key);
    }

    public interface IDetailedHzCache : IHzCache
    {
        /// <summary>
        ///     Cleans up expired items (dont' wait for the background job)
        ///     There's rarely a need to execute this method, b/c getting an item checks TTL anyway.
        /// </summary>
        void EvictExpired();

        /// <summary>
        ///     Removes all items from the cache
        /// </summary>
        void Clear();


        /// <summary>
        ///     Tries to remove item with the specified key, also returns the object removed in an "out" var
        /// </summary>
        /// <param name="key">The key of the element to remove</param>
        /// <param name="sendBackplaneNotification">Send backplane notification or not</param>
        /// <param name="checksumEqualsFunc">If function returns true, skip removing the entry</param>
        bool Remove(string key, bool sendBackplaneNotification = true, Func<string?, bool>? checksumEqualsFunc = null);

        CacheStatistics GetStatistics();
    }

    public class CacheStatistics
    {
        public long Counts { get; set; }
        public long SizeInBytes { get; set; }
    }
}
