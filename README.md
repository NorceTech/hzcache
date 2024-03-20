# HzCache

Simple Memory Cache for .NET Standard 2.0 backed by Redis pub/sub to be able to manage cache evictions
across multiple instances of the same application.

## TL;DR

Basically it's just a `ConcurrentDictionary` with expiration with the ability to be notified of
changes and on top of that a Redis pub/sub to be able to manage cache evictions across multiple instances.

It's heavily inspired by [Jitbit.FastCache](https://github.com/jitbit/FastCache) with a bit inspiration
from [FusionCache](https://github.com/ZiggyCreatures/FusionCache) which is a much more feature rich implementation
and generally a better choice unless you have dependency between cache items. However it have the sad misfortune of
being based on `IDistributedCache` (and I don't blame FusionCache, I blame the inventor of `IDistributedCache`)
which makes it impossible to have dependency between cache items. And in this specific case that is managed
by using the ability to remove items using a pattern (with `*` - asterisk as wildcard). Simply as that.

Works for my case, might work for you if you have the same problem as I had.

If you want a *distributed* cache, use [Hazelcast](https://hazelcast.com/). 

## Very fast

It's designed to be fast, really fast. In it's simplest configuration it's faster than IMemoryCache. However,
adding value change notifications and Redis pub/sub makes it slower than IMemoryCache. But it's still quite fast
when asynchronous notifications are enabled.

## Ensure using the correct eviction policy
`LRU` means that the cache expiry is extended at every read. This is the default policy.
`FIFO` means that the cache expiry is extended at every write. This is the policy to use if you want to ensure that
data is re-read every now and then and don't have any other mechanisms to evict cache items.

## Work in progress

The `IHzCache` isn't completely thought through, but it covers many needs. Not your need? PR please.

Bug free? No, not at all. Please report bugs if you find them.

The `IHzCache` isn't completely thought through, but it covers many needs. Not your? PR please.