using System;

namespace HzCache.Diagnostics
{
    public class HzCacheTracesInstrumentationOptions
    {
        public static HzCacheTracesInstrumentationOptions Instance { get; } = new();
        public Func<bool> Active { private get; set; }

        public bool IsActive => Active();
    }
}