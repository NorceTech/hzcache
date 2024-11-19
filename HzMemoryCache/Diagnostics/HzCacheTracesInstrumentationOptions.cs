using System;

namespace HzCache.Diagnostics
{
    public class HzCacheTracesInstrumentationOptions
    {
        public static HzCacheTracesInstrumentationOptions Instance { get; } = new();
        public Func<string, bool> Active { private get; set; }

        public bool IsActive(string activityName) => Active(activityName);
    }
}