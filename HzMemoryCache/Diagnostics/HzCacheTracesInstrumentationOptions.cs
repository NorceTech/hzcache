using System;

namespace HzCache.Diagnostics
{
    public class HzCacheTracesInstrumentationOptions
    {
        public static HzCacheTracesInstrumentationOptions Instance { get; } = new();
        public Func<string, string, string, bool> Active { private get; set; }

        public bool IsActive(string activityName, string project, string? key) => Active == null || Active(activityName, project, key);
    }
}