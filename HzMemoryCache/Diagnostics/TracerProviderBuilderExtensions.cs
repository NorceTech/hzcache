using OpenTelemetry.Trace;
using System;

namespace HzCache.Diagnostics
{
    public static class TracerProviderBuilderBuilderExtensions
    {
        public static TracerProviderBuilder AddHzCacheMemoryInstrumentation(this TracerProviderBuilder builder,
            Action<HzCacheTracesInstrumentationOptions>? configure = null)
        {
            if (builder is null)
                throw new ArgumentNullException(nameof(builder));

            var options = HzCacheTracesInstrumentationOptions.Instance;
            configure?.Invoke(options);

            builder.AddSource(HzCacheDiagnostics.ActivitySourceName);

            return builder;
        }
    }
}