using System;

namespace Voron
{
    internal static class VoronConfiguration
    {
        // PERF: Because all those values are static readonly booleans that are defined during the process of loading the type,
        // the JIT will detect them as such and will use the values instead. 
        // https://alexandrnikitin.github.io/blog/jit-optimization-static-readonly-to-const/
        
        public static readonly bool FailFastForStability;

        static VoronConfiguration()
        {
#if DEBUG
            FailFastForStability = true;
#else
            if (bool.TryParse(Environment.GetEnvironmentVariable("RAVEN_VORON_FAILFAST"), out var isEnabled) == false)
                isEnabled = false;

            if (isEnabled)
            {
                // We are enabling Voron fail-fast in release mode, this is useful to analyze corruptions even in
                // release mode. 
                FailFastForStability = true;
            }
#endif
        }
    }
}
