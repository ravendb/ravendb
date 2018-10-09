using System;
using Raven.Client.Util;
using Sparrow.Platform;
using Xunit;

namespace Tests.Infrastructure
{
    public class NightlyBuildTheory64BitAttribute : NightlyBuildTheoryAttribute
    {
        internal static bool Is64Bit = true;

        internal new static string SkipMessage =
            "Nightly build tests on 64bits are only working between 21:00 and 6:00 UTC and when 'RAVEN_ENABLE_NIGHTLY_BUILD_TESTS' is set to 'true'.";

        public NightlyBuildTheory64BitAttribute()
        {
            if (PlatformDetails.Is32Bits)
            {
                Is64Bit = false;
                return;
            }
            if (bool.TryParse(Environment.GetEnvironmentVariable("VORON_INTERNAL_ForceUsing32BitsPager"), out var result))
                if (result == false)
                    Is64Bit = false;
        }

        public override string Skip
        {
            get
            {
                if (IsNightlyBuild && Is64Bit)
                    return null;

                return SkipMessage;
            }
        }
    }
}
