using System;
using Sparrow.Platform;
using Xunit;

namespace Tests.Infrastructure
{
    public class NightlyBuildFact32Attribute : FactAttribute
    {
        internal static bool Is32Bit = false;

        internal new static string SkipMessage =
            "Nightly build tests on 32bits are only working between 21:00 and 6:00 UTC and when 'RAVEN_ENABLE_NIGHTLY_BUILD_TESTS' is set to 'true'.";

        public NightlyBuildFact32Attribute()
        {
            if (PlatformDetails.Is32Bits)
            {
                Is32Bit = true;
                return;
            }
            if (bool.TryParse(Environment.GetEnvironmentVariable("VORON_INTERNAL_ForceUsing32BitsPager"), out var result))
                if (result == false)
                    Is32Bit = true;
        }
        public override string Skip
        {
            get
            {
                if (NightlyBuildTheoryAttribute.IsNightlyBuild && Is32Bit)
                    return null;

                return SkipMessage;
            }
        }
    }
}
