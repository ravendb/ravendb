using System;
using Sparrow.Platform;
using Xunit;

namespace Tests.Infrastructure
{
    internal class Theory32BitAttribute : TheoryAttribute
    {
        internal static bool Is32Bit = false;

        internal static string SkipMessage =
            "Not supported for 64 bits.";

        public Theory32BitAttribute()
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
                if (Is32Bit)
                    return null;

                return SkipMessage;
            }
        }
    }
}
