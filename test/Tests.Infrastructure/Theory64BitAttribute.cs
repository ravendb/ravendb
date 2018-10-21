using System;
using Sparrow.Platform;
using Xunit;

namespace Tests.Infrastructure
{
    internal class Theory64BitAttribute : TheoryAttribute
    {
        internal static bool Is64Bit = true;
        internal static string SkipMessage =
            "Not supported for 32 bits.";
        public Theory64BitAttribute()
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
                if (Is64Bit)
                    return null;

                return SkipMessage;
            }
        }
    }
}
