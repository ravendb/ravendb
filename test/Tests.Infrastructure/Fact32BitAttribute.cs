using System;
using System.Collections.Generic;
using System.Text;
using Sparrow.Platform;
using Xunit;

namespace Tests.Infrastructure
{
    internal class Fact32BitAttribute : FactAttribute
    {
        internal static bool Is32Bit = false;

        internal static string SkipMessage =
            "Not supported for 64 bits.";

        public Fact32BitAttribute()
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
