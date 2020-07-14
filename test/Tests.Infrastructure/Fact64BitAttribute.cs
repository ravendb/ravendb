using System;
using Sparrow.Platform;
using Xunit;

namespace Tests.Infrastructure
{
    internal class Fact64BitAttribute : FactAttribute
    {
        internal static bool Is64Bit = true;

        internal static string SkipMessage = "Not supported for 32 bits.";

        private string _customSkipMessage;

        public Fact64BitAttribute()
        {
            if (PlatformDetails.Is32Bits)
            {
                Is64Bit = false;
                return;
            }

            if (bool.TryParse(Environment.GetEnvironmentVariable("VORON_INTERNAL_ForceUsing32BitsPager"), out var result) && result == false)
                Is64Bit = false;
        }

        public override string Skip
        {
            get
            {
                if (_customSkipMessage != null)
                    return _customSkipMessage;

                if (Is64Bit)
                    return null;

                return SkipMessage;
            }

            set
            {
                _customSkipMessage = value;
            }
        }
    }
}
