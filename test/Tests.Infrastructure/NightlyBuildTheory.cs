using System;
using Raven.Client.Util;
using Xunit;

namespace Tests.Infrastructure
{
    public class NightlyBuildTheory : TheoryAttribute
    {
        public override string Skip
        {
            get
            {
                var now = SystemTime.UtcNow;
                if (now.Hour >= 21 || now.Hour <= 6)
                    return null;

                return "Nightly build tests are only working between 21:00 and 6:00 UTC.";
            }
        }
    }
}