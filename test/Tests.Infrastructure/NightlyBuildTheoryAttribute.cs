using System;
using Raven.Client.Util;
using Xunit;

namespace Tests.Infrastructure
{
    public class NightlyBuildTheoryAttribute : TheoryAttribute
    {
        private readonly bool _enable;

        public NightlyBuildTheoryAttribute()
        {
            var variable = Environment.GetEnvironmentVariable("RAVEN_ENABLE_NIGHTLY_BUILD_TESTS");
            if (variable == null || bool.TryParse(variable, out _enable) == false)
                return;

            if (_enable == false)
                return;

            var now = SystemTime.UtcNow;
            _enable = now.Hour >= 21 || now.Hour <= 6;
        }

        public override string Skip
        {
            get
            {
                if (_enable == false)
                    return "Nightly build tests are only working between 21:00 and 6:00 UTC and when 'RAVEN_ENABLE_NIGHTLY_BUILD_TESTS' is set to 'true'.";

                return null;
            }
        }
    }
}