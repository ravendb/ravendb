using System;
using Raven.Client.Util;
using Xunit;

namespace Tests.Infrastructure
{
    public class NightlyBuildTheoryAttribute : TheoryAttribute
    {
        internal static bool Force = false; // set to true if you want to force the tests to run

        internal static bool IsNightlyBuild = Force;

        internal static string SkipMessage =
            "Nightly build tests are only working between 21:00 and 6:00 UTC and when 'RAVEN_ENABLE_NIGHTLY_BUILD_TESTS' is set to 'true'.";

        static NightlyBuildTheoryAttribute()
        {
            if (IsNightlyBuild)
                return;

            var variable = Environment.GetEnvironmentVariable("RAVEN_ENABLE_NIGHTLY_BUILD_TESTS");
            if (variable == null || bool.TryParse(variable, out IsNightlyBuild) == false)
            {
                IsNightlyBuild = false;
                return;
            }

            if (IsNightlyBuild == false)
                return;

            var now = SystemTime.UtcNow;
            IsNightlyBuild = now.Hour >= 21 || now.Hour <= 6;
        }

        public override string Skip
        {
            get
            {
                if (IsNightlyBuild)
                    return null;

                return SkipMessage;
            }
        }
    }
}
