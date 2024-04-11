using System;
using Raven.Client.Util;
using Xunit;

namespace Tests.Infrastructure
{
    public class NightlyBuildTheoryAttribute : TheoryAttribute
    {
        internal static bool Force = false; // set to true if you want to force the tests to run

        internal static bool IsNightlyBuild = Force;

        internal static readonly string SkipMessage;

        static NightlyBuildTheoryAttribute()
        {
            int startHourUtc = 16;
            int endHourUtc = 6;

            var startHourUtcAsString = Environment.GetEnvironmentVariable("RAVEN_NIGHTLY_BUILD_TESTS_START_HOUR");
            var endHourUtcAsString = Environment.GetEnvironmentVariable("RAVEN_NIGHTLY_BUILD_TESTS_END_HOUR");

            if (startHourUtcAsString != null && int.TryParse(startHourUtcAsString, out var newStartHourUtc))
                startHourUtc = newStartHourUtc;

            if (endHourUtcAsString != null && int.TryParse(endHourUtcAsString, out var newEndHourUtc))
                endHourUtc = newEndHourUtc;

            SkipMessage = $"Nightly build tests are only working between {startHourUtc}:00 and {endHourUtc}:00 UTC and when 'RAVEN_ENABLE_NIGHTLY_BUILD_TESTS' is set to 'true'. They also can be enforced by setting 'RAVEN_FORCE_NIGHTLY_BUILD_TESTS' to 'true'.";

            if (IsNightlyBuild)
                return;

            var forceVariable = Environment.GetEnvironmentVariable("RAVEN_FORCE_NIGHTLY_BUILD_TESTS");
            if (forceVariable != null && bool.TryParse(forceVariable, out var forceNightlyBuildTests) && forceNightlyBuildTests)
            {
                IsNightlyBuild = true;
                return;
            }

            var variable = Environment.GetEnvironmentVariable("RAVEN_ENABLE_NIGHTLY_BUILD_TESTS");
            if (variable == null || bool.TryParse(variable, out IsNightlyBuild) == false)
            {
                IsNightlyBuild = false;
                return;
            }

            if (IsNightlyBuild == false)
                return;

            var now = SystemTime.UtcNow;
            IsNightlyBuild = now.Hour >= startHourUtc || now.Hour <= endHourUtc;
        }

        public override string Skip
        {
            get
            {
                if (IsNightlyBuild)
                    return null;

                return SkipMessage;
            }
            set => base.Skip = value;
        }
    }
}
