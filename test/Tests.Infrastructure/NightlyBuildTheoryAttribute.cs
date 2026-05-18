using Raven.Client.Util;
using Xunit;

namespace Tests.Infrastructure
{
    public class NightlyBuildTheoryAttribute : TheoryAttribute, Xunit.v3.IFactAttribute
    {
        string Xunit.v3.IFactAttribute.Skip => this.Skip;

        internal static bool Force = false; // set to true if you want to force the tests to run

        internal static bool IsNightlyBuild = Force;

        internal static readonly string SkipMessage;

        static NightlyBuildTheoryAttribute()
        {
            var startHourUtc = RavenTestHelper.EnvironmentVariables.HasNightlyBuildTestsStartHour ? RavenTestHelper.EnvironmentVariables.NightlyBuildTestsStartHour : 16;
            var endHourUtc = RavenTestHelper.EnvironmentVariables.HasNightlyBuildTestsEndHour ? RavenTestHelper.EnvironmentVariables.NightlyBuildTestsEndHour : 6;

            SkipMessage = $"Nightly build tests are only working between {startHourUtc}:00 and {endHourUtc}:00 UTC and when '{RavenTestHelper.EnvironmentVariables.EnableNightlyBuildTestsKey}' is set to 'true'. They also can be enforced by setting '{RavenTestHelper.EnvironmentVariables.ForceNightlyBuildTestsKey}' to 'true'.";

            if (IsNightlyBuild)
                return;

            if (RavenTestHelper.EnvironmentVariables.ForceNightlyBuildTests)
            {
                IsNightlyBuild = true;
                return;
            }

            if (RavenTestHelper.EnvironmentVariables.HasEnableNightlyBuildTests == false)
            {
                IsNightlyBuild = false;
                return;
            }

            IsNightlyBuild = RavenTestHelper.EnvironmentVariables.EnableNightlyBuildTests;
            if (IsNightlyBuild == false)
                return;

            var now = SystemTime.UtcNow;
            IsNightlyBuild = now.Hour >= startHourUtc || now.Hour <= endHourUtc;
        }

        public new string Skip
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
