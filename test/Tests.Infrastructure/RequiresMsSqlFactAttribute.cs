using System;
using Tests.Infrastructure.ConnectionString;
using Xunit;

namespace Tests.Infrastructure;

[Obsolete("Use RavenFact(RavenTestCategory.YourCategory, MsSqlRequired = true) instead")]
public class RequiresMsSqlFactAttribute : FactAttribute
{
    public RequiresMsSqlFactAttribute()
    {
        if (RavenTestHelper.EnvironmentVariables.SkipIntegrationTests)
        {
            Skip = RavenTestHelper.SkipIntegrationMessage;
            return;
        }

        if (RavenTestHelper.EnvironmentVariables.IsRunningOnCI)
            return;

        if (MsSqlConnectionString.Instance.CanConnect == false)
            Skip = "Test requires MsSQL database";
    }

    internal static bool ShouldSkip(out string skipMessage)
    {
        if (RavenTestHelper.EnvironmentVariables.SkipIntegrationTests)
        {
            skipMessage = RavenTestHelper.SkipIntegrationMessage;
            return true;
        }

        if (RavenTestHelper.EnvironmentVariables.IsRunningOnCI)
        {
            skipMessage = null;
            return false;
        }

        if (MsSqlConnectionString.Instance.CanConnect)
        {
            skipMessage = null;
            return false;
        }

        skipMessage = "Test requires MsSQL database";
        return true;

    }
}
