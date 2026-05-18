using System;
using Tests.Infrastructure.ConnectionString;
using xRetry.v3;

namespace Tests.Infrastructure;

public class RequiresMsSqlRetryTheoryAttribute : RetryTheoryAttribute, Xunit.v3.IFactAttribute
{
        string Xunit.v3.IFactAttribute.Skip => this.Skip;

    public RequiresMsSqlRetryTheoryAttribute(int maxRetries = 3, int delayBetweenRetriesMs = 0)
        : base(maxRetries, delayBetweenRetriesMs)
    {

    }

    public new string Skip
    {
        get
        {
            if (string.IsNullOrEmpty(base.Skip) == false)
                return base.Skip;

            ShouldSkip(out var skipMessage);
            return skipMessage;
        }

        set => base.Skip = value;
    }

    private static bool ShouldSkip(out string skipMessage)
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

        if (MsSqlConnectionString.Instance.CanConnect == false)
        {
            skipMessage = "Test requires MsSQL database";
            return true;
        }

        skipMessage = null;
        return false;
    }
}
