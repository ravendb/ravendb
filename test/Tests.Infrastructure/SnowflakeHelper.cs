using System;
using Tests.Infrastructure.ConnectionString;

namespace Tests.Infrastructure;

public static class SnowflakeHelper
{
    internal static bool ShouldSkip(out string skipMessage)
    {
        if (RavenTestHelper.SkipIntegrationTests)
        {
            skipMessage = RavenTestHelper.SkipIntegrationMessage;
            return true;
        }

        if (RavenTestHelper.IsRunningOnCI)
        {
            string snowflakeTestingBranch = Environment.GetEnvironmentVariable("RAVEN_SNOWFLAKE_TESTING_BRANCH");
            string currentBranch = Environment.GetEnvironmentVariable("branch");
            if (currentBranch != snowflakeTestingBranch)
            {
                skipMessage = $"Snowflake tests are only allowed to run on branch '{snowflakeTestingBranch}' branch. Current branch: {currentBranch}";
                return true;
            }

            skipMessage = null;
            return false;
        }

        if (SnowflakeConnectionString.Instance.CanConnect)
        {
            skipMessage = null;
            return false;
        }

        skipMessage = "Test requires Snowflake database";
        return true;
    }
}
