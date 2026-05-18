using Tests.Infrastructure.ConnectionString;

namespace Tests.Infrastructure;

public static class SnowflakeHelper
{
    internal static bool ShouldSkip(out string skipMessage)
    {
        if (RavenTestHelper.EnvironmentVariables.SkipIntegrationTests)
        {
            skipMessage = RavenTestHelper.SkipIntegrationMessage;
            return true;
        }

        if (RavenTestHelper.EnvironmentVariables.IsRunningOnCI)
        {
            if (RavenTestHelper.EnvironmentVariables.Branch != RavenTestHelper.EnvironmentVariables.SnowflakeTestingBranch)
            {
                skipMessage = $"Snowflake tests are only allowed to run on branch '{RavenTestHelper.EnvironmentVariables.SnowflakeTestingBranch}' branch. Current branch: {RavenTestHelper.EnvironmentVariables.Branch}";
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
