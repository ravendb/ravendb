using Tests.Infrastructure.ConnectionString;

namespace Tests.Infrastructure;

public static class SnowflakeHelper
{
    /// <summary>
    /// Returns true when Snowflake-dependent tests can run in the current environment.
    /// Intended for use with xUnit v3 <c>SkipUnless</c>, e.g.
    /// <c>[InlineData(..., SkipType = typeof(SnowflakeHelper), SkipUnless = nameof(IsAvailable))]</c>.
    /// </summary>
    public static bool IsAvailable => ShouldSkip(out _) == false;

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
