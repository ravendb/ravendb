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
