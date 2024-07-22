using Tests.Infrastructure.ConnectionString;
using Xunit;

namespace Tests.Infrastructure;

public class RequiresSnowflakeFactAttribute : FactAttribute
{
    public RequiresSnowflakeFactAttribute()
    {
        if (RavenTestHelper.SkipIntegrationTests)
        {
            Skip = RavenTestHelper.SkipIntegrationMessage;
            return;
        }

        if (RavenTestHelper.IsRunningOnCI)
            return;

        if (SnowflakeConnectionString.Instance.CanConnect == false)
            Skip = "Test requires Snowflake database";
    }
}
