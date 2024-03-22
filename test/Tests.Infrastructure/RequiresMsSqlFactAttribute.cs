using Tests.Infrastructure.ConnectionString;
using Xunit;

namespace Tests.Infrastructure;

public class RequiresMsSqlFactAttribute : FactAttribute
{
    public RequiresMsSqlFactAttribute()
    {
        if (RavenTestHelper.SkipIntegrationTests)
        {
            Skip = RavenTestHelper.SkipIntegrationMessage;
            return;
        }

        if (RavenTestHelper.IsRunningOnCI)
            return;

        if (MsSqlConnectionString.Instance.CanConnect == false)
            Skip = "Test requires MsSQL database";
    }

    internal static bool ShouldSkip(bool isMsSqlRequired, out string skipMessage)
    {
        skipMessage = null;
        if (isMsSqlRequired == false)
            return false;

        if (RavenTestHelper.SkipIntegrationTests)
        {
            skipMessage = RavenTestHelper.SkipIntegrationMessage;
            return true;
        }

        if (RavenTestHelper.IsRunningOnCI)
            return false;

        if (MsSqlConnectionString.Instance.CanConnect)
            return false;
        
        skipMessage = "Test requires MsSQL database";
        return true;

    }
}
