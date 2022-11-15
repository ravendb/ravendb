using FastTests;
using Tests.Infrastructure.ConnectionString;
using Xunit;

namespace Tests.Infrastructure;

public class RequiresMsSqlFactAttribute : FactAttribute
{
    public RequiresMsSqlFactAttribute()
    {
        if (RavenTestHelper.IsRunningOnCI)
            return;

        if (MsSqlConnectionString.Instance.CanConnect == false)
            Skip = "Test requires MsSQL database";
    }
}
