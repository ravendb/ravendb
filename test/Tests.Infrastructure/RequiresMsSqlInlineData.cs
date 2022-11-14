using System.Collections.Generic;
using System.Reflection;
using FastTests;
using Raven.Server.SqlMigration;
using Tests.Infrastructure.ConnectionString;
using Xunit.Sdk;

namespace Tests.Infrastructure;

public class RequiresMsSqlInlineData : DataAttribute
{
    public RequiresMsSqlInlineData()
    {
        if (RavenTestHelper.IsRunningOnCI)
            return;

        if (MsSqlConnectionString.Instance.CanConnect == false)
            Skip = "Test requires MsSQL database";
    }

    public override IEnumerable<object[]> GetData(MethodInfo testMethod)
    {
        return new[] { new object[] { MigrationProvider.MsSQL } };
    }
}
