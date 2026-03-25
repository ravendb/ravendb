using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using Raven.Server.SqlMigration;
using Tests.Infrastructure.ConnectionString;
using Xunit;
using Xunit.Sdk;
using Xunit.v3;

namespace Tests.Infrastructure;

public class RequiresMsSqlInlineData : DataAttribute
{
    public override bool SupportsDiscoveryEnumeration() => false;

    public RequiresMsSqlInlineData()
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

    public override ValueTask<IReadOnlyCollection<ITheoryDataRow>> GetData(MethodInfo testMethod, DisposalTracker disposalTracker)
    {
        var result = new List<ITheoryDataRow> { new TheoryDataRow(new object[] { MigrationProvider.MsSQL }) };
        return new ValueTask<IReadOnlyCollection<ITheoryDataRow>>(result);
    }
}
