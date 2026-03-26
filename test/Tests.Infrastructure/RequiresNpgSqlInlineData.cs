using System.Collections.Generic;
using System.Reflection;
using Raven.Server.SqlMigration;
using Tests.Infrastructure.ConnectionString;
using System.Threading.Tasks;
using Xunit;
using Xunit.Sdk;
using Xunit.v3;

namespace Tests.Infrastructure
{
    public class RequiresNpgSqlInlineData : DataAttribute
    {
        public override bool SupportsDiscoveryEnumeration() => false;

        public RequiresNpgSqlInlineData()
        {
            if (RavenTestHelper.SkipIntegrationTests)
            {
                Skip = RavenTestHelper.SkipIntegrationMessage;
                return;
            }

            if (RavenTestHelper.IsRunningOnCI)
                return;

            if (NpgSqlConnectionString.Instance.CanConnect == false)
                Skip = "Test requires NpgSQL database";
        }

        public override ValueTask<IReadOnlyCollection<ITheoryDataRow>> GetData(MethodInfo testMethod, DisposalTracker disposalTracker)
        {
            var result = new List<ITheoryDataRow> { new TheoryDataRow(new object[] { MigrationProvider.NpgSQL }) };
            return new ValueTask<IReadOnlyCollection<ITheoryDataRow>>(result);
        }
    }
}
