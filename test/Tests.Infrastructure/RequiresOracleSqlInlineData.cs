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
    public class RequiresOracleSqlInlineData : DataAttribute
    {
        public override bool SupportsDiscoveryEnumeration() => false;

        public RequiresOracleSqlInlineData()
        {
            if (RavenTestHelper.EnvironmentVariables.SkipIntegrationTests)
            {
                Skip = RavenTestHelper.SkipIntegrationMessage;
                return;
            }

            if (RavenTestHelper.EnvironmentVariables.IsRunningOnCI)
                return;

            if (OracleConnectionString.Instance.CanConnect == false)
                Skip = "Test requires Oracle database";
        }

        public override ValueTask<IReadOnlyCollection<ITheoryDataRow>> GetData(MethodInfo testMethod, DisposalTracker disposalTracker)
        {
            var result = new List<ITheoryDataRow> { new TheoryDataRow(new object[] { MigrationProvider.Oracle }) };
            return new ValueTask<IReadOnlyCollection<ITheoryDataRow>>(result);
        }
    }
}
