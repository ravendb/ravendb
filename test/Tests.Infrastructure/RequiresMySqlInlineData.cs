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
    public class RequiresMySqlInlineData : DataAttribute
    {
        public override bool SupportsDiscoveryEnumeration() => false;

        public RequiresMySqlInlineData()
        {
            if (RavenTestHelper.EnvironmentVariables.SkipIntegrationTests)
            {
                Skip = RavenTestHelper.SkipIntegrationMessage;
                return;
            }
            
            if (RavenTestHelper.EnvironmentVariables.IsRunningOnCI)
                return;

            if (MySqlConnectionString.Instance.CanConnect == false)
                Skip = "Test requires MySQL database";
        }

        public override ValueTask<IReadOnlyCollection<ITheoryDataRow>> GetData(MethodInfo testMethod, DisposalTracker disposalTracker)
        {
            var result = new List<ITheoryDataRow>
            {
#pragma warning disable CS0618 // Type or member is obsolete
                new TheoryDataRow(new object[] { MigrationProvider.MySQL_MySql_Data }), 
#pragma warning restore CS0618 // Type or member is obsolete
                new TheoryDataRow(new object[] { MigrationProvider.MySQL_MySqlConnector })
            };
            return new ValueTask<IReadOnlyCollection<ITheoryDataRow>>(result);
        }
    }
}
