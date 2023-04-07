using System.Collections.Generic;
using System.Reflection;
using FastTests;
using Raven.Server.SqlMigration;
using Tests.Infrastructure.ConnectionString;
using Xunit.Sdk;

namespace Tests.Infrastructure
{
    public class RequiresMySqlInlineData : DataAttribute
    {
        public RequiresMySqlInlineData()
        {
            if (RavenTestHelper.IsRunningOnCI)
                return;

            if (MySqlConnectionString.Instance.CanConnect == false)
                Skip = "Test requires MySQL database";
        }

        public override IEnumerable<object[]> GetData(MethodInfo testMethod)
        {
            return new[] { new object[] { MigrationProvider.MySQL_MySql_Data }, new object[] { MigrationProvider.MySQL_MySqlConnector } };
        }
    }
}
