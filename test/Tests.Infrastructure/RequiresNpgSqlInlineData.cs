using System.Collections.Generic;
using System.Reflection;
using FastTests;
using Raven.Server.SqlMigration;
using Tests.Infrastructure.ConnectionString;
using Xunit.Sdk;

namespace Tests.Infrastructure
{
    public class RequiresNpgSqlInlineData : DataAttribute
    {
        public RequiresNpgSqlInlineData()
        {
            if (RavenTestHelper.IsRunningOnCI)
                return;

            if (NpgSqlConnectionString.Instance.CanConnect == false)
                Skip = "Test requires NpgSQL database";
        }

        public override IEnumerable<object[]> GetData(MethodInfo testMethod)
        {
            return new[] { new object[] { MigrationProvider.NpgSQL } };
        }
    }
}
