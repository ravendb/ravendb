using System.Collections.Generic;
using System.Reflection;
using Raven.Server.SqlMigration;
using Tests.Infrastructure.ConnectionString;
using Xunit.Sdk;

namespace Tests.Infrastructure
{
    public class RequiresMySqlInlineData : DataAttribute
    {
        public RequiresMySqlInlineData()
        {
            if (MySqlConnectionString.Instance.CanConnect() == false)
                Skip = "Test requires MySQL database";
        }

        public override IEnumerable<object[]> GetData(MethodInfo testMethod)
        {
            return new[] { new object[] { MigrationProvider.MySQL } };
        }
    }
}
