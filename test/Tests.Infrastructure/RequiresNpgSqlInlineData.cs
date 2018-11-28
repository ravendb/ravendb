using System.Collections.Generic;
using System.Reflection;
using Raven.Server.SqlMigration;
using Xunit.Sdk;

namespace Tests.Infrastructure
{
    public class RequiresNpgSqlInlineData : DataAttribute
    {
        public RequiresNpgSqlInlineData()
        {
            if (RequiresNpgSqlFactAttribute.IsNpgSqlAvailable == false)
                Skip = "Test requires NpgSQL database";
        }

        public override IEnumerable<object[]> GetData(MethodInfo testMethod)
        {
            return new[] { new object[] { MigrationProvider.NpgSQL } };
        }
    }
}
