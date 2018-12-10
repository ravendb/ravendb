using System.Collections.Generic;
using System.Reflection;
using Raven.Server.SqlMigration;
using Xunit.Sdk;

namespace Tests.Infrastructure
{
    public class RequiresOracleSqlInlineData : DataAttribute
    {
        public RequiresOracleSqlInlineData()
        {
            if (RequiresOracleSqlFactAttribute.IsOracleSqlAvailable == false)
                Skip = "Test requires Oracle database";
        }

        public override IEnumerable<object[]> GetData(MethodInfo testMethod)
        {
            return new[] { new object[] { MigrationProvider.Oracle } };
        }
    }
}
