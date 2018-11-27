using System.Collections.Generic;
using System.Reflection;
using Raven.Server.SqlMigration;
using Tests.Infrastructure;
using Xunit.Sdk;

namespace FastTests
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
            return new[] { new object[] { MigrationProvider.OracleClient } };
        }
    }
}
