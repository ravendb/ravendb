using System.Collections.Generic;
using System.Reflection;
using Raven.Server.SqlMigration;
using Tests.Infrastructure;
using Xunit.Sdk;

namespace FastTests
{
    public class RequiresMySqlInlineData : DataAttribute
    {
        public RequiresMySqlInlineData()
        {
            if (RequiresMySqlFactAttribute.IsMySqlAvailable == false)
                Skip = "Test requires MySQL database";
        }

        public override IEnumerable<object[]> GetData(MethodInfo testMethod)
        {
            return new[] { new object[] { MigrationProvider.MySQL } };
        }
    }
}
