using System;
using System.Collections.Generic;
using System.Reflection;
using MySql.Data.MySqlClient;
using Raven.Server.SqlMigration;
using Tests.Infrastructure;
using Xunit.Sdk;

namespace FastTests
{
    public class RequiresMySqlInlineData : DataAttribute
    {
        public RequiresMySqlInlineData()
        {
            try
            {
                using (var con = new MySqlConnection(MySqlTests.LocalConnection))
                {
                    con.Open();
                }
            } 
            catch (Exception e)
            {
                Skip = "Test requires MySQL database";
            }
        }

        public override IEnumerable<object[]> GetData(MethodInfo testMethod)
        {
            return new[] { new object[] { MigrationProvider.MySQL } };
        }
    }
}
