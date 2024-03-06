using System;
using Npgsql;
using Tests.Infrastructure.ConnectionString;
using Xunit;

namespace Tests.Infrastructure
{
    public class RequiresNpgSqlFactAttribute : FactAttribute
    {
        public RequiresNpgSqlFactAttribute()
        {
            if (RavenTestHelper.SkipIntegrationTests)
            {
                Skip = RavenTestHelper.SkipIntegrationMessage;
                return;
            }

            if (RavenTestHelper.IsRunningOnCI)
                return;

            if (NpgSqlConnectionString.Instance.CanConnect == false)
                Skip = "Test requires NpgSQL database";
        }
    }
}
