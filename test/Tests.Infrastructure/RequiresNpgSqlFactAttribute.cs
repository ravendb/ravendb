using System;
using Tests.Infrastructure.ConnectionString;
using Xunit;

namespace Tests.Infrastructure
{
    [Obsolete("Use RavenFact(RavenTestCategory.YourCategory, NpgSqlRequired = true) instead - Note: NpgSqlRequired parameter may need to be added to RavenFact")]
public class RequiresNpgSqlFactAttribute : FactAttribute
    {
        public RequiresNpgSqlFactAttribute()
        {
            if (RavenTestHelper.EnvironmentVariables.SkipIntegrationTests)
            {
                Skip = RavenTestHelper.SkipIntegrationMessage;
                return;
            }

            if (RavenTestHelper.EnvironmentVariables.IsRunningOnCI)
                return;

            if (NpgSqlConnectionString.Instance.CanConnect == false)
                Skip = "Test requires NpgSQL database";
        }
    }
}
