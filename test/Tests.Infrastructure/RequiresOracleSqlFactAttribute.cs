using System;
using Tests.Infrastructure.ConnectionString;
using Xunit;

namespace Tests.Infrastructure
{
    [Obsolete("Use RavenFact(RavenTestCategory.YourCategory, OracleSqlRequired = true) instead - Note: OracleSqlRequired parameter may need to be added to RavenFact")]
public class RequiresOracleSqlFactAttribute : FactAttribute
    {
        public RequiresOracleSqlFactAttribute()
        {
            if (RavenTestHelper.SkipIntegrationTests)
            {
                Skip = RavenTestHelper.SkipIntegrationMessage;
                return;
            }

            if (RavenTestHelper.IsRunningOnCI)
                return;

            if (OracleConnectionString.Instance.CanConnect == false)
                Skip = "Test requires Oracle database";
        }
    }
}
