using System;
using Tests.Infrastructure.ConnectionString;
using Xunit;

namespace Tests.Infrastructure
{
    [Obsolete("Use RavenFact(RavenTestCategory.YourCategory, MySqlRequired = true) instead")]
    public class RequiresMySqlFactAttribute : FactAttribute
    {
        public RequiresMySqlFactAttribute()
        {
            if (RavenTestHelper.EnvironmentVariables.SkipIntegrationTests)
            {
                Skip = RavenTestHelper.SkipIntegrationMessage;
                return;
            }

            if (RavenTestHelper.EnvironmentVariables.IsRunningOnCI)
                return;

            if (MySqlConnectionString.Instance.CanConnect == false)
                Skip = "Test requires MySQL database";
        }
    }
}
