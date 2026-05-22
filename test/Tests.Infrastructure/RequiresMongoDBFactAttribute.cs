using System;
using Tests.Infrastructure.ConnectionString;
using Xunit;

namespace Tests.Infrastructure
{
    [Obsolete("Use RavenFact(RavenTestCategory.YourCategory, MongoDBRequired = true) instead - Note: MongoDBRequired parameter may need to be added to RavenFact")]
    public class RequiresMongoDBFactAttribute : FactAttribute
    {
        public RequiresMongoDBFactAttribute()
        {
            if (RavenTestHelper.EnvironmentVariables.SkipIntegrationTests)
            {
                Skip = RavenTestHelper.SkipIntegrationMessage;
                return;
            }

            if (RavenTestHelper.EnvironmentVariables.IsRunningOnCI)
                return;

            if (MongoDBConnectionString.Instance.CanConnect == false)
                Skip = "Test requires MongoDB";
        }
    }
}
