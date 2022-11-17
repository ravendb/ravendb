using FastTests;
using Tests.Infrastructure.ConnectionString;
using Xunit;

namespace Tests.Infrastructure
{
    public class RequiresOracleSqlFactAttribute : FactAttribute
    {
        public RequiresOracleSqlFactAttribute()
        {
            if (RavenTestHelper.IsRunningOnCI)
                return;

            if (OracleConnectionString.Instance.CanConnect == false)
                Skip = "Test requires Oracle database";
        }
    }
}
