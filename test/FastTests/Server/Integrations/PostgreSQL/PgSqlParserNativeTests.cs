using Raven.Server.Integrations.PostgreSQL;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Integrations.PostgreSQL
{
    public sealed class PgSqlParserNativeTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenMultiplatformFact(RavenTestCategory.PostgreSql, RavenArchitecture.AllX64)]
        public void NativeParser_Loads_OnSupportedArchitecture()
        {
            Assert.True(PgSqlParserNative.IsAvailable, "Native libpg_query parser failed to load on this platform.");
        }
    }
}
