using Raven.Server.Integrations.PostgreSQL;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Integrations.PostgreSQL
{
    public sealed class PgSqlParserNativeTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenFact(RavenTestCategory.PostgreSql)]
        public void NativeParser_Loads_OnThisPlatform()
        {
            Assert.True(PgSqlParserNative.IsAvailable, "Native libpg_query parser failed to load on this platform.");
        }
    }
}
